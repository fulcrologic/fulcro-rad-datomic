(ns com.fulcrologic.rad.database-adapters.datomic-cloud-async-resolvers
  "Resolver generation"
  (:require
    [clojure.core.async :as async]
    [clojure.walk :as walk]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [datomic.client.api.async :as d]
    [edn-query-language.core :as eql]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]))

(defn dbid->ident
  "Returns a channel containing the db-ident of the given db id (or nil)."
  [db]
  (async/go
    (let [datoms (async/<! (d/datoms db {:index      :avet
                                         :components [:db/ident]
                                         :limit      -1}))]
      (into {}
        (map (juxt :e :v))
        datoms))))

(defn pull-many
  "like Datomic pull, but takes a collection of ids and returns
   a channel with a collection of entity maps. The results will be in the same order as `ids`"
  ([db pull-spec ids]
   (async/go
     (let [lookup-ref?  (vector? (first ids))
           order-map    (if lookup-ref?
                          (into {} (map vector (map second ids) (range)))
                          (into {} (map vector ids (range))))
           sort-kw      (if lookup-ref? (ffirst ids) :db/id)
           missing-ref? (nil? (seq (filter #(= sort-kw %) pull-spec)))
           pull-spec    (if missing-ref?
                          (conj pull-spec sort-kw)
                          pull-spec)
           result       (async/<! (d/q {:query '[:find (pull ?id pattern)
                                                 :in $ [?id ...] pattern]
                                        :args  [db ids pull-spec]}))]
       (->> result
         (map first)
         (sort-by #(order-map (get % sort-kw)))
         vec)))))

(defn replace-ref-types*
  "Replace numeric ref values with their db/ident."
  [db refs arg]
  (async/go
    (let [dbid->ident  (async/<! (dbid->ident db))
          replace-refs (fn replace-refs* [v]
                         (cond
                           (int? v) (get dbid->ident v v)
                           (vector? v) (mapv replace-refs* v)
                           (map? v) (let [{:db/keys [id ident]} v]
                                      (cond
                                        ident ident
                                        id (get dbid->ident id id)
                                        :else v))
                           :else v))]
      (walk/postwalk
        (fn [v]
          (if (map? v)
            (reduce-kv
              (fn [acc k v]
                (if (contains? refs k)
                  (assoc acc k (replace-refs v))
                  acc))
              v
              v)
            v))
        arg))))

(defn pull-*
  "Pulls one OR many things, and makes sure db/ident maps are replaced just by the db/ident keyword."
  [db pattern db-idents eid-or-eids]
  (async/go
    (let [items (async/<! (if (and (not (eql/ident? eid-or-eids)) (sequential? eid-or-eids))
                            (pull-many db pattern eid-or-eids)
                            (d/pull db {:selector pattern
                                        :eid      eid-or-eids})))]
      (async/<! (replace-ref-types* db db-idents items)))))

(defn entity-query*
  "Common implementation of entity-query.

  Takes in pull-fn, pull-many-fn, and datoms-for-id-fn, that is different between on-prem and cloud."
  [{:keys       [::attr/schema ::id-attribute]
    ::attr/keys [attributes]
    :as         env}
   input]
  (async/go
    (let [{native-id?  do/native-id?
           ::attr/keys [qualified-key]} id-attribute
          one? (not (sequential? input))]
      (enc/if-let [db           (some-> (get-in env [do/databases schema]) deref)
                   query        (get env ::default-query)
                   ids          (if one?
                                  [(get input qualified-key)]
                                  (into [] (keep #(get % qualified-key) input)))
                   ids          (if native-id?
                                  ids
                                  (mapv (fn [id] [qualified-key id]) ids))
                   enumerations (into #{}
                                  (keep #(when (= :enum (::attr/type %))
                                           (::attr/qualified-key %)))
                                  attributes)]
        (do
          (log/trace "Running" query "on entities with " qualified-key ":" ids)
          (let [result (async/<! (pull-* db query enumerations ids))]
            (if one?
              (first result)
              result)))
        (do
          (log/info "Unable to complete query.")
          nil)))))

(defn id-resolver-pathom*
  "Common implementation of id-resolver.

  Takes in resolver-maker-fn, which knows how to make a Pathom resolver for either Pathom2 or Pathom3.
  Its signature must be [resolve-sym qualified-key outputs resolver-fn transform] with an optional final
  map argument that can accept options.

  If the ID attribute has no `generate-minimal-pull?` or `resolver-cache?` option, then the dynamic variables
  `*minimal-pull?*` and `*use-cache?*` will be used as defaults (false, true are the legacy defaults).
  "
  [all-attributes
   {::attr/keys [qualified-key] :keys [::attr/schema ::wrap-resolve :com.wsscode.pathom.connect/transform] :as id-attribute}
   output-attributes]
  (log/debug "Building ID resolver for" qualified-key)
  (enc/if-let [_          id-attribute
               outputs    (attr/attributes->eql output-attributes)
               pull-query (common/pathom-query->datomic-query all-attributes outputs)]
    (let [wrap-resolve   (get id-attribute do/wrap-resolve (get id-attribute ::wrap-resolve))
          resolve-sym    (symbol
                           (str (namespace qualified-key))
                           (str (name qualified-key) "-resolver-datomic"))
          minimal-query? (boolean
                           (or
                             (true? (do/generate-minimal-pull? id-attribute))
                             common/*minimal-pull?*))
          use-cache?     (boolean
                           (if (boolean? (do/resolver-cache? id-attribute))
                             (do/resolver-cache? id-attribute)
                             common/*use-cache?*))
          resolver-fn    (cond-> (fn [{::attr/keys [key->attribute] :as env} input]
                                   (async/go
                                     (let [client-query-ast (common/env->client-query-ast env)
                                           query            (if (and minimal-query? client-query-ast) (common/prune-query-ast client-query-ast pull-query) pull-query)
                                           result           (async/<! (entity-query*
                                                                        (assoc env
                                                                          ::attr/schema schema
                                                                          ::attr/attributes output-attributes
                                                                          ::id-attribute id-attribute
                                                                          ::default-query query)
                                                                        input))]
                                       (->> result
                                         (common/datomic-result->pathom-result key->attribute outputs)
                                         (auth/redact env)))))
                           wrap-resolve (wrap-resolve))]
      (log/debug "Computed output is" outputs)
      (log/debug "Datomic pull query to derive output is" pull-query)
      (binding [common/*use-cache?*    use-cache?
                common/*minimal-pull?* minimal-query?]
        (common/make-pathom2-resolver resolve-sym qualified-key outputs resolver-fn transform)))
    (do
      (log/error "Unable to generate id-resolver. "
        "Attribute was missing schema, or could not be found in the attribute registry: " qualified-key)
      nil)))

(defn generate-resolvers*
  "Common implementation of generate-resolvers.

  Takes in resolver-maker-fn, which knows how to make a Pathom resolver for either Pathom2 or Pathom3.

  Bind the dynamic vars *use-cache?* and *minimal-pull?* if you want to override the defaults."
  [attributes schema]
  (let [attributes            (filter #(= schema (::attr/schema %)) attributes)
        key->attribute        (attr/attribute-map attributes)
        entity-id->attributes (group-by ::k (mapcat (fn [attribute]
                                                      (map
                                                        (fn [id-key] (assoc attribute ::k id-key))
                                                        (get attribute ::attr/identities)))
                                              attributes))
        entity-resolvers      (reduce-kv
                                (fn [result k v]
                                  (enc/if-let [attr     (key->attribute k)
                                               resolver (id-resolver-pathom* attributes attr v)]
                                    (conj result resolver)
                                    (do
                                      (log/error "Internal error generating resolver for ID key" k)
                                      result)))
                                []
                                entity-id->attributes)]
    entity-resolvers))

(defn generate-resolvers
  "Generate all of the resolvers that make sense for the given database config. This should be passed
  to your Pathom2 parser to register resolvers for each of your schemas.

  The options can include (which can also be set on individual ID attributes):

  * use-cache? - Default to using Pathom request cache for all entities (default true)
  * minimal-pulls? - Default to asking for everything an entity has from Datomic instead of just what the client wants.
    (default false). Setting to true can cause unexpected behavior unless you turn OFF the cache.
  "
  ([attributes schema]
   (generate-resolvers attributes schema {}))
  ([attributes schema {:keys [use-cache? minimal-pulls?]}]
   (binding [common/*minimal-pull?* (if (boolean? minimal-pulls?) minimal-pulls? false)
             common/*use-cache?*    (cond
                                      (boolean use-cache?) use-cache?
                                      (true? minimal-pulls?) false
                                      :else true)]
     (generate-resolvers* attributes schema))))
