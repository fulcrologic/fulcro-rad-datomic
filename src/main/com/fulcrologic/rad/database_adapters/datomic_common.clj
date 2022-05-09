(ns com.fulcrologic.rad.database-adapters.datomic-common
  (:require
    [clojure.pprint :refer [pprint]]
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [clojure.walk :as walk]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.guardrails.core :refer [>defn => ?]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.ids :refer [new-uuid select-keys-in-ns]]
    [com.fulcrologic.rad.type-support.decimal :as math]
    [edn-query-language.core :as eql]
    [taoensso.timbre :as log]
    [taoensso.encore :as enc]))

(def type-map
  {:string   :db.type/string
   :enum     :db.type/ref
   :boolean  :db.type/boolean
   :password :db.type/string
   :int      :db.type/long
   :long     :db.type/long
   :double   :db.type/double
   :float    :db.type/float
   :bigdec   :db.type/bigdec
   :bigint   :db.type/bigint
   :decimal  :db.type/bigdec
   :instant  :db.type/instant
   :keyword  :db.type/keyword
   :symbol   :db.type/symbol
   :tuple    :db.type/tuple
   :ref      :db.type/ref
   :uuid     :db.type/uuid
   :uri      :db.type/uri})

(>defn pathom-query->datomic-query [all-attributes pathom-query]
  [::attr/attributes ::eql/query => ::eql/query]
  (let [native-id? #(and (true? (do/native-id? %)) (true? (::attr/identity? %)))
        native-ids (into #{}
                     (comp
                       (filter native-id?)
                       (map ::attr/qualified-key))
                     all-attributes)]
    (walk/prewalk (fn [e]
                    (if (and (keyword? e) (contains? native-ids e))
                      :db/id
                      e)) pathom-query)))

(defn- fix-id-keys
  "Fix the ID keys recursively on result."
  [k->a ast-nodes result]
  (let [id?                (fn [{:keys [dispatch-key]}] (some-> dispatch-key k->a ::attr/identity?))
        id-key             (:key (first (filter id? ast-nodes)))
        join-key->children (into {}
                             (comp
                               (filter #(= :join (:type %)))
                               (map (fn [{:keys [key children]}] [key children])))
                             ast-nodes)
        join-keys          (set (keys join-key->children))
        join-key?          #(contains? join-keys %)]
    (reduce-kv
      (fn [m k v]
        (cond
          (= :db/id k) (assoc m id-key v)
          (and (join-key? k) (vector? v)) (assoc m k (mapv #(fix-id-keys k->a (join-key->children k) %) v))
          (and (join-key? k) (map? v)) (assoc m k (fix-id-keys k->a (join-key->children k) v))
          :otherwise (assoc m k v)))
      {}
      result)))

(>defn datomic-result->pathom-result
  "Convert a datomic result containing :db/id into a pathom result containing the proper id keyword that was used
   in the original query."
  [k->a pathom-query result]
  [(s/map-of keyword? ::attr/attribute) ::eql/query (? coll?) => (? coll?)]
  (when result
    (let [{:keys [children]} (eql/query->ast pathom-query)]
      (if (vector? result)
        (mapv #(fix-id-keys k->a children %) result)
        (fix-id-keys k->a children result)))))

(def keys-in-delta
  (fn keys-in-delta [delta]
    (let [id-keys  (into #{}
                     (map first)
                     (keys delta))
          all-keys (into id-keys
                     (mapcat keys)
                     (vals delta))]
      all-keys)))

(defn schemas-for-delta [{::attr/keys [key->attribute]} delta]
  (let [all-keys (keys-in-delta delta)
        schemas  (into #{}
                   (keep #(-> % key->attribute ::attr/schema))
                   all-keys)]
    schemas))

(defn tempid->intermediate-id [env delta]
  (let [tempids (volatile! #{})
        _       (walk/prewalk (fn [e]
                                (when (tempid/tempid? e)
                                  (vswap! tempids conj e))
                                e) delta)
        fulcro-tempid->real-id
                (into {} (map (fn [t] [t (str (:id t))]) @tempids))]
    fulcro-tempid->real-id))

(defn native-ident?
  "Returns true if the given ident is using a database native ID (:db/id)"
  [{::attr/keys [key->attribute] :as env} ident]
  (boolean (some-> ident first key->attribute do/native-id?)))

(defn uuid-ident?
  "Returns true if the ID in the given ident uses UUIDs for ids."
  [{::attr/keys [key->attribute] :as env} ident]
  (= :uuid (some-> ident first key->attribute ::attr/type)))

(defn failsafe-id
  "Returns a fail-safe id for the given ident in a transaction. A fail-safe ID will be one of the following:
  - A long (:db/id) for a pre-existing entity.
  - A string that stands for a temporary :db/id within the transaction if the id of the ident is temporary.
  - A lookup ref (the ident itself) if the ID uses a non-native ID, and it is not a tempid.
  - A keyword if it is a keyword (a :db/ident)
  "
  [{::attr/keys [key->attribute] :as env} ident]
  (if (keyword? ident)
    ident
    (let [[_ id] ident]
      (cond
        (tempid/tempid? id) (str (:id id))
        (and (native-ident? env ident) (pos-int? id)) id
        :otherwise ident))))

(defn to-one? [{::attr/keys [key->attribute]} k]
  (not (boolean (some-> k key->attribute (attr/to-many?)))))

(defn ref? [{::attr/keys [key->attribute]} k]
  (= :ref (some-> k key->attribute ::attr/type)))

(defn schema-value? [{::attr/keys [key->attribute]} target-schema k]
  (let [{:keys       [::attr/schema]
         ::attr/keys [identity?]} (key->attribute k)]
    (and (= schema target-schema) (not identity?))))

(defn fix-numerics
  "Using field types double and float can cause anomolies with Datomic because js might send an int when the number has
   no decimal digits."
  [{::attr/keys [type]} v]
  (case type
    :double (double v)
    :float (double v)
    :numeric (math/numeric v)
    :int (long v)
    :long (long v)
    v))

(defn tx-value
  "Convert `v` to a transaction-safe value based on its type and cardinality."
  [{::attr/keys [key->attribute] :as env} k v]
  (if (ref? env k)
    (failsafe-id env v)
    (let [attr (key->attribute k)]
      (fix-numerics attr v))))

(defn to-one-txn [env schema delta]
  (vec
    (mapcat
      (fn [[ident entity-delta]]
        (reduce
          (fn [tx [k {:keys [before after]}]]
            (if (and (schema-value? env schema k) (to-one? env k))
              (cond
                (not (nil? after)) (conj tx [:db/add (failsafe-id env ident) k (tx-value env k after)])
                (not (nil? before)) (conj tx [:db/retract (failsafe-id env ident) k (tx-value env k before)])
                :else tx)
              tx))
          []
          entity-delta))
      delta)))

(defn to-many-txn [env schema delta]
  (vec
    (mapcat
      (fn [[ident entity-delta]]
        (reduce
          (fn [tx [k {:keys [before after]}]]
            (if (and (schema-value? env schema k) (not (to-one? env k)))
              (let [before  (into #{} (map (fn [v] (tx-value env k v))) before)
                    after   (into #{} (map (fn [v] (tx-value env k v))) after)
                    adds    (map
                              (fn [v] [:db/add (failsafe-id env ident) k v])
                              (set/difference after before))
                    removes (map
                              (fn [v] [:db/retract (failsafe-id env ident) k v])
                              (set/difference before after))]
                (into tx (concat adds removes)))
              tx))
          []
          entity-delta))
      delta)))

(defn next-uuid [] (new-uuid))

(defn generate-next-id
  "Generate an id. You may pass a `suggested-id` as a UUID or a tempid. If it is a tempid and the ID column is a UUID, then
  the UUID *from* the tempid will be used. If the ID column is not a UUID then the suggested id is ignored. Returns nil for
  native ID columns."
  ([{::attr/keys [key->attribute] :as env} k]
   (generate-next-id env k (next-uuid)))
  ([{::attr/keys [key->attribute] :as env} k suggested-id]
   (let [{native-id?  :com.fulcrologic.rad.database-adapters.datomic/native-id?
          ::attr/keys [type]} (key->attribute k)]
     (cond
       native-id? nil
       (= :uuid type) (cond
                        (tempid/tempid? suggested-id) (:id suggested-id)
                        (uuid? suggested-id) suggested-id
                        :else (next-uuid))
       :otherwise (throw (ex-info "Cannot generate an ID for non-native ID attribute" {:attribute k}))))))

(defn tempids->generated-ids [{::attr/keys [key->attribute] :as env} delta]
  (let [idents (keys delta)
        fulcro-tempid->generated-id
               (into {} (keep (fn [[k id :as ident]]
                                (when (and (tempid/tempid? id) (not (native-ident? env ident)))
                                  [id (generate-next-id env k)])) idents))]
    fulcro-tempid->generated-id))

(>defn delta->txn
  [env schema delta]
  [map? keyword? map? => map?]
  (let [tempid->txid                 (tempid->intermediate-id env delta)
        tempid->generated-id         (tempids->generated-ids env delta)
        non-native-id-attributes-txn (keep
                                       (fn [[k id :as ident]]
                                         (when (and (tempid/tempid? id) (uuid-ident? env ident))
                                           [:db/add (tempid->txid id) k (tempid->generated-id id)]))
                                       (keys delta))]
    {:tempid->string       tempid->txid
     :tempid->generated-id tempid->generated-id
     :txn                  (into []
                             (concat
                               non-native-id-attributes-txn
                               (to-one-txn env schema delta)
                               (to-many-txn env schema delta)))}))

(defn- attribute-schema [attributes]
  (mapv
    (fn [{::attr/keys [identity? type qualified-key cardinality] :as a}]
      (let [attribute-schema (do/attribute-schema a)
            overrides        (select-keys-in-ns a "db")
            datomic-type     (get type-map type)]
        (when-not datomic-type
          (throw (ex-info (str "No mapping from attribute type to Datomic: " type) {})))
        (merge
          (cond-> {:db/ident       qualified-key
                   :db/cardinality (if (= :many cardinality)
                                     :db.cardinality/many
                                     :db.cardinality/one)
                   :db/valueType   datomic-type}
            (map? attribute-schema) (merge attribute-schema)
            identity? (assoc :db/unique :db.unique/identity))
          overrides)))
    attributes))

(defn- enumerated-values [attributes]
  (mapcat
    (fn [{::attr/keys [qualified-key type enumerated-values] :as a}]
      (when (= :enum type)
        (let [enum-nspc (str (namespace qualified-key) "." (name qualified-key))]
          (keep (fn [v]
                  (cond
                    (map? v) v
                    (qualified-keyword? v) {:db/ident v}
                    :otherwise (let [enum-ident (keyword enum-nspc (name v))]
                                 {:db/ident enum-ident})))
            enumerated-values))))
    attributes))

(>defn automatic-schema
  "Returns a Datomic transaction for the complete schema of the supplied RAD `attributes`
   that have a `::datomic/schema` that matches `schema-name`."
  [attributes schema-name]
  [::attr/attributes keyword? => vector?]
  (let [attributes (filter #(= schema-name (::attr/schema %)) attributes)]
    (when (empty? attributes)
      (log/warn "Automatic schema requested, but the attribute list is empty. No schema will be generated!"))
    (let [txn (attribute-schema attributes)
          txn (into txn (enumerated-values attributes))]
      txn)))

(defn schema-problems
  "Validate that a database supports then named `schema`, and return any problems that are found.

   This function finds all attributes that are declared on the schema, and checks that the Datomic representation of them
  meets minimum requirements for desired operation. This function returns a list of problems found, and can be used in
  applications that manage their own schema to ensure that the database will operate correctly in RAD.

  The `pull` argument is the pull function to use against Datomic, which is either the client or on-prem version of pull."
  [db schema all-attributes pull]
  (reduce
    (fn [problems attr]
      (let [{attr-schema      ::attr/schema
             attr-cardinality ::attr/cardinality
             native-id?       do/native-id?
             ::attr/keys      [qualified-key type]} attr]
        (if (and (= attr-schema schema) (not native-id?))
          (let [{:db/keys [cardinality valueType]} (pull db '[{:db/cardinality [:db/ident]}
                                                              {:db/valueType [:db/ident]}] qualified-key)
                cardinality (get cardinality :db/ident :one)
                db-type     (get valueType :db/ident :unknown)]
            (cond-> problems

              (not= (get type-map type) db-type)
              (conj (str qualified-key " has type " db-type " but was expected to have type " (get type-map type)))

              (not= (name cardinality) (name (or attr-cardinality :one)))
              (conj (str qualified-key "'s cardinality does not match."))))
          problems)))
    []
    all-attributes))

(defn ensure-schema!
  "Ensure that the schema for the given attributes exists in the datomic database of the connection `conn`.

  * `transact` - The transact function to use in order to run a transaction. MUST BE a `(fn [c tx])` (on-prem style).
  * `conn` - The datomic connection.
  * `schema-name` - Only attributes matching the given schema name will be transacted.
  * `attributes` - The attributes to ensure. Auto-filters attributes that do not match `schema-name`, and also does
    Tuples in a separate transaction to ensure composite tuples can succeed.

  See `verify-schema!` and `schema-problems` for validation functions on hand-managed schema.
  "
  [transact conn schema-name attributes]
  (let [{tuple-attrs   true
         regular-attrs false} (group-by (fn [{::attr/keys [type]}] (= :tuple type)) attributes)
        non-tuple-txn (automatic-schema regular-attrs schema-name)
        tuple-txn     (when (seq tuple-attrs)
                        (automatic-schema tuple-attrs schema-name))]
    (log/debug "Transacting automatic schema.")
    (when (log/may-log? :trace)
      (log/trace "Generated Schema:\n" (with-out-str (pprint non-tuple-txn)))
      (when tuple-txn (log/trace "Tuple Schema:\n" (with-out-str (pprint tuple-txn)))))
    (transact conn (vec non-tuple-txn))
    (when tuple-txn
      (transact conn (vec tuple-txn)))))

(defn ref-entity->ident* [db datoms-for-id-fn {:db/keys [ident id] :as ent}]
  "Using datoms-for-id-fn, get to the ident for an entity"
  (cond
    ident ident
    id (if-let [ident (:v (first (datoms-for-id-fn db id)))]
         ident
         ent)
    :else ent))

(defn replace-ref-types*
  "Common implementation of replace-ref-types.

  Takes in datoms-for-id-fn, that is different between on-prem and cloud."
  [db datoms-for-id-fn refs arg]
  (walk/postwalk
    (fn [arg]
      (cond
        (and (map? arg) (some #(contains? refs %) (keys arg)))
        (reduce
          (fn [acc ref-k]
            (cond
              (and (get acc ref-k) (not (vector? (get acc ref-k))))
              (update acc ref-k (partial ref-entity->ident* db datoms-for-id-fn))
              (and (get acc ref-k) (vector? (get acc ref-k)))
              (update acc ref-k #(mapv (partial ref-entity->ident* db datoms-for-id-fn) %))
              :else acc))
          arg
          refs)
        :else arg))
    arg))

(defn pull-*-common
  "Common implementation of pull-*.

  Takes in pull-fn, pull-many-fn, and datoms-for-id-fn, that is different between on-prem and cloud."
  ([db pull-fn pull-many-fn datoms-for-id-fn pattern db-idents eid-or-eids]
   (->> (if (and (not (eql/ident? eid-or-eids)) (sequential? eid-or-eids))
          (pull-many-fn db pattern eid-or-eids)
          (pull-fn db pattern eid-or-eids))
     (replace-ref-types* db datoms-for-id-fn db-idents)))
  ([db pull-fn pull-many-fn datoms-for-id-fn pattern ident-keywords eid-or-eids transform-fn]
   (let [result (pull-*-common db pull-fn pull-many-fn datoms-for-id-fn pattern ident-keywords eid-or-eids)]
     (if (sequential? result)
       (mapv transform-fn result)
       (transform-fn result)))))

(defn get-by-ids*
  "Common implementation of get-by-ids.

  Takes in pull-fn, pull-many-fn, and datoms-for-id-fn, that is different between on-prem and cloud."
  [db pull-fn pull-many-fn datoms-for-id-fn ids db-idents desired-output]
  (pull-*-common db pull-fn pull-many-fn datoms-for-id-fn desired-output db-idents ids))

(defn entity-query*
  "Common implementation of entity-query.

  Takes in pull-fn, pull-many-fn, and datoms-for-id-fn, that is different between on-prem and cloud."
  [pull-fn pull-many-fn datoms-for-id-fn
   {:keys       [::attr/schema ::id-attribute]
    ::attr/keys [attributes]
    :as         env}
   input]
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
        (let [result (get-by-ids* db pull-fn pull-many-fn datoms-for-id-fn ids enumerations query)]
          (if one?
            (first result)
            result)))
      (do
        (log/info "Unable to complete query.")
        nil))))

(defn id-resolver-pathom2*
  "Common implementation of id-resolver.

  Takes in pull-fn, pull-many-fn, and datoms-for-id-fn, that is different between on-prem and cloud."
  [pull-fn pull-many-fn datoms-for-id-fn
   all-attributes
   {::attr/keys [qualified-key] :keys [::attr/schema ::wrap-resolve :com.wsscode.pathom.connect/transform] :as id-attribute}
   output-attributes]
  (log/info "Building ID resolver for" qualified-key)
  (enc/if-let [_          id-attribute
               outputs    (attr/attributes->eql output-attributes)
               pull-query (pathom-query->datomic-query all-attributes outputs)]
    (let [wrap-resolve     (get id-attribute do/wrap-resolve (get id-attribute ::wrap-resolve))
          resolve-sym      (symbol
                             (str (namespace qualified-key))
                             (str (name qualified-key) "-resolver"))
          with-resolve-sym (fn [r]
                             (fn [env input]
                               (r (assoc env :com.wsscode.pathom.connect/sym resolve-sym) input)))]
      (log/debug "Computed output is" outputs)
      (log/debug "Datomic pull query to derive output is" pull-query)
      (cond-> {:com.wsscode.pathom.connect/sym     resolve-sym
               :com.wsscode.pathom.connect/output  outputs
               :com.wsscode.pathom.connect/batch?  true
               :com.wsscode.pathom.connect/resolve (cond-> (fn [{::attr/keys [key->attribute] :as env} input]
                                                             (->> (entity-query*
                                                                    pull-fn pull-many-fn datoms-for-id-fn
                                                                    (assoc env
                                                                      ::attr/schema schema
                                                                      ::attr/attributes output-attributes
                                                                      ::id-attribute id-attribute
                                                                      ::default-query pull-query)
                                                                    input)
                                                               (datomic-result->pathom-result key->attribute outputs)
                                                               (auth/redact env)))
                                                     wrap-resolve (wrap-resolve)
                                                     :always (with-resolve-sym))
               :com.wsscode.pathom.connect/input   #{qualified-key}}
        transform transform))
    (do
      (log/error "Unable to generate id-resolver. "
        "Attribute was missing schema, or could not be found in the attribute registry: " qualified-key)
      nil)))

(defn generate-resolvers*
  "Common implementation of generate-resolvers.

  Takes in pull-fn, pull-many-fn, and datoms-for-id-fn, that is different between on-prem and cloud."
  [pull-fn pull-many-fn datoms-for-id-fn
   attributes schema]
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
                                               resolver (id-resolver-pathom2* pull-fn pull-many-fn datoms-for-id-fn attributes attr v)]
                                    (conj result resolver)
                                    (do
                                      (log/error "Internal error generating resolver for ID key" k)
                                      result)))
                                []
                                entity-id->attributes)]
    entity-resolvers))

(defn wrap-env
  "Build a (fn [env] env') that adds RAD datomic support to an env. If `base-wrapper` is supplied, then it will be called
   as part of the evaluation, allowing you to build up a chain of environment middleware.

   See attributes/wrap-env for more information.
   "
  ([database-mapper connection->db] (wrap-env nil database-mapper connection->db))
  ([base-wrapper database-mapper connection->db]
   (fn [env]
     (cond-> (let [database-connection-map (database-mapper env)
                   databases               (enc/map-vals (fn [connection] (atom (connection->db connection))) database-connection-map)]
               (assoc env
                 do/connections database-connection-map
                 do/databases databases))
       base-wrapper (base-wrapper)))))

(defn pathom-plugin
  "A pathom 2 plugin that adds the necessary Datomic connections and databases to the pathom env for
  a given request. Requires a database-mapper, which is a
  `(fn [pathom-env] {schema-name connection})` for a given request.

  See `wrap-env` for Pathom 3.

  The `connection->db` argument is the proper Datomic API `d/db` function (client or on-prem).

  The resulting pathom-env available to all resolvers will then have:

  - `do/connections`: The result of database-mapper
  - `do/databases`: A map from schema name to atoms holding a database. The atom is present so that
  a mutation that modifies the database can choose to update the snapshot of the db being used for the remaining
  resolvers.

  This plugin should run before (be listed after) most other plugins in the plugin chain since
  it adds connection details to the parsing env.
  "
  [database-mapper connection->db]
  (let [augment (wrap-env database-mapper connection->db)]
    {:com.wsscode.pathom.core/wrap-parser
     (fn env-wrap-wrap-parser [parser]
       (fn env-wrap-wrap-internal [env tx]
         (parser (augment env) tx)))}))

