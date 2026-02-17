(ns com.fulcrologic.rad.database-adapters.datomic-cloud-async
  "Complete async Datomic Cloud adapter with save/delete middleware, resolver generation,
   and bootstrap functions. Mirrors `datomic-cloud` but uses `datomic.client.api.async`
   for all Datomic operations.

   Middleware signature: `(fn [async-handler] (fn [save-env] channel))`
   where `async-handler` is itself `(fn [save-env] channel)`.

   Composition example:
   ```clojure
   (def save-middleware
     (->
       (datomic-async/wrap-datomic-save)     ; terminal, returns (fn [env] ch)
       (custom/wrap-my-middleware)))           ; wraps with (fn [async-handler] (fn [env] ch))
   ```"
  (:require
   [clojure.core.async :as async]
   [clojure.pprint :refer [pprint]]
   [clojure.walk :as walk]
   [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.authorization :as auth]
   [com.fulcrologic.rad.database-adapters.datomic-common :as common]
   [com.fulcrologic.rad.database-adapters.datomic-options :as do]
   [com.fulcrologic.rad.form :as form]
   [datomic.client.api.async :as d]
   [edn-query-language.core :as eql]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]))

;; =============================================================================
;; Low-level async Datomic wrappers (all return channels)
;; =============================================================================

(defn dbid->ident
  "Returns a channel containing a map of `{entity-id db-ident}` for all idents in `db`."
  [db]
  (async/go
    (let [datoms (async/<! (d/datoms db {:index      :avet
                                         :components [:db/ident]
                                         :limit      -1}))]
      (into {}
            (map (juxt :e :v))
            datoms))))

(defn pull-many
  "Like Datomic pull, but takes a collection of `ids` and returns a channel with
   a collection of entity maps. Results are in the same order as `ids`."
  [db pull-spec ids]
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
           vec))))

(defn- pull
  "Async pull workaround for Datomic local nil-on-channel bug. Uses `d/q` instead of `d/pull`."
  [db pattern id]
  (async/go
    (let [result (async/<! (if (or (int? id) (keyword? id))
                             (d/q
                              {:query '[:find (pull ?e pattern)
                                        :in $ pattern ?e]
                               :args  [db pattern id]})
                             (d/q
                              {:query '[:find (pull ?e pattern)
                                        :in $ pattern ?a ?v
                                        :where
                                        [?e ?a ?v]]
                               :args  [db pattern (first id) (second id)]})))]
      (ffirst result))))

(defn replace-ref-types
  "Returns a channel. Replaces numeric ref values in `arg` with their `:db/ident` keywords,
   for any keys present in `refs`."
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
  "Returns a channel. Pulls one or many entities and replaces `:db/ident` maps with their keyword."
  [db pattern db-idents eid-or-eids]
  (async/go
    (let [items (async/<! (if (and (not (eql/ident? eid-or-eids)) (sequential? eid-or-eids))
                            (pull-many db pattern eid-or-eids)
                            (pull db pattern eid-or-eids)))]
      (async/<! (replace-ref-types db db-idents items)))))

;; =============================================================================
;; Core async save/delete (return channels)
;; =============================================================================

(defn refresh-current-dbs!
  "Returns a channel. Updates the database atoms in `env` to the latest db snapshot from each connection."
  [env]
  (async/go
    (doseq [k (keys (get env do/connections))
            :let [conn    (get-in env [do/connections k])
                  db-atom (get-in env [do/databases k])]]
      (reset! db-atom (async/<! (d/db conn))))))

(defn save-form!
  "Returns a channel with `{:tempids {...}}`. Saves the form `delta` from `save-params` to all
   Datomic databases involved. If `:datomic/transact` is in `env`, that function will be used
   instead of the default async transact (it must also return a channel)."
  [{:datomic/keys [transact] :as env} {::form/keys [delta] :as save-params}]
  (let [schemas (common/schemas-for-delta env delta)
        tx!     (or transact d/transact)]
    (async/go
      (let [result (atom {:tempids {}})]
        (doseq [schema schemas
                :let [connection (-> env do/connections (get schema))
                      {:keys [tempid->string
                              tempid->generated-id
                              txn]} (common/delta->txn env schema delta)]]
          (when (log/may-log? :trace)
            (log/trace "Saving form delta" (with-out-str (pprint delta)))
            (log/trace "on schema" schema)
            (log/trace "Running txn\n" (with-out-str (pprint txn))))
          (if (and connection (seq txn))
            (try
              (let [tx-result        (async/<! (tx! connection {:tx-data txn}))
                    tempids          (:tempids tx-result)
                    tempid->real-id  (into {}
                                           (map (fn [tempid] [tempid (get tempid->generated-id tempid
                                                                          (get tempids (tempid->string tempid)))]))
                                           (keys tempid->string))
                    database-atom    (get-in env [do/databases schema])]
                (when database-atom
                  (reset! database-atom (async/<! (d/db connection))))
                (swap! result update :tempids merge tempid->real-id))
              (catch Exception e
                (log/error e "Transaction failed!")
                (throw e)))
            (log/error "Unable to save form. Either connection was missing in env, or txn was empty.")))
        @result))))

(defn delete-entity!
  "Returns a channel with `{}` on success. Deletes the entity identified by `params`.
   If `:datomic/transact` is in `env`, that function will be used instead of the default
   async transact (it must also return a channel)."
  [{:datomic/keys [transact]
    ::attr/keys   [key->attribute] :as env} params]
  (enc/if-let [raw-txn    (or (do/raw-txn env) [])
               pk         (ffirst params)
               id         (get params pk)
               ident      [pk id]
               {:keys [::attr/schema]} (key->attribute pk)
               connection (-> env do/connections (get schema))
               txn        (into [[:db/retractEntity ident]] raw-txn)]
    (do
      (log/info "Deleting" ident)
      (let [database-atom (get-in env [do/databases schema])
            tx!           (or transact d/transact)]
        (async/go
          (async/<! (tx! connection {:tx-data txn}))
          (when database-atom
            (reset! database-atom (async/<! (d/db connection))))
          {})))
    (do
      (log/warn "Datomic adapter failed to delete " params)
      (async/go nil))))

;; =============================================================================
;; Async middleware
;; =============================================================================

(defn wrap-datomic-save
  "Form save middleware (async). Returns `(fn [save-env] channel)`.

   `async-handler` - An async handler `(fn [save-env] channel)` to chain after save.
   `addl-env` - Static (constant) map merged into pathom-env on every call. Supports:
     * `:datomic/transact` - Override function for transacting datoms (must return a channel)."
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (save-form! pathom-env params)))
  ([async-handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (async/go
       (let [save-result    (async/<! (save-form! pathom-env params))
             handler-result (async/<! (async-handler pathom-env))]
         (deep-merge save-result handler-result)))))
  ([async-handler addl-env]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [env (merge addl-env pathom-env)]
       (async/go
         (let [save-result    (async/<! (save-form! env params))
               handler-result (async/<! (async-handler env))]
           (deep-merge save-result handler-result)))))))

(defn wrap-datomic-delete
  "Form delete middleware (async). Returns `(fn [save-env] channel)`.

   `async-handler` - An async handler `(fn [save-env] channel)` to chain after delete.
   `addl-env` - Static (constant) map merged into pathom-env on every call. Supports:
     * `:datomic/transact` - Override function for transacting datoms (must return a channel)."
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (delete-entity! pathom-env params)))
  ([async-handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (async/go
       (let [local-result   (async/<! (delete-entity! pathom-env params))
             handler-result (async/<! (async-handler pathom-env))]
         (deep-merge handler-result local-result)))))
  ([async-handler addl-env]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [env (merge addl-env pathom-env)]
       (async/go
         (let [local-result   (async/<! (delete-entity! env params))
               handler-result (async/<! (async-handler env))]
           (deep-merge handler-result local-result)))))))

;; =============================================================================
;; Resolver generation
;; =============================================================================

(defn entity-query*
  "Returns a channel with the query result for the given `input` against the database in `env`."
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
  "Generates a resolver using `resolver-maker-fn` (Pathom 2 or 3) for the given `id-attribute`
   with async entity query internals."
  [resolver-maker-fn
   all-attributes
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
                                           query            (if (and minimal-query? client-query-ast)
                                                              (common/prune-query-ast client-query-ast pull-query)
                                                              pull-query)
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
        (resolver-maker-fn resolve-sym qualified-key outputs resolver-fn transform)))
    (do
      (log/error "Unable to generate id-resolver. "
                 "Attribute was missing schema, or could not be found in the attribute registry: " qualified-key)
      nil)))

(defn generate-resolvers*
  "Generates resolvers for all entities in `schema` using `resolver-maker-fn` (Pathom 2 or 3).
   Bind `common/*use-cache?*` and `common/*minimal-pull?*` to override defaults."
  [resolver-maker-fn attributes schema]
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
                                              resolver (id-resolver-pathom* resolver-maker-fn attributes attr v)]
                                   (conj result resolver)
                                   (do
                                     (log/error "Internal error generating resolver for ID key" k)
                                     result)))
                               []
                               entity-id->attributes)]
    entity-resolvers))

(defn generate-resolvers
  "Generates Pathom 2 resolvers for all entities in `schema`. Options:

   * `:use-cache?` - Use Pathom request cache (default true)
   * `:minimal-pulls?` - Pull only what client requests (default false)"
  ([attributes schema]
   (generate-resolvers attributes schema {}))
  ([attributes schema {:keys [use-cache? minimal-pulls?]}]
   (binding [common/*minimal-pull?* (if (boolean? minimal-pulls?) minimal-pulls? false)
             common/*use-cache?*    (cond
                                      (boolean use-cache?) use-cache?
                                      (true? minimal-pulls?) false
                                      :else true)]
     (generate-resolvers* common/make-pathom2-resolver attributes schema))))

(defn generate-resolvers-pathom3
  "Generates Pathom 3 resolvers for all entities in `schema`. Options:

   * `:use-cache?` - Use Pathom request cache (default true)
   * `:minimal-pulls?` - Pull only what client requests (default false)"
  ([attributes schema]
   (generate-resolvers-pathom3 attributes schema {}))
  ([attributes schema {:keys [use-cache? minimal-pulls?]}]
   (binding [common/*minimal-pull?* (if (boolean? minimal-pulls?) minimal-pulls? false)
             common/*use-cache?*    (cond
                                      (boolean use-cache?) use-cache?
                                      (true? minimal-pulls?) false
                                      :else true)]
     (generate-resolvers* common/make-pathom3-resolver attributes schema))))

;; =============================================================================
;; Bootstrap (blocking wrappers — one-time startup operations)
;; =============================================================================

(def suggested-logging-blacklist
  "A vector of namespace strings that generate debug noise with Datomic. Add to Timbre's ns-blacklist."
  ["shadow.cljs.devtools.server.worker.impl"])

(defn verify-schema!
  "Validates that `db` supports the named `schema`. Throws if problems are found."
  [db schema all-attributes]
  (let [blocking-pull (fn [db pattern eid]
                        (async/<!! (pull db pattern eid)))
        problems      (common/schema-problems db schema all-attributes blocking-pull)]
    (when (seq problems)
      (doseq [p problems]
        (log/error p))
      (throw (ex-info "Validation Failed" {:schema schema})))))

(defn config->client
  "Returns a Datomic client from `config`. Blocks on the async client creation."
  [config]
  (if (= (:datomic/env config) :test)
    (:datomic/test-client config)
    (async/<!! (d/client (:datomic/client config)))))

(defn ^:deprecated ensure-schema!
  "Use common/ensure-schema! instead."
  ([all-attributes config conn]
   (ensure-schema! all-attributes config {} conn))
  ([all-attributes {:datomic/keys [schema] :as config} schemas conn]
   (let [generator (get schemas schema :auto)]
     (cond
       (= :auto generator) (common/ensure-schema!
                            (fn [c txn] (async/<!! (d/transact c {:tx-data txn})))
                            conn schema all-attributes)
       (ifn? generator) (do
                          (log/info "Running custom schema function.")
                          (generator conn))
       :otherwise (log/info "Schema management disabled.")))))

(defn start-database!
  "Starts a Datomic Cloud database connection. Blocks during setup (schema migration, verification).
   Returns a migrated database connection.

   * `all-attributes` - RAD attributes (must be registered before calling)
   * `config` - Map with `:datomic/schema`, `:datomic/database`, `:datomic/client` keys
   * `schemas` - Map from schema name to `:auto`, `:none`, or `(fn [conn])`"
  [all-attributes {:datomic/keys [schema database] :as config} schemas]
  (let [client (config->client config)
        _      (async/<!! (d/create-database client {:db-name database}))
        conn   (async/<!! (d/connect client {:db-name database}))]
    (when (= :auto (get schemas schema :auto))
      (common/ensure-schema!
       (fn [c txn] (async/<!! (d/transact c {:tx-data txn})))
       conn schema all-attributes))
    (verify-schema! (async/<!! (d/db conn)) schema all-attributes)
    (log/info "Finished connecting to and migrating database for" schema)
    conn))

(defn adapt-external-database!
  "Verifies schema of a Datomic database not under this adapter's control. Blocks."
  [conn schema all-attributes]
  (verify-schema! (async/<!! (d/db conn)) schema all-attributes))

(defn start-databases
  "Starts all databases described in `config`. Returns `{db-key connection}`.

   * `config` - Map containing `do/databases` entry
   * `schemas` - Map of schema name to `:auto` or `(fn [conn])`"
  ([all-attributes config]
   (start-databases all-attributes config {}))
  ([all-attributes config schemas]
   (reduce-kv
    (fn [m k v]
      (log/info "Starting database " k)
      (assoc m k (start-database! all-attributes v schemas)))
    {}
    (do/databases config))))

;; =============================================================================
;; Environment / plugin
;; =============================================================================

(defn mock-resolver-env
  "Returns a mock env with `do/connections` and `do/databases` for a single `schema` and `connection`.
   Blocks on `d/db` to initialize the database atom."
  [schema connection]
  {do/connections {schema connection}
   do/databases   {schema (atom (async/<!! (d/db connection)))}})

(def datomic-api
  "Async Datomic Cloud API function map for use with common internals."
  {:datomic-api/datoms      d/datoms
   :datomic-api/index-pull  d/index-pull
   :datomic-api/index-range d/index-range
   :datomic-api/pull-many   pull-many
   :datomic-api/transact    d/transact
   :datomic-api/db          d/db})

(defn pathom-plugin
  "A Pathom 2 plugin that adds Datomic connections and databases to the pathom env.
   Uses a blocking `d/db` wrapper so database atoms are populated synchronously at request start.

   `database-mapper` - `(fn [pathom-env] {schema-name connection})`"
  [database-mapper]
  (common/pathom-plugin database-mapper (fn [conn] (async/<!! (d/db conn))) datomic-api))

(defn wrap-env
  "Builds a `(fn [env] env')` that adds RAD Datomic support to a Pathom 3 env.
   Uses a blocking `d/db` wrapper for database atom initialization.

   * `base-wrapper` - Optional middleware `(fn [env] env')`, or nil
   * `database-mapper` - `(fn [env] {schema-name connection})`"
  ([database-mapper]
   (common/wrap-env nil database-mapper (fn [conn] (async/<!! (d/db conn))) datomic-api))
  ([base-wrapper database-mapper]
   (common/wrap-env base-wrapper database-mapper (fn [conn] (async/<!! (d/db conn))) datomic-api)))
