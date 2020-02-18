(ns com.fulcrologic.rad.database-adapters.datomic
  (:require
    [cljs.spec.alpha :as s]
    [clojure.set :as set]
    [clojure.walk :as walk]
    [clojure.pprint :refer [pprint]]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.guardrails.core :refer [>defn => ?]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.ids :refer [select-keys-in-ns]]
    [com.rpl.specter :as sp]
    [com.wsscode.pathom.connect :as pc]
    [datomic.api :as d]
    [datomic.function :as df]
    [datomock.core :as dm :refer [mock-conn]]
    [edn-query-language.core :as eql]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]))

(def type-map
  {:string   :db.type/string
   :enum     :db.type/ref
   :boolean  :db.type/boolean
   :password :db.type/string
   :int      :db.type/long
   :long     :db.type/long
   :decimal  :db.type/bigdec
   :instant  :db.type/instant
   :keyword  :db.type/keyword
   :symbol   :db.type/symbol
   :ref      :db.type/ref
   :uuid     :db.type/uuid})

;; FIXME: There should be a client API query that runs on startup to find idents so that this is more efficient and also so we don't use the entity API.
(defn replace-ref-types
  "dbc   the database to query
   refs  a set of keywords that ref datomic entities, which you want to access directly
          (rather than retrieving the entity id)
   m     map returned from datomic pull containing the entity IDs you want to deref"
  [db refs arg]
  (walk/postwalk
    (fn [arg]
      (cond
        (and (map? arg) (some #(contains? refs %) (keys arg)))
        (reduce
          (fn [acc ref-k]
            (cond
              (and (get acc ref-k) (not (vector? (get acc ref-k))))
              (update acc ref-k (comp :db/ident (partial d/entity db) :db/id))
              (and (get acc ref-k) (vector? (get acc ref-k)))
              (update acc ref-k #(mapv (comp :db/ident (partial d/entity db) :db/id) %))
              :else acc))
          arg
          refs)
        :else arg))
    arg))

(defn pull-*
  "Will either call d/pull or d/pull-many depending on if the input is
  sequential or not.

  Optionally takes in a transform-fn, applies to individual result(s)."
  ([db pattern ident-keywords eid-or-eids]
   (->> (if (and (not (eql/ident? eid-or-eids)) (sequential? eid-or-eids))
          (d/pull-many db pattern eid-or-eids)
          (d/pull db pattern eid-or-eids))
     (replace-ref-types db ident-keywords)))
  ([db pattern ident-keywords eid-or-eids transform-fn]
   (let [result (pull-* db pattern ident-keywords eid-or-eids)]
     (if (sequential? result)
       (mapv transform-fn result)
       (transform-fn result)))))

(defn get-by-ids [db pk ids ident-keywords desired-output]
  ;; TODO: Should use consistent DB for atomicity
  (let [eids (mapv (fn [id] [pk id]) ids)]
    (pull-* db desired-output ident-keywords eids)))

(defn ref->ident
  "Sometimes references on the client are actual idents and sometimes they are
  nested maps, this function attempts to return an ident regardless."
  [x]
  (when (eql/ident? x) x))

(defn delta->datomic-txn
  "Takes in a normalized form delta, usually from client, and turns in
  into a Datomic transaction for the given schema (returns empty txn if there is nothing on the delta for that schema)."
  [schema delta]
  (vec
    (mapcat (fn [[[id-k id] entity-diff]]
              (let [id-attribute (attr/key->attribute id-k)]
                (when (-> id-attribute ::schema (= schema))
                  (conj
                    (mapcat (fn [[k diff]]
                              (let [{:keys [before after]} diff
                                    {::attr/keys [cardinality type] :as attribute} (attr/key->attribute k)]
                                (when-not attribute
                                  (log/error "MISSING ATTRIBUTE IN ATTRIBUTE REGISTRY!" k))
                                (cond
                                  (and (= :enum type) (= :many cardinality))
                                  (let [ident [id-k id]]
                                    [[:com.fulcrologic.rad.fn/set-to-many-enumeration ident k (set after)]])

                                  (ref->ident after) (if (nil? after)
                                                       [[:db/retract (str id) k (ref->ident before)]]
                                                       [[:com.fulcrologic.rad.fn/add-ident (str id) k (ref->ident after)]])

                                  (and (sequential? after) (every? ref->ident after))
                                  (let [before   (into #{}
                                                   (comp (map ref->ident) (remove nil?))
                                                   before)
                                        after    (into #{}
                                                   (comp (map ref->ident) (remove nil?))
                                                   after)
                                        retracts (set/difference before after)
                                        adds     (set/difference after before)
                                        eid      (str id)]
                                    (vec
                                      (concat
                                        (for [r retracts] [:db/retract eid k r])
                                        (for [a adds] [:com.fulcrologic.rad.fn/add-ident eid k a]))))

                                  (and (sequential? after) (every? keyword? after))
                                  (let [before   (into #{}
                                                   (comp (remove nil?))
                                                   before)
                                        after    (into #{}
                                                   (comp (remove nil?))
                                                   after)
                                        retracts (set/difference before after)
                                        adds     (set/difference after before)
                                        eid      (str id)]
                                    (vec
                                      (concat
                                        (for [r retracts] [:db/retract eid k r])
                                        (for [a adds] [:db/add eid k a]))))

                                  ;; Assume field is optional and omit
                                  (and (nil? before) (nil? after)) []

                                  :else (if (nil? after)
                                          (if (ref->ident before)
                                            [[:db/retract (str id) k (ref->ident before)]]
                                            [[:db/retract (str id) k before]])
                                          [[:db/add (str id) k after]]))))
                      entity-diff)
                    {id-k id :db/id (str id)}))))
      delta)))

(def keys-in-delta
  (fn keys-in-delta [delta]
    (let [id-keys  (into #{}
                     (map first)
                     (keys delta))
          all-keys (into id-keys
                     (mapcat keys)
                     (vals delta))]
      all-keys)))

(defn schemas-for-delta [delta]
  (let [all-keys (keys-in-delta delta)
        schemas  (into #{}
                   (keep #(-> % attr/key->attribute ::schema))
                   all-keys)]
    schemas))

(defn save-form!
  "Do all of the possible Datomic operations for the given form delta (save to all Datomic databases involved)"
  [env form-delta]
  (let [tempids (sp/select (sp/walker tempid/tempid?) form-delta)
        fulcro-tempid->real-id
                (into {} (map (fn [t] [t (d/squuid)]) tempids))
        schemas (schemas-for-delta (::form/delta form-delta))]
    (log/debug "Saving form across " schemas)
    (doseq [schema schemas
            :let [connection (-> env ::connections (get schema))
                  form-delta (sp/transform (sp/walker tempid/tempid?) fulcro-tempid->real-id form-delta)
                  txn        (delta->datomic-txn schema (::form/delta form-delta))]]
      (log/debug "Saving form delta" form-delta)
      (log/debug "on schema" schema)
      (log/debug "Running txn\n" (with-out-str (pprint txn)))
      (if (and connection (seq txn))
        (let [database-atom (get-in env [::databases schema])]
          @(d/transact connection txn)
          (when database-atom
            (reset! database-atom (d/db connection))))
        (log/error "Unable to save form. Either connection was missing in env, or txn was empty.")))
    fulcro-tempid->real-id))

(defn delete-entity!
  "Delete the given entity, if possible."
  [env [pk id :as ident]]
  (enc/if-let [{::keys [schema]} (attr/key->attribute pk)
               connection (-> env ::connections (get schema))
               txn        [[:db/retractEntity ident]]]
    (do
      (log/info "Deleting" ident)
      (let [database-atom (get-in env [::databases schema])]
        @(d/transact connection txn)
        (when database-atom
          (reset! database-atom (d/db connection)))))
    (log/warn "Datomic adapter failed to delete ident " ident)))

(def suggested-logging-blacklist
  "A vector containing a list of namespace strings that generate a lot of debug noise when using Datomic. Can
  be added to Timbre's ns-blacklist to reduce logging overhead."
  ["com.mchange.v2.c3p0.impl.C3P0PooledConnectionPool"
   "com.mchange.v2.c3p0.stmt.GooGooStatementCache"
   "com.mchange.v2.resourcepool.BasicResourcePool"
   "com.zaxxer.hikari.pool.HikariPool"
   "com.zaxxer.hikari.pool.PoolBase"
   "com.mchange.v2.c3p0.impl.AbstractPoolBackedDataSource"
   "com.mchange.v2.c3p0.impl.NewPooledConnection"
   "datomic.common"
   "datomic.connector"
   "datomic.coordination"
   "datomic.db"
   "datomic.index"
   "datomic.kv-cluster"
   "datomic.log"
   "datomic.peer"
   "datomic.process-monitor"
   "datomic.reconnector2"
   "datomic.slf4j"
   "io.netty.buffer.PoolThreadCache"
   "org.apache.http.impl.conn.PoolingHttpClientConnectionManager"
   "org.projectodd.wunderboss.web.Web"
   "org.quartz.core.JobRunShell"
   "org.quartz.core.QuartzScheduler"
   "org.quartz.core.QuartzSchedulerThread"
   "org.quartz.impl.StdSchedulerFactory"
   "org.quartz.impl.jdbcjobstore.JobStoreTX"
   "org.quartz.impl.jdbcjobstore.SimpleSemaphore"
   "org.quartz.impl.jdbcjobstore.StdRowLockSemaphore"
   "org.quartz.plugins.history.LoggingJobHistoryPlugin"
   "org.quartz.plugins.history.LoggingTriggerHistoryPlugin"
   "org.quartz.utils.UpdateChecker"
   "shadow.cljs.devtools.server.worker.impl"])

(defn- attribute-schema [attributes]
  (mapv
    (fn [{::attr/keys [unique? identity? type qualified-key cardinality] :as a}]
      (let [overrides    (select-keys-in-ns a "db")
            datomic-type (get type-map type)]
        (when-not datomic-type
          (throw (ex-info (str "No mapping from attribute type to Datomic: " type) {})))
        (merge
          (cond-> {:db/ident       qualified-key
                   :db/cardinality (if (= :many cardinality)
                                     :db.cardinality/many
                                     :db.cardinality/one)
                   :db/index       true
                   :db/valueType   datomic-type}
            unique? (assoc :db/unique :db.unique/value)
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
  (let [attributes (filter #(= schema-name (::schema %)) attributes)]
    (when (empty? attributes)
      (log/warn "Automatic schema requested, but the attribute list is empty. No schema will be generated!"))
    (let [txn (attribute-schema attributes)
          txn (into txn (enumerated-values attributes))]
      txn)))

(defn config->postgres-url [{:postgresql/keys [user host port password database]
                             datomic-db       :datomic/database}]
  (assert user ":postgresql/user must be specified")
  (assert host ":postgresql/host must be specified")
  (assert port ":postgresql/port must be specified")
  (assert password ":postgresql/password must be specified")
  (assert database ":postgresql/database must be specified")
  (assert datomic-db ":datomic/database must be specified")
  (str "datomic:sql://" datomic-db "?jdbc:postgresql://" host (when port (str ":" port)) "/"
    database "?user=" user "&password=" password))

(defn config->mysql-url [{:mysql/keys [user host port password database]
                          datomic-db       :datomic/database}]
  (assert user ":mysql/user must be specified")
  (assert host ":mysql/host must be specified")
  (assert port ":mysql/port must be specified")
  (assert password ":mysql/password must be specified")
  (assert database ":mysql/database must be specified")
  (assert datomic-db ":datomic/database must be specified")
  (str "datomic:sql://" datomic-db "?jdbc:mysql://" host (when port (str ":" port)) "/"
    database "?user=" user "&password=" password "&useSSL=false"))

(defn config->free-url [{:free/keys [host port]
                         datomic-db :datomic/database}]
  (assert host ":free/host must be specified")
  (assert port ":free/port must be specified")
  (assert datomic-db ":datomic/database must be specified")
  (str "datomic:free://" host ":" port "/" datomic-db))

(defn config->url [{:datomic/keys [driver]
                    :as           config}]
  (case driver
    :mem (str "datomic:mem://" (:datomic/database config))
    :free (config->free-url config)
    :postgresql (config->postgres-url config)
    :mysql (config->mysql-url config)
    (throw (ex-info "Unsupported Datomic driver." {:driver driver}))))

(defn ensure-transactor-functions!
  "Must be called on any Datomic database that will be used with automatic form save. This
  adds transactor functions.  The built-in startup logic (if used) will automatically call this,
  but if you create/start your databases with custom code you should run this on your newly
  created database."
  [conn]
  @(d/transact conn [{:db/id    (d/tempid :db.part/user)
                      :db/ident :com.fulcrologic.rad.fn/set-to-many-enumeration
                      :db/fn    (df/construct
                                  '{:lang   "clojure"
                                    :params [db eid rel set-of-enumerated-values]
                                    :code   (do
                                              (when-not (every? qualified-keyword? set-of-enumerated-values)
                                                (throw (IllegalArgumentException.
                                                         (str "set-to-many-enumeration expects a set of keywords that are idents" set-of-enumerated-values))))
                                              (let [old-val    (into #{}
                                                                 (map :db/ident)
                                                                 (get (datomic.api/pull db [{rel [:db/ident]}] eid) rel))
                                                    to-retract (into []
                                                                 (map (fn [k] [:db/retract eid rel k]))
                                                                 (clojure.set/difference old-val set-of-enumerated-values))
                                                    eid        (or (:db/id (datomic.api/pull db [:db/id] eid)) (str (second eid)))
                                                    txn        (into to-retract
                                                                 (map (fn [k] [:db/add eid rel k]))
                                                                 (clojure.set/difference set-of-enumerated-values old-val))]
                                                txn))})}
                     {:db/id    (d/tempid :db.part/user)
                      :db/ident :com.fulcrologic.rad.fn/add-ident
                      :db/fn    (df/construct
                                  '{:lang   "clojure"
                                    :params [db eid rel ident]
                                    :code   (do
                                              (when-not (and (= 2 (count ident))
                                                          (keyword? (first ident)))
                                                (throw (IllegalArgumentException.
                                                         (str "ident must be an ident, got " ident))))
                                              (let [ref-val (or (:db/id (datomic.api/entity db ident))
                                                              (str (second ident)))]
                                                [[:db/add eid rel ref-val]]))})}]))

(>defn verify-schema!
  "Validate that a database supports then named `schema`. This function finds all attributes
  that are declared on the schema, and checks that the Datomic representation of them
  meets minimum requirements for desired operation.

  This function throws an exception if a problem is found, and can be used in
  applications that manage their own schema to ensure that the database will
  operate correctly in RAD."
  [db schema all-attributes]
  [any? keyword? ::attr/attributes => any?]
  (let [die! #(throw (ex-info "Validation Failed" {:schema schema}))]
    (doseq [attr all-attributes
            :let [{attr-schema      ::schema
                   attr-cardinality ::attr/cardinality
                   ::attr/keys      [qualified-key type]} attr]]
      (when (= attr-schema schema)
        (log/debug "Checking schema" schema "attribute" qualified-key)
        (let [{:db/keys [cardinality valueType]} (d/pull db '[{:db/cardinality [:db/ident]} {:db/valueType [:db/ident]}] qualified-key)
              cardinality (get cardinality :db/ident :one)
              db-type     (get valueType :db/ident :unknown)]
          (when (not= (get type-map type) db-type)
            (log/error qualified-key "for schema" schema "is incorrect in the database. It has type" valueType
              "but was expected to have type" (get type-map type))
            (die!))
          (when (not= (name cardinality) (name (or attr-cardinality :one)))
            (log/error qualified-key "for schema" schema "is incorrect in the database, since cardinalities do not match:" (name cardinality) "vs" attr-cardinality)
            (die!)))))))

(defn start-database!
  "Starts a Datomic database connection given the standard sub-element config described
  in `start-databases`. Typically use that function instead of this one.

  NOTE: This function relies on the attribute registry, which you must populate before
  calling this.

  `all-attributes

  * `:config` a map of k-v pairs for setting up a connection.
  * `schemas` a map from schema name to either :auto, :none, or (fn [conn]).

  Returns a migrated database connection."
  [all-attributes {:datomic/keys [schema prevent-changes?] :as config} schemas]
  (let [url             (config->url config)
        generator       (get schemas schema :auto)
        _               (d/create-database url)
        mock?           (boolean (or prevent-changes? (System/getProperty "force.mocked.connection")))
        real-connection (d/connect url)
        conn            (if mock? (dm/fork-conn real-connection) real-connection)]
    (when mock?
      (log/warn "==========================================================")
      (log/warn "Database mocking enabled. No database changes will persist!")
      (log/warn "=========================================================="))
    (log/info "Adding form save support to database transactor functions.")
    (ensure-transactor-functions! conn)
    (cond
      (= :auto generator) (let [txn (automatic-schema all-attributes schema)]
                            (log/info "Transacting automatic schema.")
                            (log/debug "Schema:\n" (with-out-str (pprint txn)))
                            @(d/transact conn txn))
      (ifn? generator) (do
                         (log/info "Running custom schema function.")
                         (generator conn))
      :otherwise (log/info "Schema management disabled."))
    (verify-schema! (d/db conn) schema all-attributes)
    (log/info "Finished connecting to and migrating database.")
    conn))

(defn adapt-external-database!
  "Adds necessary transactor functions and verifies schema of a Datomic database that is not
  under the control of this adapter, but is used by it."
  [conn schema all-attributes]
  (ensure-transactor-functions! conn)
  (verify-schema! (d/db conn) schema all-attributes))

(defn start-databases
  "Start all of the databases described in config, using the schemas defined in schemas.

  * `config`:  a map that contains the key ::datomic/databases.
  * `schemas`:  a map whose keys are schema names, and whose values can be missing (or :auto) for
  automatic schema generation, a `(fn [schema-name conn] ...)` that updates the schema for schema-name
  on the database reachable via `conn`. You may omit `schemas` if automatic generation is being used
  everywhere.

  WARNING: Be sure all of your model files are required before running this function, since it
  will use the attribute definitions during startup.

  The `::datomic/databases` entry in the config is a map with the following form:

  ```
  {:production-shard-1 {:datomic/schema :production
                        :datomic/driver :postgresql
                        :datomic/database \"prod\"
                        :postgresql/host \"localhost\"
                        :postgresql/port \"localhost\"
                        :postgresql/user \"datomic\"
                        :postgresql/password \"datomic\"
                        :postgresql/database \"datomic\"}}
  ```

  The `:datomic/schema` is used to select the attributes that will appear in that database's schema.
  The remaining parameters select and configure the back-end storage system for the database.

  Each supported driver type has custom options for configuring it. See Fulcro's config
  file support for a good method of defining these in EDN config files for use in development
  and production environments.

  Returns a map whose keys are the database keys (i.e. `:production-shard-1`) and
  whose values are the live database connection.
  "
  ([all-attributes config]
   (start-databases all-attributes config {}))
  ([all-attributes config schemas]
   (reduce-kv
     (fn [m k v]
       (log/info "Starting database " k)
       (assoc m k (start-database! all-attributes v schemas)))
     {}
     (::databases config))))

(defn entity-query
  [{::keys      [schema]
    ::attr/keys [qualified-key attributes]
    :as         env} input]
  (let [one? (not (sequential? input))]
    (enc/if-let [db             (some-> (get-in env [::databases schema]) deref)
                 query          (get env ::default-query)
                 ids            (if one?
                                  [(get input qualified-key)]
                                  (into [] (keep #(get % qualified-key) input)))
                 ident-keywords (into #{}
                                  (keep #(when (= :enum (::attr/type %))
                                           (::attr/qualified-key %)))
                                  attributes)]
      (do
        (log/info "Running" query "on entities with " qualified-key ":" ids)
        (let [result (get-by-ids db qualified-key ids ident-keywords query)]
          (if one?
            (first result)
            result)))
      (do
        (log/info "Unable to complete query.")
        nil))))

(>defn id-resolver
  [id-key attributes]
  [qualified-keyword? ::attr/attributes => ::pc/resolver]
  (log/info "Building ID resolver for" id-key)
  (enc/if-let [outputs   (attr/attributes->eql attributes)
               attribute (attr/key->attribute id-key)
               schema    (::schema attribute)]
    {::pc/sym     (symbol
                    (str (namespace id-key))
                    (str (name id-key) "-resolver"))
     ::pc/output  outputs
     ::pc/batch?  true
     ::pc/resolve (fn [env input] (->>
                                    (entity-query
                                      (assoc env
                                        ::schema schema
                                        ::attr/attributes attributes
                                        ::attr/qualified-key id-key
                                        ::default-query outputs)
                                      input)
                                    (auth/redact env)))
     ::pc/input   #{id-key}}
    (do
      (log/error "Unable to generate id-resolver. "
        "Attribute was missing schema, or could not be found in the attribute registry: " id-key)
      nil)))

(defn generate-resolvers
  "Generate all of the resolvers that make sense for the given database config. This should be passed
  to your Pathom parser to register resolvers for each of your schemas."
  [attributes schema]
  (let [attributes            (filter #(= schema (::schema %)) attributes)
        entity-id->attributes (group-by ::k (mapcat (fn [attribute]
                                                      (map
                                                        (fn [id-key] (assoc attribute ::k id-key))
                                                        (get attribute ::entity-ids)))
                                              attributes))
        entity-resolvers      (reduce-kv
                                (fn [result k v] (conj result (id-resolver k v)))
                                []
                                entity-id->attributes)]
    entity-resolvers))

(def ^:private pristine-db (atom nil))
(def ^:private migrated-dbs (atom {}))

(defn pristine-db-connection
  "Returns a Datomic database that has no application schema or data."
  []
  (locking pristine-db
    (when-not @pristine-db
      (let [db-url (str "datomic:mem://" (gensym "test-database"))
            _      (log/info "Creating test database" db-url)
            _      (d/create-database db-url)
            conn   (d/connect db-url)]
        (ensure-transactor-functions! conn)
        (reset! pristine-db (d/db conn))))
    (dm/mock-conn @pristine-db)))

(defn empty-db-connection
  "Returns a Datomic database that contains the given application schema, but no data.
   This function must be passed a schema name (keyword).  The optional second parameter
   is the actual schema to use in the empty database, otherwise automatic generation will be used
   against RAD attributes. This function memoizes the resulting database for speed.

   See `reset-test-schema`."
  ([all-attributes schema-name]
   (empty-db-connection all-attributes schema-name nil))
  ([all-attributes schema-name txn]
   (let [h (hash {:id         schema-name
                  :attributes all-attributes})]
     (locking migrated-dbs
       (if-let [db (get @migrated-dbs h)]
         (do
           (log/info "Returning cached test db")
           (dm/mock-conn db))
         (let [base-connection (pristine-db-connection)
               conn            (dm/mock-conn (d/db base-connection))
               txn             (if (vector? txn) txn (automatic-schema all-attributes schema-name))]
           (log/debug "Transacting schema: " (with-out-str (pprint txn)))
           @(d/transact conn txn)
           (let [db (d/db conn)]
             (swap! migrated-dbs assoc h db)
             (dm/mock-conn db))))))))

(defn reset-migrated-dbs!
  "Forget the cached versions of test databases obtained from empty-db-connection."
  []
  (reset! migrated-dbs {}))

(defn mock-resolver-env
  "Returns a mock env that has the ::connections and ::databases keys that would be present in
  a properly-set-up pathom resolver `env` for a given single schema. This should be called *after*
  you have seeded data against a `connection` that goes with the given schema.

  * `schema` - A schema name
  * `connection` - A database connection that is connected to a database with that schema."
  [schema connection]
  {::connections {schema connection}
   ::databases   {schema (atom (d/db connection))}})

(defn add-datomic-env
  "Adds runtime Datomic info to pathom `env` so that resolvers will work correctly.

  `database-connection-map` is a map from schema name (keyword) to database connection.

  NOTE: You do not have to use anything in this entire adapter *except* this function. You may use
  your own schema and connection management strategy as long as you use this to augment your
  Pathom env during request processing."
  [env database-connection-map]
  (let [databases (sp/transform [sp/MAP-VALS] (fn [v] (atom (d/db v))) database-connection-map)]
    (-> env
      (assoc
        ::connections database-connection-map
        ::databases databases)
      (update ::form/save-handlers (fnil conj []) save-form!)
      (update ::form/delete-handlers (fnil conj []) delete-entity!))))
