(ns com.fulcrologic.rad.database-adapters.datomic-options)


(def native-id?
  "If true it will map the given ID attribute (which must be type long) to :db/id."
  :com.fulcrologic.rad.database-adapters.datomic/native-id?)

(def attribute-schema
  "If true it will map the given ID attribute (which must be type long) to :db/id."
  :com.fulcrologic.rad.database-adapters.datomic/attribute-schema)

(def connections
  "A map, keyed by schema, of the database connection that should be used\nin the context of the current request."
  :com.fulcrologic.rad.database-adapters.datomic/connections)

(def databases
  "A map, keyed by schema, of the most recent database value that should be used in the context of the current request
  (for consistent reads across multiple resolvers)."
  :com.fulcrologic.rad.database-adapters.datomic/databases)

