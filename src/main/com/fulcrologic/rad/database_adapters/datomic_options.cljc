(ns com.fulcrologic.rad.database-adapters.datomic-options)


(def native-id?
  "If true it will map the given ID attribute (which must be type long) to :db/id."
  :com.fulcrologic.rad.database-adapters.datomic/native-id?)

(def attribute-schema
  "If true it will map the given ID attribute (which must be type long) to :db/id."
  :com.fulcrologic.rad.database-adapters.datomic/attribute-schema)

