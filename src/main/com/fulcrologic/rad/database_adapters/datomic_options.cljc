(ns com.fulcrologic.rad.database-adapters.datomic-options)


(def native-id?
  "If true it will map the given ID attribute (which must be type long) to :db/id."
  :com.fulcrologic.rad.database-adapters.datomic/native-id?)

