(ns com.fulcrologic.rad.database-adapters.indexed-access-spec
  (:require
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [com.fulcrologic.rad.database-adapters.indexed-access-checks :refer [run-checks]]
    [com.fulcrologic.rad.type-support.date-time :as dt]
    [fulcro-spec.core :refer [specification]]))

(use-fixtures :once
  (fn [t]
    (datomic/reset-migrated-dbs!)
    (dt/with-timezone "America/Los_Angeles"
      (t))))

(specification "Datomic On-Prem Indexed Access"
  (run-checks (assoc
                datomic/datomic-api
                :generate-resolvers datomic/generate-resolvers
                :make-connection datomic/empty-db-connection)))
