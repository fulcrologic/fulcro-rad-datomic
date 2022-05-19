(ns com.fulcrologic.rad.database-adapters.indexed-access-spec
  (:require
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [com.fulcrologic.rad.database-adapters.indexed-access-checks :refer [run-checks]]
    [datomic.api :as d]
    [fulcro-spec.core :refer [specification]]))

(use-fixtures :once
  (fn [t]
    (datomic/reset-migrated-dbs!)
    (t)))

(specification "Datomic On-Prem Indexed Access"
  (run-checks {:datoms          (fn datoms-adapter
                                  [db {:keys [index components]}] (apply d/datoms db index components))
               :index-pull      d/index-pull
               :transact        (fn [c {:keys [tx-data]}] @(d/transact c tx-data))
               :db              d/db
               :make-connection datomic/empty-db-connection}))
