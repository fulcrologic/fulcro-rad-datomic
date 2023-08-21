(ns com.fulcrologic.rad.database-adapters.indexed-access-cloud-spec
  (:require
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.rad.database-adapters.datomic-cloud :as datomic]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.indexed-access-checks :as checks :refer [run-checks]]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.type-support.date-time :as dt]
    [datomic.client.api :as d]
    [fulcro-spec.core :refer [specification]]))

(use-fixtures :once
  (fn [t]
    (dt/with-timezone "America/Los_Angeles"
      (t))))

(def schema-attributes (map
                         (fn [a]
                           (if (contains? a do/attribute-schema)
                             (update a do/attribute-schema dissoc :db/index)
                             a))
                         checks/all-attributes))

(specification "Datomic Cloud Indexed Access"
  (run-checks (assoc
                datomic/datomic-api
                :generate-resolvers datomic/generate-resolvers
                :make-connection (fn [& _args]
                                   (let [dbname (str (gensym "test-database"))
                                         client (d/client {:server-type :datomic-local
                                                           :system (str (ids/new-uuid))
                                                           :storage-dir :mem})]
                                    (try
                                      (let [config {:datomic/env         :test
                                                    :datomic/schema      :main
                                                    :datomic/database    dbname
                                                    :datomic/test-client client}
                                            conn   (datomic/start-database! schema-attributes config {})]
                                        conn)
                                      (finally
                                        (d/delete-database client {:db-name dbname}))))))))

