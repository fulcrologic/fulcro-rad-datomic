(ns com.fulcrologic.rad.database-adapters.indexed-access-cloud-spec
  (:require
    [com.fulcrologic.rad.database-adapters.datomic-cloud :as datomic]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.indexed-access-checks :as checks :refer [run-checks]]
    [datomic.client.api :as d]
    [dev-local-tu.core :as dev-local-tu]
    [fulcro-spec.core :refer [specification]]))

(def schema-attributes (map
                         (fn [a]
                           (if (contains? a do/attribute-schema)
                             (update a do/attribute-schema dissoc :db/index)
                             a))
                         checks/all-attributes))

(specification "Datomic Cloud Indexed Access"
  (run-checks {:datoms          d/datoms
               :index-pull      d/index-pull
               :transact        d/transact
               :db              d/db
               :make-connection (fn [& args]
                                  (with-open [db-env (dev-local-tu/test-env)]
                                    (let [config {:datomic/env         :test
                                                  :datomic/schema      :main
                                                  :datomic/database    (str (gensym "test-database"))
                                                  :datomic/test-client (:client db-env)}
                                          conn   (datomic/start-database! schema-attributes config {})]
                                      conn)))}))
