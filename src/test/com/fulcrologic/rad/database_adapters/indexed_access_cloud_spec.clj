(ns com.fulcrologic.rad.database-adapters.indexed-access-cloud-spec
  (:require
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.rad.database-adapters.datomic-cloud :as datomic]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.indexed-access-checks :as checks :refer [run-checks]]
    [com.fulcrologic.rad.type-support.date-time :as dt]
    [dev-local-tu.core :as dev-local-tu]
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
                :make-connection (fn [& args]
                                   (with-open [db-env (dev-local-tu/test-env)]
                                     (let [config {:datomic/env         :test
                                                   :datomic/schema      :main
                                                   :datomic/database    (str (gensym "test-database"))
                                                   :datomic/test-client (:client db-env)}
                                           conn   (datomic/start-database! schema-attributes config {})]
                                       conn))))))

