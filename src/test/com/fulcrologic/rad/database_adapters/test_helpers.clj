(ns com.fulcrologic.rad.database-adapters.test-helpers
  (:require
    [com.fulcrologic.rad.ids :as ids]
    [datomic.client.api :as d]))

(defn client-test-conn
  "Create a datomic database (datomic client API)"
  []
  (let [dbname (str (gensym "test-database"))
        client (d/client {:server-type :datomic-local
                          :system      (str (ids/new-uuid))
                          :storage-dir :mem})
        _      (d/create-database client {:db-name dbname})
        conn   (d/connect client {:db-name dbname})]
    conn))

