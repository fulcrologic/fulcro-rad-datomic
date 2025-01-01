(ns com.fulcrologic.rad.database-adapters.datomic-cloud-async-resolvers-test
  (:require
    [clojure.core.async :as async]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.datomic-cloud :as datomic]
    [com.fulcrologic.rad.database-adapters.datomic-cloud-async-resolvers :as car]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.test-helpers :refer [client-test-conn]]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.pathom-async :as p.async]
    [com.fulcrologic.rad.test-schema.address :as address]
    [com.fulcrologic.rad.test-schema.person :as person]
    [com.fulcrologic.rad.test-schema.thing :as thing]
    [datomic.client.api.async :as d]
    [datomic.client.api :as dsync]
    [clojure.test :refer [use-fixtures]]
    [fulcro-spec.core :refer [=> =fn=> assertions component specification]]))

(def ref-schema
  [{:db/ident       :item/cat
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :item/subcats
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/id "ca" :db/ident :item.cat/a}
   {:db/id "cb" :db/ident :item.cat/b}])

(specification "dbid->ident"
  (let [c  (client-test-conn)
        _  (async/<!! (d/transact c {:tx-data ref-schema}))
        {{:strs [A]} :tempids} (async/<!! (d/transact c {:tx-data [{:db/id        "A"
                                                                    :item/cat     :item.cat/a
                                                                    :item/subcats #{:item.cat/a :item.cat/b}}]}))
        db (d/db c)
        v  (async/<!! (d/q {:query '[:find ?a ?b
                                     :in $ ?e
                                     :keys :item/cat :item/subcats
                                     :where
                                     [?e :item/cat ?a]
                                     [?e :item/subcats ?b]]
                            :args  [db A]}))
        nv (async/<!! (car/replace-ref-types* db #{:item/cat :item/subcats} v))]
    (assertions
      "Looks up ref ids and replaces them with the keywords"
      (set nv) => #{{:item/cat :item.cat/a :item/subcats :item.cat/b}
                    {:item/cat :item.cat/a :item/subcats :item.cat/a}})))

(specification "pull-*"
  (let [c  (client-test-conn)
        _  (async/<!! (d/transact c {:tx-data ref-schema}))
        {{:strs [A B]} :tempids} (async/<!! (d/transact c {:tx-data [{:db/id        "A"
                                                                      :item/cat     :item.cat/a
                                                                      :item/subcats #{:item.cat/a :item.cat/b}}
                                                                     {:db/id        "B"
                                                                      :item/cat     :item.cat/b
                                                                      :item/subcats #{:item.cat/b}}]}))
        db (d/db c)
        v1 (async/<!! (car/pull-* db [:item/cat :item/subcats] #{:item/cat :item/subcats} A))
        vs (async/<!! (car/pull-* db [:item/cat :item/subcats] #{:item/cat :item/subcats} [A B]))]
    (assertions
      "Can look up just one thing"
      v1 => {:item/cat     :item.cat/a
             :item/subcats [:item.cat/a :item.cat/b]}
      "Can look up many things (in order)"
      vs => [{:db/id        A
              :item/cat     :item.cat/a
              :item/subcats [:item.cat/a :item.cat/b]}
             {:db/id        B
              :item/cat     :item.cat/b
              :item/subcats [:item.cat/b]}])))

(defattr composite-tuple ::person/name+email :tuple
  {::attr/identities   #{::person/id}
   ::attr/schema       :production
   do/attribute-schema {:db/tupleAttrs [::person/full-name ::person/email]}})


(def all-attributes (vec (concat person/attributes [composite-tuple] address/attributes thing/attributes)))
(def key->attribute (into {}
                      (map (fn [{::attr/keys [qualified-key] :as a}]
                             [qualified-key a]))
                      all-attributes))

(def ^:dynamic *conn* nil)
(def ^:dynamic *env* {})

(defn with-env [tests]
  (let [dbname (str (gensym "test-database"))
        client (dsync/client {:server-type :datomic-local
                          :system      (str (ids/new-uuid))
                          :storage-dir :mem})]
    (try
      (let [config {:datomic/env         :test
                    :datomic/schema      :production
                    :datomic/database    dbname
                    :datomic/test-client client}
            conn   (datomic/start-database! all-attributes config {})]
        (binding [*conn* conn
                  *env*  {::attr/key->attribute key->attribute
                          do/connections        {:production conn}}]
          (tests)))
      (finally
        (dsync/delete-database client {:db-name dbname})))))

(use-fixtures :each with-env)

(defn runnable? [txn]
  (try
    (d/transact *conn* {:tx-data txn})
    true
    (catch Exception e
      (.getMessage e))))

(specification "Pathom parser integration (save + generated resolvers)"
  (let [save-middleware     (datomic/wrap-datomic-save)
        delete-middleware   (datomic/wrap-datomic-delete)
        automatic-resolvers (car/generate-resolvers all-attributes :production)
        parser              (p.async/new-async-parser {}
                              [(attr/pathom-plugin all-attributes)
                               (form/pathom-plugin save-middleware delete-middleware)
                               (datomic/pathom-plugin (fn [env] {:production *conn*}))]
                              [automatic-resolvers form/resolvers])]
    (component "Saving new items (native ID)"
      (let [temp-person-id (tempid/tempid)
            delta          {[::person/id temp-person-id] {::person/id        {:after temp-person-id}
                                                          ::person/full-name {:after "Bob"}}}
            {::form/syms [save-form]} (async/<!! (parser {} `[{(form/save-form ~{::form/id        temp-person-id
                                                                       ::form/master-pk ::person/id
                                                                       ::form/delta     delta}) [:tempids ::person/id ::person/full-name]}]))
            {:keys [tempids]} save-form
            real-id        (get tempids temp-person-id)
            entity         (dissoc save-form :tempids)]
        (assertions
          "Includes the remapped (native) ID for native id attribute"
          (pos-int? real-id) => true
          "Returns the newly-created attributes"
          entity => {::person/id        real-id
                     ::person/full-name "Bob"})))
    (component "Saving new items (generated ID)"
      (let [temp-address-id (tempid/tempid)
            delta           {[::address/id temp-address-id] {::address/id     {:after temp-address-id}
                                                             ::address/street {:after "A St"}}}
            {::form/syms [save-form]} (async/<!! (parser {} `[{(form/save-form ~{::form/id        temp-address-id
                                                                       ::form/master-pk ::address/id
                                                                       ::form/delta     delta}) [:tempids ::address/id ::address/street]}]))
            {:keys [tempids]} save-form
            real-id         (get tempids temp-address-id)
            entity          (dissoc save-form :tempids)]
        (assertions
          "Includes the remapped (UUID) ID for id attribute"
          (uuid? real-id) => true
          "Returns the newly-created attributes"
          entity => {::address/id     real-id
                     ::address/street "A St"})
        (component "id resolver"
          (assertions
            "Can return :db/id when requested"
            (-> (async/<!! (parser {} [{[::address/id real-id] [:db/id]}]))
              first val :db/id) =fn=> int?))))
    (component "Saving a tree"
      (let [temp-person-id  (tempid/tempid)
            temp-address-id (tempid/tempid)
            delta           {[::person/id temp-person-id]
                             {::person/id              {:after temp-person-id}
                              ::person/role            {:after :com.fulcrologic.rad.test-schema.person.role/admin}
                              ::person/primary-address {:after [::address/id temp-address-id]}}

                             [::address/id temp-address-id]
                             {::address/id     {:after temp-address-id}
                              ::address/street {:after "A St"}}}
            {::form/syms [save-form]} (async/<!! (parser {} `[{(form/save-form ~{::form/id        temp-person-id
                                                                       ::form/master-pk ::person/id
                                                                       ::form/delta     delta})
                                                     [:tempids ::person/id ::person/role
                                                      {::person/primary-address [::address/id ::address/street]}]}]))
            {:keys [tempids]} save-form
            addr-id         (get tempids temp-address-id)
            person-id       (get tempids temp-person-id)
            entity          (dissoc save-form :tempids)]
        (assertions
          "Returns the newly-created graph"
          entity => {::person/id              person-id
                     ::person/role            :com.fulcrologic.rad.test-schema.person.role/admin
                     ::person/primary-address {::address/id     addr-id
                                               ::address/street "A St"}})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Attr Options Tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Attribute Options"
  (let [person-resolver (first (datomic/generate-resolvers person/attributes :production))]
    (component "defattr applies ::pc/transform to the resolver map"
      (assertions
        "person resolver has been transformed by ::pc/transform"
        (::person/transform-succeeded person-resolver) => true))))
