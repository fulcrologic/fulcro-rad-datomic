(ns com.fulcrologic.rad.database-adapters.indexed-access-spec
  (:require
    [cljc.java-time.local-date :as ld]
    [clojure.set :as set]
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.indexed-access :as idx]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.pathom :as pathom]
    [com.fulcrologic.rad.test-schema.index-resolver-schema :as schema]
    [com.fulcrologic.rad.type-support.date-time :as dt]
    [datomic.api :as d]
    [fulcro-spec.core :refer [specification assertions component when-mocking =1x=> =>]]
    [taoensso.timbre :as log]))

(use-fixtures :once
  (fn [t]
    (datomic/reset-migrated-dbs!)
    (t)))

(def all-attributes schema/attributes)
(def key->attribute (into {}
                      (map (fn [{::attr/keys [qualified-key] :as a}]
                             [qualified-key a]))
                      all-attributes))
(def env {:index-pull           d/index-pull
          :datoms               (fn datoms-adapter
                                  [db {:keys [index components]}] (apply d/datoms db index components))
          ::attr/key->attribute key->attribute})

(specification "Basic indexed access"
  (let [c          (datomic/empty-db-connection all-attributes :main)
        idA        (ids/new-uuid 1)
        idB        (ids/new-uuid 2)
        idC        (ids/new-uuid 3)
        {{:strs [A B C]} :tempids} @(d/transact c
                                      (into
                                        [{:db/id "A" :customer/id idA :customer/name "A"}
                                         {:db/id "B" :customer/id idB :customer/name "B"}
                                         {:db/id "C" :customer/id idC :customer/name "C"}]
                                        (map
                                          (fn [idx]
                                            (let [c    (mod idx 3)
                                                  m    (inc (mod idx 12))
                                                  d    (inc (mod idx 28))
                                                  cust (case c
                                                         0 "A"
                                                         1 "B"
                                                         2 "C")]
                                              {:db/id             (str "P" idx)
                                               :purchase/id       (ids/new-uuid (+ 1000 idx))
                                               :purchase/date     (dt/local-date->inst "America/Los_Angeles"
                                                                    (ld/of 2021 m d))
                                               :purchase/customer cust
                                               :purchase/amount   (+ 10M (* 2.5M idx))}))
                                          (range 0 1000))))
        db         (d/db c)
        customerA? (fn [{:purchase/keys [customer]}]
                     (= {:customer/id idA} customer))]
    (component "Scanning for a page"
      (let [{:keys [results next-offset]} (idx/index-scan db env schema/purchase-customer
                                            {:start      A
                                             :limit      10
                                             :attributes [schema/purchase-customer]
                                             :while      customerA?})]
        (assertions
          "Scanning for a single customers data gives the correct number of results"
          (count results) => 10
          "Indicates where the next offset is"
          next-offset => 10)))
    (component "Scanning to the end"
      (let [{:keys [results next-offset]} (idx/index-scan db env schema/purchase-customer
                                            {:start      A
                                             :limit      1000
                                             :attributes [schema/purchase-customer]
                                             :while      customerA?})]
        (assertions
          "Returns just the items desired"
          (count results) => 334
          (every? #(= idA (get-in % [:purchase/customer :customer/id])) results) => true
          "Returns -1 for the next offset"
          next-offset => -1)))
    (component "Starting from a non-native value on a ref (uuid instead of :db/id)"
      (let [{:keys [results next-offset]} (idx/index-scan db env schema/purchase-customer
                                            {:start      idB
                                             :attributes [schema/purchase-customer]
                                             :limit      1})]
        (assertions
          "Finds the correct data"
          (first results) => {:purchase/customer {:customer/id idB}})))
    (component "Including attributes in the pull"
      (let [{:keys [results next-offset]} (idx/index-scan db env schema/purchase-customer
                                            {:start      A
                                             :limit      1000
                                             :attributes [schema/purchase-customer
                                                          schema/purchase-date
                                                          schema/purchase-amount]
                                             :while      customerA?})]
        (assertions
          "Returns just the items desired"
          (count results) => 334
          "The items have the desired result attributes"
          (first results) => {:purchase/date     (dt/local-date->inst "America/Los_Angeles"
                                                   (ld/of 2021 1 1))
                              :purchase/amount   10M
                              :purchase/customer {:customer/id idA}}
          (every? #(= idA (get-in % [:purchase/customer :customer/id])) results) => true
          "Returns -1 for the next offset"
          next-offset => -1)))))

(specification "Tuple access" :focus
  (let [c             (datomic/empty-db-connection all-attributes :main)
        idA           (ids/new-uuid 1)
        idB           (ids/new-uuid 2)
        idC           (ids/new-uuid 3)
        {{:strs [A B C]} :tempids} @(d/transact c
                                      (into
                                        [{:db/id "A" :customer/id idA :customer/name "A"}
                                         {:db/id "B" :customer/id idB :customer/name "B"}
                                         {:db/id "C" :customer/id idC :customer/name "C"}]
                                        (map
                                          (fn [idx]
                                            (let [c    (mod idx 3)
                                                  m    (inc (mod idx 12))
                                                  d    (inc (mod idx 28))
                                                  cust (case c
                                                         0 "A"
                                                         1 "B"
                                                         2 "C")]
                                              {:db/id             (str "P" idx)
                                               :purchase/id       (ids/new-uuid (+ 1000 idx))
                                               :purchase/date     (dt/local-date->inst "America/Los_Angeles"
                                                                    (ld/of 2021 m d))
                                               :purchase/customer cust
                                               :purchase/amount   (+ 10M (* 2.5M idx))}))
                                          (range 0 20))))
        db            (d/db c)
        start-date    (dt/local-date->inst "America/Los_Angeles" (ld/of 2021 2 1))
        expected-date (dt/local-date->inst "America/Los_Angeles" (ld/of 2021 2 2))]
    (component "Forward scan"
      (let [{:keys [results next-offset]} (idx/index-scan db env schema/purchase-customer+date
                                            {:start      [idB start-date]
                                             :limit      1
                                             :attributes [schema/purchase-customer
                                                          schema/purchase-date
                                                          schema/purchase-amount]})]
        (assertions
          "Finds the correct first result with desired attributes"
          (first results) => {:purchase/customer {:customer/id idB}
                              :purchase/date     expected-date
                              :purchase/amount   12.5M}
          "Gives a valid next offset"
          next-offset => 1)))
    (component "Reverse scan"
      (let [start-date (dt/local-date->inst "America/Los_Angeles" (ld/of 2021 3 4))
            {:keys [results next-offset]} (idx/index-scan db env schema/purchase-customer+date
                                            {:start      [idB start-date]
                                             :reverse?   true
                                             :limit      100
                                             :while      (fn [item]
                                                           (= idB (get-in item [:purchase/customer :customer/id])))
                                             :attributes [schema/purchase-customer
                                                          schema/purchase-date
                                                          schema/purchase-amount]})]
        (assertions
          "Finds the correct results in reverse order"
          results => [#:purchase{:customer
                                 #:customer{:id idB},
                                 :date   #inst "2021-02-14T08:00:00.000-00:00",
                                 :amount 42.5M}
                      #:purchase{:customer
                                 #:customer{:id idB},
                                 :date   #inst "2021-02-02T08:00:00.000-00:00",
                                 :amount 12.5M}])))))
