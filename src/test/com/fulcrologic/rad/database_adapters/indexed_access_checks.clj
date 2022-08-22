(ns com.fulcrologic.rad.database-adapters.indexed-access-checks
  (:require
    [cljc.java-time.local-date :as ld]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.indexed-access :as idx]
    [com.fulcrologic.rad.pathom :as pathom]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.test-schema.index-resolver-schema :as schema]
    [com.fulcrologic.rad.type-support.date-time :as dt]
    [fulcro-spec.core :refer [=> assertions component]]
    [clojure.string :as str]
    [taoensso.timbre :as log]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [com.fulcrologic.rad.database-adapters.indexed-access :as ia]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]))

(def all-attributes schema/attributes)
(def key->attribute (attr/attribute-map all-attributes))

(defn run-checks [{:datomic-api/keys [transact db]
                   :keys             [make-connection generate-resolvers]
                   :as               api}]
  (let [env (merge api
              {::attr/key->attribute key->attribute})]
    (let [c   (make-connection all-attributes :main)
          idA (ids/new-uuid 1)
          idB (ids/new-uuid 2)
          {{:strs [A B P1 P2 P3 P4 P5 P6]} :tempids} (transact c
                                                       {:tx-data
                                                        (into
                                                          [{:db/id "A" :customer/id idA :customer/name "A"}
                                                           {:db/id "B" :customer/id idB :customer/name "B"}
                                                           {:db/id             "P1"
                                                            :purchase/id       (ids/new-uuid 100)
                                                            :purchase/date     #inst "2021-01-06T12"
                                                            :purchase/customer "A"
                                                            :purchase/amount   -100.0M}
                                                           {:db/id             "P2"
                                                            :purchase/id       (ids/new-uuid 101)
                                                            :purchase/date     #inst "2021-01-05T12"
                                                            :purchase/customer "A"
                                                            :purchase/amount   40.0M}
                                                           {:db/id             "P3"
                                                            :purchase/id       (ids/new-uuid 102)
                                                            :purchase/date     #inst "2021-01-04T12"
                                                            :purchase/customer "A"
                                                            :purchase/amount   20.0M}
                                                           {:db/id             "P4"
                                                            :purchase/id       (ids/new-uuid 103)
                                                            :purchase/date     #inst "2021-01-03T12"
                                                            :purchase/customer "A"
                                                            :purchase/amount   15.0M}
                                                           {:db/id             "P5"
                                                            :purchase/id       (ids/new-uuid 104)
                                                            :purchase/date     #inst "2021-01-02T12"
                                                            :purchase/customer "A"
                                                            :purchase/amount   10.0M}
                                                           {:db/id             "P6"
                                                            :purchase/id       (ids/new-uuid 105)
                                                            :purchase/date     #inst "2021-01-01T12"
                                                            :purchase/customer "B"
                                                            :purchase/amount   20.0M}])})
          db  (db c)]
      (component "Tuple Scan"
        (let [{:keys [range filters]} (idx/search-parameters->range+filters env schema/purchase-date+filters
                                        (attr/attribute-map schema/attributes)
                                        {:purchase/customer   A
                                         :purchase.date/start (ld/of 2021 1 1)
                                         :purchase.date/end   (ld/of 2021 1 6)})
              {:keys [start end]} range]
          (component "Raw access of datoms"
            (let [{:keys [results next-offset]} (idx/tuple-index-scan db env schema/purchase-date+filters
                                                  start end filters {})]
              (assertions
                "finds results"
                (pos? (count results)) => true
                "the results are datoms"
                (str/includes? (str (type (first results))) "Datum") => true
                "the datoms are in the right order"
                (mapv :e results) => [P5 P4 P3 P2 P1]
                "The next offset at the end is -1"
                next-offset => -1)))
          (component "Reverse order"
            (let [{:keys [results]} (idx/tuple-index-scan db env schema/purchase-date+filters
                                      start end filters {:reverse? true})]
              (assertions
                "the datoms are in the right order"
                (mapv :e results) => [P1 P2 P3 P4 P5])))
          (component "As maps"
            (let [{:keys [results]} (idx/tuple-index-scan db env schema/purchase-date+filters
                                      start end filters {:reverse? true
                                                         :maps?    true})]
              (assertions
                "The results are maps with the expected keys directly from the index"
                results => [{:db/id             P1
                             :purchase/date     #inst "2021-01-06T12"
                             :purchase/customer A
                             :purchase/amount   -100.0M}
                            {:db/id             P2
                             :purchase/date     #inst "2021-01-05T12"
                             :purchase/customer A
                             :purchase/amount   40.0M}
                            {:db/id             P3
                             :purchase/date     #inst "2021-01-04T12"
                             :purchase/customer A
                             :purchase/amount   20.0M}
                            {:db/id             P4
                             :purchase/date     #inst "2021-01-03T12"
                             :purchase/customer A
                             :purchase/amount   15.0M}
                            {:db/id             P5
                             :purchase/date     #inst "2021-01-02T12"
                             :purchase/customer A
                             :purchase/amount   10.0M}])))
          (component "As selector"
            (let [{:keys [results]} (idx/tuple-index-scan db env schema/purchase-date+filters
                                      start end filters {:reverse? true
                                                         :selector [:purchase/id]})]
              (assertions
                "The results are pulls from the entities"
                (mapv #(dissoc % :db/id) results) => [{:purchase/id (ids/new-uuid 100)}
                                                      {:purchase/id (ids/new-uuid 101)}
                                                      {:purchase/id (ids/new-uuid 102)}
                                                      {:purchase/id (ids/new-uuid 103)}
                                                      {:purchase/id (ids/new-uuid 104)}])))
          (component "pagination (forward)"
            (let [
                  {page1 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:limit 2})
                  {page2 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:offset next
                                                            :limit  2})
                  {page3 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:offset next
                                                            :limit  2})]
              (assertions
                "Gives the expected pages of data"
                (mapv :e page1) => [P5 P4]
                (mapv :e page2) => [P3 P2]
                (mapv :e page3) => [P1]
                "indicates end"
                next => -1)))
          (component "pagination (reverse)"
            (let [
                  {page1 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:reverse? true

                                                            :limit    2})
                  {page2 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:reverse? true
                                                            :offset   next
                                                            :limit    2})
                  {page3 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:reverse? true
                                                            :offset   next
                                                            :limit    2})]
              (assertions
                "Gives the expected pages of data"
                (map :e page1) => [P1 P2]
                (map :e page2) => [P3 P4]
                (map :e page3) => [P5])))
          (component "pagination (forward with sorting)"
            (let [
                  {page1 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:limit       2
                                                            :sort-column :purchase/amount})
                  {page2 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:offset      next
                                                            :sort-column :purchase/amount
                                                            :limit       2})
                  {page3 :results
                   next  :next-offset} (idx/tuple-index-scan db env schema/purchase-date+filters
                                         start end filters {:offset      next
                                                            :sort-column :purchase/amount
                                                            :limit       2})]
              (assertions
                "Gives the expected pages of data"
                (mapv :e page1) => [P1 P5]
                (mapv :e page2) => [P4 P3]
                (mapv :e page3) => [P2]
                "indicates end"
                next => -1))))
        (let [{:keys [range filters]} (idx/search-parameters->range+filters env schema/purchase-date+filters
                                        (attr/attribute-map schema/attributes)
                                        {:purchase/customer   A
                                         :purchase/amount     (fn [v] (> v 15.0M))
                                         :purchase.date/start (ld/of 2021 1 1)
                                         :purchase.date/end   (ld/of 2021 1 6)})
              {:keys [start end]} range]
          (component "Filtering by lambda"
            (let [{:keys [results]} (idx/tuple-index-scan db env schema/purchase-date+filters
                                      start end filters {})]
              (assertions
                "Has the correct sub-results"
                (mapv :e results) => [P3 P2]))))
        (let [{:keys [range filters]} (idx/search-parameters->range+filters env schema/purchase-date+filters
                                        (attr/attribute-map schema/attributes)
                                        {:purchase/customer      A
                                         :purchase.amount/subset #{15.0M 20.0M}
                                         :purchase.date/start    (ld/of 2021 1 1)
                                         :purchase.date/end      (ld/of 2021 1 6)})
              {:keys [start end]} range]
          (component "Filtering by subset"
            (let [{:keys [results]} (idx/tuple-index-scan db env schema/purchase-date+filters
                                      start end filters {})]
              (assertions
                "Has the correct sub-results"
                (mapv :e results) => [P4 P3]))))
        (let [{:keys [range filters]} (idx/search-parameters->range+filters env schema/purchase-date+filters
                                        (attr/attribute-map schema/attributes)
                                        {:purchase/date #inst "2021-01-01T12"})
              {:keys [start end]} range]
          (component "with just the first qualifier"
            (let [{:keys [results]} (idx/tuple-index-scan db env schema/purchase-date+filters
                                      start end filters {})]
              (assertions
                "Has the correct sub-results"
                (mapv :e results) => [P6])))))
      (component "Pathom indexed access resolver generation"
        (let [parser       (pathom/new-parser {}
                             [(attr/pathom-plugin all-attributes)
                              (common/pathom-plugin (fn [_] {:main c}) (:datomic-api/db api) api)]
                             [(generate-resolvers all-attributes :main)
                              (ia/generate-tuple-resolver schema/purchase-date+filters key->attribute 2)])
              query-params {:purchase.date/start    #inst "2021-01-01T12"
                            :purchase.date/end      #inst "2021-01-04T12"
                            :indexed-access/options {:limit    2
                                                     :offset   1
                                                     :reverse? true}}
              env          (merge {} api)]
          (let [results (parser env [{`(:purchase.date+filters/page ~query-params)
                                      [:next-offset
                                       {:results [:purchase/id {:purchase/customer [:customer/id
                                                                                    :customer/name]}]}]}])]
            (assertions
              results => #:purchase.date+filters{:page
                                                 {:next-offset 3,
                                                  :results
                                                  [#:purchase{:id       (ids/new-uuid 103)
                                                              :customer #:customer{:id   (ids/new-uuid 1)
                                                                                   :name "A"}}
                                                   #:purchase{:id       (ids/new-uuid 104)
                                                              :customer #:customer{:id   (ids/new-uuid 1)
                                                                                   :name "A"}}]}})))))))
