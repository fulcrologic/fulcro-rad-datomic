(ns com.fulcrologic.rad.database-adapters.indexed-access-common-spec
  (:require
    [cljc.java-time.local-date :as ld]
    [cljc.java-time.local-date-time :as ldt]
    [clojure.test :refer [use-fixtures]]
    [clojure.test.check :as tc]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop :include-macros true]
    [com.fulcrologic.rad.attributes :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.indexed-access :as ia]
    [com.fulcrologic.rad.type-support.date-time :as dt]
    [fulcro-spec.core :refer [=> assertions component specification]]))

(use-fixtures :once
  (fn [t]
    (dt/with-timezone "America/Los_Angeles"
      (t))))

(def default-coercion-acts-as-identity-function
  (prop/for-all [v (gen/one-of
                     [gen/small-integer
                      gen/boolean
                      gen/big-ratio
                      gen/double
                      gen/string])
                 k gen/keyword]
    (= v (ia/default-coercion {} k v))))

(specification "default-coercion"
  (assertions
    "Acts as identity for most data"
    (:shrunk (tc/quick-check 100 default-coercion-acts-as-identity-function)) => nil)
  (component "Local Date coercion"
    (dt/with-timezone "America/Los_Angeles"
      (let [dt (ld/of 2020 1 4)]
        (assertions
          "Converts to a local inst for most keys"
          (ia/default-coercion {} :x dt) => #inst "2020-01-04T08:00:00"
          "If the keyword has the name `end`, sets it to end of day"
          (ia/default-coercion {} :x/end dt) => #inst "2020-01-05T08:00:00"))))
  (component "Local Date-time coercion"
    (dt/with-timezone "America/Los_Angeles"
      (let [dt (ldt/of 2020 1 4 12 30 0)]
        (assertions
          "Converts to a local inst"
          (ia/default-coercion {} :x dt) => #inst "2020-01-04T20:30:00"
          (ia/default-coercion {} :x/end dt) => #inst "2020-01-04T20:30:00")))))

(def exclusive-end-string-gives-next-string
  (prop/for-all [start gen/string
                 other gen/string]
    (let [next (ia/exclusive-end start)]
      (and
        (< (compare start next) 0)
        (or
          (<= (compare other start) 0)
          (<= (compare next other) 0))))))

(def exclusive-end-of-inst-gives-next-inst
  (prop/for-all [start gen/large-integer
                 other gen/large-integer]
    (let [start (dt/new-date start)
          other (dt/new-date other)
          next  (ia/exclusive-end start)]
      (and
        (< (compare start next) 0)
        (or
          (<= (compare other start) 0)
          (<= (compare next other) 0))))))

(specification "exclusive-end"
  (assertions
    (compare "" "\u0000") => -1
    "returns one bigger for integers"
    (ia/exclusive-end 1) => 2
    "returns the next string"
    (:shrunk (tc/quick-check 100 exclusive-end-string-gives-next-string)) => nil
    "returns the next Date"
    (:shrunk (tc/quick-check 100 exclusive-end-of-inst-gives-next-inst)) => nil))

(defattr invoice-owner+date+filters :invoice/owner+date+filters :tuple
  {do/attribute-schema {:db/tupleAttrs [:invoice/owner
                                        :invoice/date
                                        :invoice/number
                                        :invoice/status
                                        :invoice/customer]}})

(specification "search-parameters->range+filters" :focus
  (component "Ranges"
    (assertions
      "For a sequence of single-valued items that start the tuple: uses exclusive end on the final element to allocate a range"
      (ia/search-parameters->range+filters {} invoice-owner+date+filters {}
        {:invoice/owner 42})
      => {:range   {:start [42] :end [43]}
          :filters {}}
      (ia/search-parameters->range+filters {} invoice-owner+date+filters {}
        {:invoice/owner 42
         :invoice/date  (ld/of 2020 1 1)})
      => {:range   {:start [42 #inst "2020-01-01T08:00"] :end [42 (ia/exclusive-end #inst "2020-01-01T08:00")]}
          :filters {}}
      "Extended keyword range on integers uses exclusive-end"
      (ia/search-parameters->range+filters {} invoice-owner+date+filters {}
        {:invoice.owner/start 42
         :invoice.owner/end   50})
      => {:range   {:start [42] :end [51]}
          :filters {}}
      "Extended keyword range on local dates uses coercion, and end-of-day"
      (ia/search-parameters->range+filters {} invoice-owner+date+filters {}
        {:invoice/owner      42
         :invoice.date/start (ld/of 2020 1 1)
         :invoice.date/end   (ld/of 2020 1 31)})
      => {:range   {:start [42 #inst "2020-01-01T08"] :end [42 #inst "2020-02-01T08"]}
          :filters {}}))
  (component "Filters"
    (assertions
      "Uses explicit value when a literal"
      (ia/search-parameters->range+filters {} invoice-owner+date+filters {}
        {:invoice/owner    42
         :invoice/customer 9})
      => {:range   {:start [42] :end [43]}
          :filters {:invoice/customer 9}}
      "Supports coercion"
      (ia/search-parameters->range+filters {} invoice-owner+date+filters {}
        {:invoice/owner    42
         :invoice/customer (ld/of 2020 1 1)})
      => {:range   {:start [42] :end [43]}
          :filters {:invoice/customer #inst "2020-01-01T08:00"}})
    (let [{:keys [filters] :as params} (ia/search-parameters->range+filters {}
                                         invoice-owner+date+filters {}
                                         {:invoice.owner/start     42
                                          :invoice.owner/end       43
                                          :invoice.date/end        #inst "2020-01-01"
                                          :invoice.customer/subset #{1 3 45}})
          cust-filter (get filters :invoice/customer)
          date-filter (get filters :invoice/date)]
      (assertions
        "generates a working lambda for subsets"
        (fn? cust-filter) => true
        (cust-filter 1) => true
        (cust-filter 9) => false
        "Generates a working lambda for dates"
        (fn? date-filter) => true
        (date-filter #inst "2019-02-01") => true
        (date-filter #inst "2020-02-01") => false))))
