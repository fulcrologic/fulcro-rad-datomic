(ns com.fulcrologic.rad.database-adapters.datomic-common-spec
  (:require
    [fulcro-spec.core :refer [specification assertions component behavior when-mocking]]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.test-schema.person :as person]
    [com.fulcrologic.rad.test-schema.address :as address]
    [com.fulcrologic.rad.test-schema.thing :as thing]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [fulcro-spec.core :refer [specification assertions]]
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.pathom :as pathom]
    [taoensso.timbre :as log]
    [datomic.client.api :as d]))

(declare =>)

(def all-attributes (vec (concat person/attributes address/attributes thing/attributes)))
(def key->attribute (into {}
                      (map (fn [{::attr/keys [qualified-key] :as a}]
                             [qualified-key a]))
                      all-attributes))

(def ^:dynamic *conn* nil)
(def ^:dynamic *env* {})


; TODO -- move to common-spec
(specification "native ID pull query transform" :focus
  (component "pathom-query->datomic-query"
    (let [incoming-query         [::person/id
                                  {::person/addresses [::address/id ::address/street]}
                                  {::person/things [::thing/id ::thing/label]}]
          expected-datomic-query [:db/id
                                  {::person/addresses [::address/id ::address/street]}
                                  {::person/things [:db/id ::thing/label]}]
          actual-query           (common/pathom-query->datomic-query all-attributes incoming-query)]
      (assertions
        "can convert a recursive pathom query to a proper Datomic query"
        actual-query => expected-datomic-query)))
  (component "datomic-result->pathom-result"
    (let [pathom-query    [::person/id
                           {::person/addresses [::address/id ::address/street]}
                           {::person/things [::thing/id ::thing/label]}]
          datomic-result  {:db/id             100
                           ::person/addresses [{::address/id     (ids/new-uuid 1)
                                                ::address/street "111 Main St"}]
                           ::person/things    [{:db/id        191
                                                ::thing/label "ABC"}]}
          pathom-result   (common/datomic-result->pathom-result key->attribute pathom-query datomic-result)
          expected-result {::person/id        100
                           ::person/addresses [{::address/id     (ids/new-uuid 1)
                                                ::address/street "111 Main St"}]
                           ::person/things    [{::thing/id    191
                                                ::thing/label "ABC"}]}]
      (assertions
        "can convert a recursive datomic result to a proper Pathom response"
        pathom-result => expected-result))))
