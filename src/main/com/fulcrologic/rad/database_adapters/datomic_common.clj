(ns com.fulcrologic.rad.database-adapters.datomic-common
  (:require
    [com.fulcrologic.guardrails.core :refer [>defn => ?]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.rpl.specter :as sp]
    [edn-query-language.core :as eql]))

(def type-map
  {:string   :db.type/string
   :enum     :db.type/ref
   :boolean  :db.type/boolean
   :password :db.type/string
   :int      :db.type/long
   :long     :db.type/long
   :decimal  :db.type/bigdec
   :instant  :db.type/instant
   :keyword  :db.type/keyword
   :symbol   :db.type/symbol
   :tuple    :db.type/tuple
   :ref      :db.type/ref
   :uuid     :db.type/uuid})

(>defn pathom-query->datomic-query [all-attributes pathom-query]
  [::attr/attributes ::eql/query => ::eql/query]
  (let [native-id? #(and (true? (do/native-id? %)) (true? (::attr/identity? %)))
        native-ids (set (sp/select [sp/ALL native-id? ::attr/qualified-key] all-attributes))]
    (sp/transform (sp/walker keyword?) (fn [k] (if (contains? native-ids k) :db/id k)) pathom-query)))

(defn- fix-id-keys
  "Fix the ID keys recursively on result."
  [k->a ast-nodes result]
  (let [id?                (fn [{:keys [dispatch-key]}] (some-> dispatch-key k->a ::attr/identity?))
        id-key             (:key (sp/select-first [sp/ALL id?] ast-nodes))
        join-key->children (into {}
                             (comp
                               (filter #(= :join (:type %)))
                               (map (fn [{:keys [key children]}] [key children])))
                             ast-nodes)
        join-keys          (set (keys join-key->children))
        join-key?          #(contains? join-keys %)]
    (reduce-kv
      (fn [m k v]
        (cond
          (= :db/id k) (assoc m id-key v)
          (and (join-key? k) (vector? v)) (assoc m k (mapv #(fix-id-keys k->a (join-key->children k) %) v))
          (and (join-key? k) (map? v)) (assoc m k (fix-id-keys k->a (join-key->children k) v))
          :otherwise (assoc m k v)))
      {}
      result)))

(>defn datomic-result->pathom-result
  "Convert a datomic result containing :db/id into a pathom result containing the proper id keyword that was used
   in the original query."
  [k->a pathom-query result]
  [(s/map-of keyword? ::attr/attribute) ::eql/query (? coll?) => (? coll?)]
  (when result
    (let [{:keys [children]} (eql/query->ast pathom-query)]
      (if (vector? result)
        (mapv #(fix-id-keys k->a children %) result)
        (fix-id-keys k->a children result)))))
