(ns com.fulcrologic.rad.database-adapters.datomic-common)

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
