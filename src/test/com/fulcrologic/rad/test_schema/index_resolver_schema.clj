(ns com.fulcrologic.rad.test-schema.index-resolver-schema
  (:require
    [com.fulcrologic.rad.attributes :refer [defattr]]
    [com.fulcrologic.rad.attributes-options :as ao]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]))

(defattr customer-id :customer/id :uuid
  {ao/schema    :main
   ao/identity? true})

(defattr customer-name :customer/name :string
  {ao/schema           :main
   ao/required?        true
   do/attribute-schema {:db/index true}
   ao/identities       #{:customer/id}})

(defattr purchase-id :purchase/id :uuid
  {ao/schema    :main
   ao/identity? true})

(defattr purchase-amount :purchase/amount :decimal
  {ao/schema     :main
   ao/required?  true
   ao/identities #{:purchase/id}})

(defattr purchase-date :purchase/date :instant
  {ao/schema           :main
   ao/required?        true
   do/attribute-schema {:db/index true}
   ao/identities       #{:purchase/id}})

(defattr purchase-shipped? :purchase/shipped? :boolean
  {ao/schema           :main
   do/attribute-schema {:db/index true}
   ao/identities       #{:purchase/id}})

(defattr purchase-customer :purchase/customer :ref
  {ao/schema           :main
   ao/required?        true
   do/attribute-schema {:db/index true}
   ao/target           :customer/id
   ao/identities       #{:purchase/id}})

(defattr purchase-customer+date :purchase/customer+date :tuple
  {ao/schema           :main
   do/attribute-schema {:db/tupleAttrs [:purchase/customer :purchase/date]
                        :db/index      true}
   ao/identities       #{:purchase/id}})

(defattr purchase-date+filters :purchase/date+filters :tuple
  {ao/schema           :main
   do/attribute-schema {:db/tupleAttrs [:purchase/date :purchase/customer :purchase/shipped? :purchase/amount]
                        :db/index      true}
   ao/identities       #{:purchase/id}})

(def attributes [customer-id customer-name purchase-id purchase-date purchase-amount
                 purchase-customer purchase-shipped? purchase-customer+date
                 purchase-date+filters])
