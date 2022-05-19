(ns com.fulcrologic.rad.test-schema.person
  (:require
    [com.fulcrologic.rad.attributes :refer [defattr]]
    [com.fulcrologic.rad.attributes-options :as ao]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.wsscode.pathom.connect :as pc]))

(defattr id ::id :long
  {ao/identity?   true
   do/native-id?  true
   ao/schema      :production
   ::pc/transform (fn [resolver]
                    (assoc resolver ::transform-succeeded true))})

(defattr full-name ::full-name :string
  {ao/schema     :production
   ao/identities #{::id}
   ao/required?  true})

(defattr role ::role :enum
  {ao/schema            :production
   ao/identities        #{::id}
   ao/enumerated-values #{:user :admin}
   ao/cardinality       :one})

(defattr permissions ::permissions :enum
  {ao/schema            :production
   ao/identities        #{::id}
   ao/enumerated-values #{:read :write :execute}
   ao/cardinality       :many})

(defattr email ::email :string
  {ao/schema     :production
   ao/identities #{::id}
   ao/required?  true})

(defattr primary-address ::primary-address :ref
  {ao/target     :com.fulcrologic.rad.test-schema.address/id
   ao/schema     :production
   ao/identities #{::id}})

(defattr addresses ::addresses :ref
  {ao/target      :com.fulcrologic.rad.test-schema.address/id
   ao/cardinality :many
   ao/schema      :production
   ao/identities  #{::id}})

(defattr things ::things :ref
  {ao/target      :com.fulcrologic.rad.test-schema.thing/id
   ao/cardinality :many
   ao/schema      :production
   ao/identities  #{::id}})

(def attributes [id full-name email primary-address addresses role permissions])
