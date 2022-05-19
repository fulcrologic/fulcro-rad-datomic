(ns com.fulcrologic.rad.test-schema.thing
  (:require
    [com.fulcrologic.rad.attributes :refer [defattr]]
    [com.fulcrologic.rad.attributes-options :as ao]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]))

(defattr id ::id :long
  {ao/identity?  true
   do/native-id? true
   ao/schema     :production})

(defattr label ::label :string
  {ao/schema     :production
   ao/identities #{::id}
   ao/required?  true})

(def attributes [id label])
