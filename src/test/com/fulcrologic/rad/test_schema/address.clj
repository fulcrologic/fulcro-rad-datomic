(ns com.fulcrologic.rad.test-schema.address
  (:require
   [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
   [com.fulcrologic.rad.attributes-options :as ao]))

(defattr id ::id :uuid
  {ao/identity? true
   ao/schema    :production})

(defattr enabled? ::enabled? :boolean
  {ao/identities #{::id}
   ao/schema     :production})

(defattr street ::street :string
  {ao/identities #{::id}
   ao/required?  true
   ao/schema     :production})

(def attributes [id enabled? street])
