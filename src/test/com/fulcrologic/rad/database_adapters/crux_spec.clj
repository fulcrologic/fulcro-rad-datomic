(ns com.fulcrologic.rad.database-adapters.crux-spec
  (:require
   [fulcro-spec.core :refer [specification assertions component behavior when-mocking]]
   [com.fulcrologic.rad.ids :as ids]
   [com.fulcrologic.rad.form :as form]
   [com.fulcrologic.rad.test-schema.person :as person]
   [com.fulcrologic.rad.test-schema.address :as address]
   [com.fulcrologic.rad.test-schema.thing :as thing]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.crux.wrap-crux-save :as wcs]
   [com.fulcrologic.rad.database-adapters.crux :as crux-adapter]
   [com.fulcrologic.rad.database-adapters.crux-options :as co]
   [crux.api :as crux]
   [clojure.test :refer [use-fixtures]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.pathom :as pathom]
   [taoensso.timbre :as log]))

(declare =>)

(def all-attributes (vec (concat person/attributes address/attributes thing/attributes)))
(def key->attribute (into {}
                          (map (fn [{::attr/keys [qualified-key] :as a}]
                                 [qualified-key a]))
                          all-attributes))
(def ^:dynamic *node* nil)
(def ^:dynamic *env* {})

(defn with-env [tests]
  (let [node (:main (crux-adapter/start-databases {co/databases {:main {}}}))]
    (binding [*node* node
              *env* {::attr/key->attribute key->attribute
                     co/nodes       {:production node}}]
      (tests)
      (.close node))))

(use-fixtures :each with-env)

(specification "save-form!"
               (let [tx (crux/submit-tx *node* [[:crux.tx/put {:crux.db/id (ids/new-uuid 1) ::address/id (ids/new-uuid 1) ::address/street "A St"}]])
                     _ (crux/await-tx *node* tx)
                     tempid1 (tempid/tempid (ids/new-uuid 100))
                     delta {[::person/id tempid1]           {::person/id              tempid1
                                                             ::person/full-name       {:after "Bob"}
                                                             ::person/primary-address {:after [::address/id (ids/new-uuid 1)]}
                                                             ::person/role            :com.fulcrologic.rad.test-schema.person.role/admin}
                            [::address/id (ids/new-uuid 1)] {::address/street {:before "A St" :after "A1 St"}}}]
                 (let [{:keys [tempids]} (wcs/save-form! *env* {::form/delta delta})
                       real-id (get tempids tempid1)
                       person (-> (crux/db *node*)
                                  (crux/q '{:find [(eql/project ?uid [::person/full-name {::person/primary-address [::address/street]}])]
                                            :in [id]
                                            :where [[?uid :crux.db/id id]]}
                                          real-id)
                                  ffirst)
                       ]
                   (assertions
                    "Gives a proper remapping"
                    (uuid? (get tempids tempid1)) => true
                    "Updates the db"
                    person => {::person/full-name       "Bob"
                               ::person/primary-address {::address/street "A1 St"}}))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; Round-trip tests
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Pathom parser integration (save + generated resolvers)"
               (let [save-middleware (crux-adapter/wrap-crux-save)
                     delete-middleware (crux-adapter/wrap-crux-delete)
                     automatic-resolvers (crux-adapter/generate-resolvers all-attributes :production)
                     parser (pathom/new-parser {}
                                               [(attr/pathom-plugin all-attributes)
                                                (form/pathom-plugin save-middleware delete-middleware)
                                                (crux-adapter/pathom-plugin (fn [env] {:production *node*}))]
                                               [automatic-resolvers form/resolvers])]
                 (component "Saving new items"
                            (let [temp-person-id (tempid/tempid)
                                  delta {[::person/id temp-person-id] {::person/id        {:after temp-person-id}
                                                                       ::person/full-name {:after "Bob"}}}
                                  {::form/syms [save-form]} (parser {} `[{(form/save-form ~{::form/id        temp-person-id
                                                                                            ::form/master-pk ::person/id
                                                                                            ::form/delta     delta}) [:tempids ::person/id ::person/full-name]}])
                                  {:keys [tempids]} save-form
                                  real-id (get tempids temp-person-id)
                                  entity (dissoc save-form :tempids)]
                              (assertions
                               "Includes the remapped (native) ID for native id attribute"
                               (uuid? real-id) => true
                               "Returns the newly-created attributes"
                               entity => {::person/id        real-id
                                          ::person/full-name "Bob"})))
                 (component "Saving new items (generated ID)"
                            (let [temp-address-id (tempid/tempid)
                                  delta {[::address/id temp-address-id] {::address/id     {:after temp-address-id}
                                                                         ::address/street {:after "A St"}}}
                                  {::form/syms [save-form]} (parser {} `[{(form/save-form ~{::form/id        temp-address-id
                                                                                            ::form/master-pk ::address/id
                                                                                            ::form/delta     delta}) [:tempids ::address/id ::address/street]}])
                                  {:keys [tempids]} save-form
                                  real-id (get tempids temp-address-id)
                                  entity (dissoc save-form :tempids)]
                              (assertions
                               "Includes the remapped (UUID) ID for id attribute"
                               (uuid? real-id) => true
                               "Returns the newly-created attributes"
                               entity => {::address/id     real-id
                                          ::address/street "A St"})))
                 (component "Saving a tree"
                            (let [temp-person-id (tempid/tempid)
                                  temp-address-id (tempid/tempid)
                                  delta {[::person/id temp-person-id]
                                         {::person/id              {:after temp-person-id}
                                          ::person/role            {:after :com.fulcrologic.rad.test-schema.person.role/admin}
                                          ::person/primary-address {:after [::address/id temp-address-id]}}

                                         [::address/id temp-address-id]
                                         {::address/id     {:after temp-address-id}
                                          ::address/street {:after "A St"}}}
                                  {::form/syms [save-form]} (parser {} `[{(form/save-form ~{::form/id        temp-person-id
                                                                                            ::form/master-pk ::person/id
                                                                                            ::form/delta     delta})
                                                                          [:tempids ::person/id ::person/role
                                                                           {::person/primary-address [::address/id ::address/street]}]}])
                                  {:keys [tempids]} save-form
                                  addr-id (get tempids temp-address-id)
                                  person-id (get tempids temp-person-id)
                                  entity (dissoc save-form :tempids)]
                              (assertions
                               "Returns the newly-created graph"
                               entity => {::person/id              person-id
                                          ::person/role            :com.fulcrologic.rad.test-schema.person.role/admin
                                          ::person/primary-address {::address/id     addr-id
                                                                    ::address/street "A St"}})))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; Attr Options Tests
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Attribute Options"
               (let [person-resolver (first (crux-adapter/generate-resolvers person/attributes :production))]
                 (component "defattr applies ::pc/transform to the resolver map"
                            (assertions
                             "person resolver has been transformed by ::pc/transform"
                             (do
                               (log/spy :info person-resolver)
                               (::person/transform-succeeded person-resolver)) => true))))
