(ns com.fulcrologic.rad.database-adapters.indexed-access
  "Generalized functions for pulling ranges of data from Datomic efficiently, which allows
   for offset/limit scans in client-server interactions. The `index-pull` and `datoms` API
   calls from Datomic are abstracted so that on-prem or cloud can be used as the target
   database."
  (:require
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [taoensso.timbre :as log]))

(defn index-scan
  "Scan a range of the index on `attribute` (which may be a tuple or regular attribute)
   starting at the given `start` value with the given `offset` (default 0). The results are optionally
   filtered by `predicate`, and the scan terminates with a page of `limit` (default 10).

   The `env` must include:

   `::attr/key->attribute` - The map from keyword to RAD attribute for any key used in this function.
   `:datoms` - A Datomic-client compatible `datoms` function. On-prem must be adapted to fix
   `:index-pull` - A Datomic-client compatible `index-pull` function. On-prem API matches. You may want to
     use a custom index pull when on client in order to modify the network behavior. The default index pull
     in cloud (at the time of this writing) is overly aggressive at read-ahead.

   `attributes` (REQUIRED) is a list of attributes *that can be on the resolved entity* which you want to pull
   into the result maps. These can be refs or scalars, but the search `attribute`'s `identities` must
   be a non-empty intersection for them to appear.

   Any ref attributes pulled will automatically be converted to an acceptable RAD format. For example,
   If using native IDs this means that pulling something like `:purchase/customer` will return
   `{:purchase/customer {:customer/id 42}}` instead of `{:purchase/customer {:db/id 42}}`.

   The `predicate` will be passed each item before it is accumulated in order to filter them out.
   Doing so causes additional steps in the scan to accumulate up to `limit`, and should be done
   carefully (e.g. it is generally ok to filter out things that appear randomly (somewhat evenly distributed)
   in the order being scanned, but filtering out things that are common will result in a
   very inefficient scan of a large portion of the index, and should be accomplished using
   other means (a new tuple or a direct `q`).

   `start` (required) must match the attribute's type. For tuples this means
   it must be a vector of values that are compatible with the underlying tuple (and nil has meaning to
   Datomic in this case). The start MAY use a native value on refs whose target RAD attribute is a non
   native identity. Doing so will
   internally do the proper conversion on the Datomic index to native, and the `predicate` and `while` will
   see the *non-native* value if the target reference attribute in RAD is non-native.

   The `while` condition is a `(fn [item] boolean?)` that will receive each item as it is
   found (pre-filtering with predicate) and must return `true` if you
   expect there are more interesting results, and `false` if you are done with the scan.

   `reverse?` reverses the order of the scan, but you must made sure `start` correctly picks the highest
   value desired in that case.

   Returns a map with:

   ```
   {:next-offset N
    :results [{pulled data} ...]}

   `N` will be -1 if you reached the end of the index range (`max`). This allows you to call this function
   with that as your `offset` in order to get the next page of results.

   WARNING: Index scanning continues until the end of the given index UNLESS you specify a while condition. You
   *should* usually supply one.
   ```"
  [db
   {::attr/keys [key->attribute]
    :keys       [datoms index-pull]
    :as         env}
   {::attr/keys [qualified-key] :as attribute}
   {:keys [start attributes while predicate limit offset reverse?]
    :or   {limit 10 offset 0}}]
  (when (empty? attributes)
    (throw (IllegalArgumentException. "`attributes` MUST contain something, or your results would be empty")))
  (when (nil? start)
    (throw (IllegalArgumentException. "`start` MUST be non-nil")))
  (let [scan-count    (volatile! 0)
        ref?          (fn [{::attr/keys [type]}] (= :ref type))
        stopx         (take-while (fn [item]
                                    (vswap! scan-count inc)
                                    (if while
                                      (while item)
                                      true)))
        eql           (attr/attributes->eql attributes)
        selector      (common/pathom-query->datomic-query (vals key->attribute) eql)
        filterx       (when predicate (filter predicate))
        patch-results (map (fn [item] (common/datomic-result->pathom-result key->attribute eql item)))
        transducers   (cond-> [stopx patch-results]
                        filterx (conj filterx)
                        :then (conj (take limit)))
        transducer    (apply comp transducers)
        tupleAttrs    (get-in attribute [do/attribute-schema :db/tupleAttrs])
        start-attrs   (mapv key->attribute (or tupleAttrs [qualified-key]))
        fix-ref-value (fn [attr v]
                        (if (and (ref? attr) (not (int? v)))
                          (common/id->eid db env attr v)
                          v))
        start-values  (map fix-ref-value start-attrs (cond
                                                       (vector? start) start
                                                       (nil? start) nil
                                                       :else [start]))
        start         (cond-> [qualified-key start-values]
                        (not tupleAttrs) (-> flatten vec))
        results       (into []
                        transducer
                        (index-pull db (log/spy :debug
                                         "index-pull"
                                         {:index    :avet
                                          :selector selector
                                          :start    start
                                          :reverse  (boolean reverse?)
                                          :offset   (or offset 0)})))
        next-offset   (if (< (count results) limit)
                        -1
                        (+ offset @scan-count))]
    {:results     results
     :next-offset next-offset}))
