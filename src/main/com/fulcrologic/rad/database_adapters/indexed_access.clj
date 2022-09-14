(ns com.fulcrologic.rad.database-adapters.indexed-access
  "Generalized functions for pulling ranges of data from Datomic efficiently, which allows
   for offset/limit scans in client-server interactions. The `index-pull`, `index-range`,
   `pull-many`, and `datoms` API calls from Datomic are abstracted so that
   on-prem or cloud can be used as the target database."
  (:require
    [clojure.set :as set]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.database-adapters.indexed-access-options :as iao]
    [com.fulcrologic.rad.options-util :refer [narrow-keyword]]
    [com.fulcrologic.rad.type-support.date-time :as dt]
    [edn-query-language.core :as eql]
    [taoensso.timbre :as log]
    [taoensso.tufte :refer [p]])
  (:import (java.time LocalDate LocalDateTime)
           (java.util Date)))

(defn ^:deprecated index-scan
  "Prefer tuple-index-scan over this function when possible. Direct index access is faster.

   Scan a range of the index on `attribute` (which may be a tuple or regular attribute)
   starting at the given `start` value with the given `offset` (default 0). The results are optionally
   filtered by `predicate`, and the scan terminates with a page of `limit` (default 10).

   The `env` must include:

   `::attr/key->attribute` - The map from keyword to RAD attribute for any key used in this function.
   `:datomic-api/datoms` - A Datomic-client compatible `datoms` function. On-prem must be adapted to fix
   `:datomic-api/index-pull` - A Datomic-client compatible `index-pull` function. On-prem API matches. You may want to
     use a custom index pull when on client in order to modify the network behavior. The default index pull
     in cloud (at the time of this writing) is overly aggressive at read-ahead.

   The options map contains:

   `attributes` (REQUIRED unless `selector`) is a list of attributes *that can be on the resolved entity* which you want to pull
   into the result maps. These can be refs or scalars, but the search `attribute`'s `identities` must
   be a non-empty intersection for them to appear. Any ref attributes pulled will automatically be converted to an
   acceptable RAD format. For example, If using native IDs this means that pulling something like `:purchase/customer`
   will return `{:purchase/customer {:customer/id 42}}` instead of `{:purchase/customer {:db/id 42}}`. Normally
   you should use this and allow EQL resolvers to resolve any nesting; however, if you want exact control of the
   pull query use `selector` instead.

   `selector` (REQUIRED unless `attributes`). The Datomic pull selector. Used when you don't want one constructed
   from `attributes`. Will be used as-is instead of constructing a pull selector from `attributes` (which is then
   no longer needed).

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
   {::attr/keys       [key->attribute]
    :datomic-api/keys [datoms index-pull]
    :as               env}
   {::attr/keys [qualified-key] :as attribute}
   {:keys [start attributes selector while predicate limit offset reverse?]
    :or   {limit 10 offset 0}}]
  (when (or
          (and (vector? selector) (seq attributes))
          (and (nil? selector) (empty? attributes)))
    (throw (IllegalArgumentException. "`attributes` OR `selector` is required. BUT NOT BOTH.")))
  (when (nil? start)
    (throw (IllegalArgumentException. "`start` MUST be non-nil")))
  (let [datoms        (or (:datoms env) (:datomic-api/datoms env))
        index-pull    (or (:index-pull env) (:datomic-api/index-pull env))
        scan-count    (volatile! 0)
        ref?          (fn [{::attr/keys [type]}] (= :ref type))
        stopx         (take-while (fn [item]
                                    (vswap! scan-count inc)
                                    (if while
                                      (while item)
                                      true)))
        eql           (when (seq attributes) (attr/attributes->eql attributes))
        selector      (or selector (common/pathom-query->datomic-query (vals key->attribute) eql))
        filterx       (when predicate (filter predicate))
        patch-results (if eql
                        (map (fn [item] (common/datomic-result->pathom-result key->attribute eql item)))
                        (map identity))
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
                        (index-pull db {:index    :avet
                                        :selector selector
                                        :start    start
                                        :reverse  (boolean reverse?)
                                        :offset   (or offset 0)}))
        next-offset   (if (< (count results) limit)
                        -1
                        (+ offset @scan-count))]
    {:results     results
     :next-offset next-offset}))

(defn default-coercion
  "Coerce the incoming UI parameter value `v` to a database-compatible type. This function
   coerces LocalDate and LocalDateTime to java.util.Date. If the UI `k` has the name \"end\", then
   it will also advance the time of any Date or LocalDate to midnight of the next day. Must be called
   in the context of a time zone (see date-time/with-time-zone)."
  [{:datomic-api/keys [datoms]
    ::attr/keys       [key->attribute] :as pathom-env} parameter-keyword attribute-key v]
  (let [end?           (= "end" (name parameter-keyword))
        {::attr/keys [schema type target]} (get key->attribute attribute-key)
        ref-uuid->dbid (fn ref->dbid* [db target-key uuid]
                         (when (and db target-key uuid)
                           (some-> (datoms db {:index :avet :components [target-key uuid]}) first :e)))
        ref?           (= type :ref)
        uuid?          (uuid? v)
        ident?         (eql/ident? v)
        db             (when (and ref? schema)
                         (some-> pathom-env
                           (get-in [do/databases schema])
                           deref))
        local-date?    (instance? LocalDate v)]
    (cond
      (and (nil? db) ref? (or uuid? ident?)) (do
                                               (log/warn "Cannot coerce ref. db isn't in pathom env. Did you install the Datomic RAD plugin properly?")
                                               v)
      (and db ref? uuid?) (ref-uuid->dbid db target v)
      (and db ref? ident?) (ref-uuid->dbid db (first v) (second v))
      :else (cond-> v
              local-date? (dt/local-date->inst)
              (and (or (inst? v) local-date?) end?) (dt/end-of-day)
              (instance? LocalDateTime v) (dt/local-datetime->inst)))))

(defmulti exclusive-end (fn [v] (type v)))
(defmethod exclusive-end String [v] (str v "\u0000"))
(defmethod exclusive-end Date [v] (dt/new-date (inc (inst-ms v))))
(defmethod exclusive-end Integer [v] (inc v))
(defmethod exclusive-end Long [v] (inc v))
(defmethod exclusive-end :default [v] v)

(defn search-parameters->range+filters
  "Convert (RAD report) search paramters into an argument list that is usable by tuple-index-scan.

   pathom-env - A Pathom env that includes additions from RAD plugins (at least attribute and RAD Datomic)
   tuple-attribute - The tuple that will be scanned
   k->a - The RAD attribute lookup map (keyword -> attribute)
   search-parameters - Valid search parameters

   Search is a map of keys that are part of the tuple, and can include, for a key :ns/nm in the tuple:

   `:ns/nm value` - A specific value that exactly matches the schema type of that key
   `:ns/nm uuid(s)` - IF the key is a ref (many or one), then UUIDs are acceptable even though the low-level value
     will be integer(s). This function will use the database in the `pathom-env` to convert such UUIDs to their low-level
     :db/id.
   `:ns/nm ident(s)` - Same as the uuid case above.
   `:ns/nm (fn [v] boolean)` - A predicate function. Of course the client cannot pass code, so to use this you'd have
     to leverage the `iao/pathom-env->search-parameters` option on the tuple to convert incoming parameters into a predicate.
   `:ns.nm/start value` - A value of the correct type that should act as a starting value of a range. Primarily used for inst.
   `:ns.nm/end value` - A value of the correct type that should act as an EXCLUSIVE ending value of a range. Primarily used for inst.
     NOTE: The default system will coerce local dates to the end of the (timezone-appropriate) date (e.g. midnight the next day)
   `:ns.nm/subset #{value...}` - A subset of values that are acceptable for the `:ns/nm` element of the tuple.

   Returns a map containing:

   ```
   {:range {:start tuple-compatible-inclusive-value
            :end tuple-compatible-exclusive-value}
    :filters tuple-compatible-filter-map}
   ```

   Additional Notes:

   * If you include a value or start-without-an-end for a tuple attribute that will be used in the `range`
     (determined by left-to-right scanning of the tuple order), then the multimethod `exclusive-end` will be used on
     that value to derive the end. This has built-in implementations for String (add \u0000 to the end), Date (+ 1ms),
     Integer, and Long (inc).
   * The `iao/coerce` setting on attribute within the tuple (e.g. tuple contains :invoice/date, then the attribute invoice-date
     is the one we mean), then that function will be used to coerce the incoming search paramter to a compatible value. The
     `default-coercion` handles:
     ** fixing UUID/idents on refs
     ** LocalDateTime (to inst)
     ** LocalDate (to inst). If the search parameter is using `:ns.nm/end`, then it will be pushed to end-of-day
        (midnight next day), otherwise it will use local time midnight as the time.
   "
  ([{::attr/keys [key->attribute] :as pathom-env} tuple-attribute search-parameters]
   (search-parameters->range+filters pathom-env tuple-attribute key->attribute search-parameters))
  ([pathom-env tuple-attribute k->a {:keys [row-filter] :as search-parameters}]
   (when-not k->a
     (throw "Cannot map keys to attributes. Check pathom-env and make sure you have the RAD attribute plugin installed."))
   (let [tupleAttrs (get-in tuple-attribute [do/attribute-schema :db/tupleAttrs])
         _          (assert (vector? tupleAttrs) "Tuple must have schema tupleAttrs defined")
         result     (reduce
                      (fn [{:keys [filtering?] :as acc} k]
                        (let [{::keys [coerce]
                               :or    {coerce default-coercion}} (k->a k)
                              start-key    (narrow-keyword k "start")
                              end-key      (narrow-keyword k "end")
                              subset-key   (narrow-keyword k "subset")
                              value        (some->> (get search-parameters k) (coerce pathom-env k k))
                              subset-value (some->> (get search-parameters subset-key)
                                             (into #{} (map (partial coerce pathom-env subset-key k))))
                              minimum      (some->> (get search-parameters start-key) (coerce pathom-env start-key k))
                              maximum      (some->> (get search-parameters end-key) (coerce pathom-env end-key k))
                              maximum      (when maximum
                                             (if (inst? maximum) maximum (exclusive-end maximum)))]
                          (cond
                            (seq subset-value) (-> acc
                                                 (assoc :filtering? false)
                                                 (assoc-in [:filters k] (fn [v] (contains? subset-value v))))

                            (and filtering? (some? value)) (update acc :filters assoc k value)

                            (and filtering?
                              (or (some? minimum)
                                (some? maximum))) (assoc-in acc [:filters k]
                                                    (fn [v]
                                                      (and
                                                        (or (nil? minimum) (<= (compare minimum v) 0))
                                                        (or (nil? maximum) (< (compare v maximum) 0)))))

                            filtering? acc

                            (some? value) (-> acc
                                            (update :range
                                              (fn [{:keys [start end]}]
                                                {:start (conj start value)
                                                 :end   (conj end value)})))
                            (and
                              (some? minimum)
                              (some? maximum)) (-> acc
                                                 (assoc :filtering? true)
                                                 (update :range
                                                   (fn [{:keys [start end]}]
                                                     {:start (conj start minimum)
                                                      :end   (conj end maximum)})))

                            (some? maximum) (-> acc
                                                 (assoc :filtering? true)
                                                 (update :range
                                                   (fn [{:keys [start end]}]
                                                     {:start (conj start nil)
                                                      :end   (conj end maximum)})))

                            :else (assoc acc :filtering? true))))
                      {:filtering? false
                       :range      {:start [] :end []}
                       :filters    (cond-> {}
                                     row-filter (assoc :row-filter row-filter))}
                      tupleAttrs)]
     ;; If the end of the range is the same constance for start and end, then increment the end
     (let [{:keys [range] :as result} (dissoc result :filtering?)
           {:keys [start end]} (:range result)
           sl (last start)
           el (last end)]
       (cond
         (fn? sl)
         (let [k         (get tupleAttrs (dec (count start)))
               next-last (exclusive-end (second (reverse range)))]
           (-> result
             (update-in [:range :start] pop)
             (update-in [:range :end] (comp #(conj % next-last) pop pop))
             (assoc-in [:filters k] sl)))
         (and (some? sl) (= sl el))
         (let [next-last (exclusive-end el)]
           (update-in result [:range :end] (comp #(conj % next-last) pop)))
         :else result)))))

(defn tuple-index-scan
  "Given a RAD `tuple-attr` with the `do/attribute-schema` option that specifies `:db/tupleAttrs`: search that tuple
   via index-range on `db`. The `pathom-env` shouuld include the items installed by the datomic and attribute RAD plugins.

   `start` and `end` are values of the tuple to use as the total range to scan. See `d/index-range`.

   `filters` is a map whose keys are attributes in the tuple, and whose values are *either* constant values of that attribute's type, or
   are predicate functions `(fn [v] bool?)` that receive that attribute's value (not the whole tuple). Any attributes that are missing
   in this map will accept any value in the tuple. Using `nil` for a filter value is valid, and will keep only entities for which that
   attribute is missing. The special key `:row-filter` can be used in place of tuple keys in order to supply a predicate
   `(fn [tuple-value] boolean?)`. All filters are combined with AND logic, so using a row-filter is necessary if you want
   to create more complex row conditions.

   `options` can include:

    * `:reverse?` Reverses the result order
    * `:sort-column` A keyword (of an attribute in the tuple) on which you want to sort.
    * `:selector` A pull-query selector
    * `:maps?` Turn the tuple into a map (including :db/id). Faster than `selector`, but limited to the tuple's
       content.
    * `:include-total?` Includes a `:total` in the result, which is a count of all possible results
    * `:offset` (default 0) A starting offset of the results you want. A number between 0 and total - 1.
    * `:limit` (default 1000) A limit on the number of items returned. When NOT using a selector it
      is very fast to get large numbers of results; however, when using a `selector`
      you should keep this limit small, since you are making additional round-trips to the database
      to pull non-indexed details for each item.

   WARNING: `:sort-column`, `:reverse?` and `:include-total?` MUST run through as much of the index as start/end
   dictate. This is usually fast, but can get expensive on very large data sets (you should measure on your hardware for
   a start/end segment of over 100k items).

   Returns:

   ```
   {:results [...]
    :total n ; if you specified :include-total?
    :next-offset n}
   ```

   where `:results` will be one of:

   * A vector of maps for the `:selector` or `:maps?` options.
   * A vector of the datoms

   The `:next-offset` is -1 if there are no more possible results. It will be positive if there are more results,
   and can be used as the `offset` to get items starting at the next available item.

   EXAMPLE:

   You could scan an attribute of some entity `item` that has date, email, and sent? fields using this:

   ```
   (tuple-index-scan db pathom-env date+email+sent? [#inst \"2022-01-01\"] [#inst \"2022-01-02\"]
     {:item/email (fn [email] (str/ends-with? email \"gmail.com\"))
      :item/sent? true}
     {:limit 10})
   ```

   to find the items where the email ends in \"gmail.com\" and whose sent? flag is true.

   NOTES:

   * This function expects exact type matching on the tuple. Use `search-params->range+filters` to fix up search parameters
     from a (RAD) UI.
   * The start and end can contain any number of elements (up to the tuple size), but they are Datomic index-range start/end,
     and doing so may or may not be what you want. If you're trying to actually limit the *range* then they are what you want, but if you need to filter
     a wide range, then be sure to use filters instead. For example, say you have invoices with numbers, and a tuple that has
     owner+number+status. A start/end of `[A 1]` `[A 2]` will get you A's invoice number 1 (only). Filters are applied after the range. If you
     want all of the invoices owned by `A` with some status then you'd use `[A]` as start and `[(inc A)]` as end, with a filter
     of `{:invoice/status some-status}`.
   "
  [db {:datomic-api/keys [index-range pull-many] :as env}
   {::attr/keys [qualified-key] :as tuple-attr} start end
   {:keys [row-filter] :as filters}
   {:keys [limit offset
           reverse?
           include-total?
           sort-column
           maps?
           selector] :as options
    :or {limit          1000
         maps?          false
         include-total? false
         reverse?       false
         offset         0}}]
  (p `tuple-index-scan
    (let [{:db/keys [tupleAttrs]} (get tuple-attr do/attribute-schema)
          limit          (or limit 1000)
          reverse?       (boolean reverse?)
          offset         (if (number? offset) offset 0)
          a->idx         (zipmap tupleAttrs (range))
          datum-sort-key (when-let [col (a->idx sort-column)]
                           (fn [{:keys [v]}] (nth v col)))
          filter-keys    (set/difference (set (keys filters)) #{:row-filter})]
      (when-not (fn? index-range) (throw (ex-info "Missing index-range in datomic API config" {})))
      (when-not (fn? pull-many) (throw (ex-info "Missing pull-many in datomic API config" {})))
      (when (and (number? limit) (not (pos? limit)))
        (throw (ex-info "Limit must be a positive integer" {})))
      (when (or (nil? qualified-key) (empty? tupleAttrs))
        (throw (ex-info "tuple-attr must be a RAD attribute with Datomic tuple schema" {})))
      (when (or (not (vector? start)) (empty? start))
        (throw (ex-info "start must be a vector of at least one element that specifies the lower range of the scan" {})))
      (when (or (not (vector? end)) (< (count end) (count start)))
        (throw (ex-info "end must be a vector with at least as many elements as start" {})))
      (let [filter-pred    (fn [{:keys [v]}]
                             (try
                               (and
                                 (or (nil? row-filter) (row-filter v))
                                 (reduce
                                   (fn [_ k]
                                     (let [pred     (get filters k)
                                           idx      (a->idx k)
                                           ev       (get v idx)
                                           include? (if (fn? pred)
                                                      (pred ev)
                                                      (= ev pred))]
                                       (if include?
                                         true
                                         (reduced false))))
                                   true
                                   filter-keys))
                               (catch Exception e
                                 (log/error e "Filter predicate failed"))))
            sort?          (or reverse? datum-sort-key)
            raw-items      (p `tuple-index-scan-index-range
                             (index-range db {:attrid qualified-key
                                              :start  start
                                              :end    end
                                              :limit  -1}))
            filtered-items (p `tuple-index-scan-filter
                             (into [] (filter filter-pred) raw-items))
            nitems         (count filtered-items)
            sorted-items   (if sort?
                             (p `tuple-index-scan-sort
                               (cond->> filtered-items
                                 datum-sort-key (sort-by datum-sort-key)
                                 reverse? reverse))
                             filtered-items)
            page           (into [] (take limit (drop offset sorted-items)))]
        (cond-> {:results page
                 :total   nitems}
          (and maps? (not selector)) (update :results (fn [datoms]
                                                        (mapv (fn [{:keys [e v]}] (assoc
                                                                                    (zipmap tupleAttrs v)
                                                                                    :db/id e)) datoms)))
          (vector? selector) (update :results (fn [datoms]
                                                (let [ids (into [] (map :e) datoms)]
                                                  (p `tuple-index-scan-pull-many
                                                    (pull-many db selector ids))))))))))

(defn generate-tuple-resolver
  "Generate a Datomic resolver.

  * tuple-attribute : the RAD Attribute that represents the tuple that will be used as the index for
    sorting and filtering.
  * key->attribute : RAD attribute map for your model.
  * pathom-version: 2 or 3

   The resulting resolver can be found at the extended keyword made from the tuple's qualified key and the name
   `page`. E.g. :invoice/owner+date+filters -> :invoice.owner+date+filters/page.

   You can override this naming convention using indexed-access-options/resolver-key.

   WARNING: Does not yet support attributes that use native IDs.
   "
  [{::keys      [pathom-env->search-params]
    ::attr/keys [schema qualified-key identities] :as tuple-attribute} key->attribute pathom-version]
  (let [tupleAttrs       (get-in tuple-attribute [do/attribute-schema :db/tupleAttrs])
        selector         (mapv
                           (fn [k]
                             (if-let [{::attr/keys [type target]} (some-> k key->attribute)]
                               (if (= type :ref)
                                 {k [target]}
                                 k)
                               k))
                           (into tupleAttrs identities))
        resolver-key     (get tuple-attribute iao/resolver-key (narrow-keyword qualified-key "page"))
        resolver-sym     (symbol (get tuple-attribute iao/resolver-key (narrow-keyword qualified-key "page-resolver")))
        outputs          [{resolver-key [{:results selector} :next-offset]}]
        with-resolve-sym (fn [r]
                           (fn [env input]
                             (r (assoc env :com.wsscode.pathom.connect/sym (symbol resolver-sym)) input)))
        resolve-fn       (fn [pathom-env _]
                           (let [{:indexed-access/keys [options]
                                  :as                  search-params} (if pathom-env->search-params
                                                                        (pathom-env->search-params pathom-env)
                                                                        (:query-params pathom-env))
                                 search-params (dissoc search-params :indexed-access/options)
                                 db            (some-> (get-in pathom-env [do/databases schema]) deref)
                                 {{:keys [start end]} :range
                                  :keys               [filters]} (search-parameters->range+filters pathom-env tuple-attribute
                                                                   search-params)
                                 result        (try
                                                 (tuple-index-scan db pathom-env tuple-attribute
                                                   (log/spy :debug start) (log/spy :debug end) (log/spy :debug filters)
                                                   (merge {:selector selector} options))
                                                 (catch Exception e
                                                   (log/error (ex-message e))
                                                   []))]
                             {resolver-key result}))]
    (log/info "Generating indexed access resolver for" qualified-key "using output" outputs)
    (case pathom-version
      2 {:com.wsscode.pathom.connect/sym     resolver-sym
         :com.wsscode.pathom.connect/output  outputs
         :com.wsscode.pathom.connect/resolve (with-resolve-sym resolve-fn)}
      3 (common/lazy-invoke com.wsscode.pathom3.connect.operation/resolver
          (merge
            {:com.wsscode.pathom3.connect.operation/op-name resolver-sym
             :com.wsscode.pathom3.connect.operation/output  outputs
             :com.wsscode.pathom3.connect.operation/resolve resolve-fn})))))

(comment
  (log/set-level! :debug))
