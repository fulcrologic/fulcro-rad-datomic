(ns com.fulcrologic.rad.database-adapters.indexed-access-options)

(def coerce
  "An attribute option on a tuple. When using indexed access with reports in the UI, you may wish to
   use a control that returns a data type that doesn't match the low-level database type.
   For example you may use LocalDate for an inst.

   This option allows you to specify a `(fn [pathom-env ui-key value] attribute-value)` to
   convert the UI data into the low-level type that the indexed access system can use.

   See also `com.fulcrologic.rad.database-adapters.indexed-access/default-coercion` for
   what happens when you do not specify this option."
  :com.fulcrologic.rad.database-adapters.indexed-access/coerce)

(def resolver-key
  "An attribute option on a tuple. Sets the global pathom name that will be used when generating an
   indexed resolver for the tuple. Overrides the default name."
  :com.fulcrologic.rad.database-adapters.indexed-access/resolver-key)

(def pathom-env->search-params
  "An attribute option on a tuple. Overrides the default way of getting search parameters from the Pathom env.
   By default they are assumed to be in `:query-params`. This is a `(fn [env] params)`."
  :com.fulcrologic.rad.database-adapters.indexed-access/pathom-env->search-params)
