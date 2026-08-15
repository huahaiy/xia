(ns xia.secret
  "Credential and secret protection.

   Defines which DB attributes and config keys are sensitive, and provides
   safe wrappers for use in the SCI sandbox. System code (xia.llm, xia.setup)
   accesses credentials directly through xia.db; sandboxed tool handlers go
   through these filtered functions instead."
  (:require [xia.db :as db]
            [xia.sensitive :as sensitive]))

;; ---------------------------------------------------------------------------
;; Secret definitions
;; ---------------------------------------------------------------------------

(defn secret-attr?
  "True if the given attribute keyword is secret."
  [attr]
  (sensitive/secret-attr? attr))

(defn secret-config-key?
  "True if the given config key should be treated as secret."
  [k]
  (sensitive/secret-config-key? k))

;; ---------------------------------------------------------------------------
;; Safe wrappers for SCI sandbox
;; ---------------------------------------------------------------------------

(defn safe-get-config
  "Like db/get-config but refuses to return secret keys."
  [k]
  (when (secret-config-key? k)
    (throw (ex-info "Access denied: cannot read secret config key from tool"
                    {:key k})))
  (db/get-config k))

(defn safe-set-config!
  "Like db/set-config! but refuses to write secret or security-boundary keys."
  [k v]
  (when (sensitive/sandbox-blocked-config-write-key? k)
    (throw (ex-info "Access denied: cannot write protected config key from tool"
                    {:key k})))
  (db/set-config! k v))

(def ^:private blocked-query-ops
  '#{aggregate pull pull-many})

(def ^:private allowed-find-ops
  "Datalevin built-in aggregates and arithmetic find expressions that do not
   resolve arbitrary host vars. Qualified symbols are intentionally absent."
  '#{+ - * / mod rem quot
     avg count count-distinct distinct max median min rand sample stddev sum
     variance vec})

(def ^:private allowed-logical-query-ops
  '#{and not not-join or or-join})

(def ^:private query-section-keys
  #{:find :with :in :where :keys :strs :syms
    :having :timeout :order-by :limit :offset})

(defn- secret-like-ident?
  [form]
  (sensitive/secret-query-ident? form))

(defn- split-query-sections
  [query]
  (loop [xs       query
         current  nil
         sections {}
         valid?   true]
    (if-let [x (first xs)]
      (if (and (keyword? x) (contains? query-section-keys x))
        (recur (rest xs)
               x
               (if (contains? sections x)
                 sections
                 (assoc sections x []))
               (and valid? (not (contains? sections x))))
        (recur (rest xs)
               current
               (if current
                 (update sections current conj x)
                 sections)
               (and valid? (some? current))))
      {:valid? (and valid?
                    (contains? sections :find)
                    (seq (get sections :find)))
       :sections sections})))

(declare unsafe-where-clause?)

(defn- unsafe-form?
  [form]
  (cond
    (secret-like-ident? form)
    true

    (seq? form)
    (or (contains? blocked-query-ops (first form))
        (some unsafe-form? form))

    (coll? form)
    (some unsafe-form? form)

    :else false))

(defn- unsafe-find-form?
  [form]
  (cond
    (secret-like-ident? form)
    true

    (and (sequential? form)
         (symbol? (first form))
         (not (.startsWith ^String (name (first form)) "?")))
    (let [op (first form)]
      (or (not (contains? allowed-find-ops op))
          (some unsafe-find-form? (rest form))))

    (coll? form)
    (some unsafe-find-form? form)

    :else false))

(defn- data-pattern-clause?
  [clause]
  (and (vector? clause)
       (<= 3 (count clause) 4)
       (not (seq? (first clause)))))

(defn- computed-clause?
  [clause]
  (and (vector? clause)
       (sequential? (first clause))))

(defn- unsafe-lookup-ref?
  [form]
  (and (vector? form)
       (= 2 (count form))
       (secret-like-ident? (first form))))

(defn- unsafe-data-pattern?
  [clause]
  (let [entity (nth clause 0)
        attr   (nth clause 1)
        value  (nth clause 2)]
    (or (unsafe-lookup-ref? entity)
        (unsafe-lookup-ref? value)
        (not (keyword? attr))
        (secret-like-ident? attr)
        (case attr
          :config/key
          (let [config-key (nth clause 2)]
            (or (not (keyword? config-key))
                (secret-like-ident? config-key)
                (secret-config-key? config-key)))

          :config/value
          true

          false))))

(defn- unsafe-where-clause?
  [clause]
  (cond
    (data-pattern-clause? clause)
    (unsafe-data-pattern? clause)

    (computed-clause? clause)
    true

    (vector? clause)
    (unsafe-form? clause)

    (seq? clause)
    (let [op   (first clause)
          args (if (#{'or-join 'not-join} op)
                 (rest (rest clause))
                 (rest clause))]
      (or (not (contains? allowed-logical-query-ops op))
          (some unsafe-where-clause? args)))

    :else false))

(defn- query-references-secret?
  "Check if a Datalog query references secret attrs or uses query forms that
   can enumerate attributes indirectly. Computed :where clauses are rejected
   outright because they can call host functions outside the attribute filter."
  [query]
  (if-not (vector? query)
    true
    (let [{:keys [valid? sections]} (split-query-sections query)]
      (boolean
       (or (not valid?)
           (contains? sections :having)
           (unsafe-find-form? (get sections :find))
           (some unsafe-where-clause? (get sections :where)))))))

(defn safe-q
  "Restricted Datalog query for the SCI sandbox.
   Rejects queries that reference secret attributes or use indirect attribute
   access such as pull, secret lookup refs, rules, computed clauses,
   host-resolved aggregates, or attr-position variables."
  [query & inputs]
  (when (query-references-secret? query)
    (throw (ex-info "Access denied: query references secret attributes or uses unsupported query forms"
                    {:query query})))
  (apply db/q query inputs))
