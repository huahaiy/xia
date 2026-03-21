(ns xia.secret
  "Credential and secret protection.

   Defines which DB attributes and config keys are sensitive, and provides
   safe wrappers for use in the SCI sandbox. System code (xia.llm, xia.setup)
   accesses credentials directly through xia.db; sandboxed tool handlers go
   through these filtered functions instead."
  (:require [clojure.string :as str]
            [xia.db :as db]
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
  "Like db/set-config! but refuses to write to secret keys."
  [k v]
  (when (secret-config-key? k)
    (throw (ex-info "Access denied: cannot write secret config key from tool"
                    {:key k})))
  (db/set-config! k v))

(def ^:private blocked-attrs-pattern
  "Regex matching attribute names that tools cannot query."
  (re-pattern
    (str "(?i)"
         (str/join "|"
                   ["api.key" "api-key" "apikey"
                    "password" "passwd"
                    "secret" "credential"
                    "token" "oauth"
                    "private.key" "private-key"]))))

(def ^:private blocked-query-ops
  '#{pull pull-many})

(def ^:private query-section-keys
  #{:find :with :in :where :keys :strs :syms})

(defn- ident-name
  [form]
  (cond
    (keyword? form) (name form)
    (symbol? form)  (name form)
    :else           nil))

(defn- secret-like-ident?
  [form]
  (or (and (keyword? form)
           (secret-attr? form))
      (when-let [n (ident-name form)]
        (re-find blocked-attrs-pattern n))))

(defn- split-query-sections
  [query]
  (loop [xs       query
         current  nil
         sections {}]
    (if-let [x (first xs)]
      (if (and (keyword? x) (contains? query-section-keys x))
        (recur (rest xs) x (assoc sections x []))
        (recur (rest xs) current (update sections current (fnil conj []) x)))
      sections)))

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

(defn- data-pattern-clause?
  [clause]
  (and (vector? clause)
       (<= 3 (count clause) 4)
       (not (seq? (first clause)))))

(defn- computed-clause?
  [clause]
  (and (vector? clause)
       (seq? (first clause))))

(defn- unsafe-data-pattern?
  [clause]
  (let [attr (nth clause 1)]
    (or (not (keyword? attr))
        (secret-like-ident? attr))))

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
      (or (contains? blocked-query-ops op)
          (some unsafe-where-clause? args)))

    :else false))

(defn- query-references-secret?
  "Check if a Datalog query references secret attrs or uses query forms that
   can enumerate attributes indirectly. Computed :where clauses are rejected
   outright because they can call host functions outside the attribute filter."
  [query]
  (if-not (vector? query)
    true
    (let [sections (split-query-sections query)]
      (boolean
        (or (unsafe-form? (get sections :find))
            (some unsafe-where-clause? (get sections :where)))))))

(defn safe-q
  "Restricted Datalog query for the SCI sandbox.
   Rejects queries that reference secret attributes or use indirect attribute
   access such as pull, computed clauses, or attr-position variables."
  [query & inputs]
  (when (query-references-secret? query)
    (throw (ex-info "Access denied: query references secret attributes or uses unsupported query forms"
                    {:query query})))
  (apply db/q query inputs))
