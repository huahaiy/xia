(ns xia.skill
  "Skill system — import and manage skills.

   A skill is a markdown/text document that describes steps, context,
   and instructions for the LLM to follow. Skills are NOT code — they
   are prompt content that gets injected into the LLM's context when
   relevant.

   Skills are stored with two representations:
   - :skill/content (string, FTS-indexed) — raw markdown for prompt injection
   - :skill/doc (idoc, markdown format) — parsed structure for section queries

   The idoc representation enables:
   - Section extraction: (skill-section :email-drafting :tone)
   - Section patching: (patch-skill-section! :email-drafting [[:set [:tone] \"...\"]])
   - Structural queries: (match-skills {:? {:prerequisites :?}})

   Note: idoc markdown requires that a heading has EITHER content OR
   sub-headings, not both. Skills that don't comply are stored without
   the idoc index (FTS still works for search).

   Skills can be imported from:
   - EDN files containing {:id, :name, :content, ...}
   - Markdown files (the file becomes the content)
   - A skill registry (future)"
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [datalevin.core :as d]
            [xia.db :as db]))

;; ---------------------------------------------------------------------------
;; Markdown normalization for idoc
;; ---------------------------------------------------------------------------

(defn- ensure-markdown-header
  "Ensure markdown content starts with a header (required for idoc :markdown).
   If content doesn't start with a heading, prepend one using the skill name."
  [content skill-name]
  (if (re-find #"(?m)^#\s+" content)
    content
    (str "# " skill-name "\n\n" content)))

;; ---------------------------------------------------------------------------
;; Skill import
;; ---------------------------------------------------------------------------

(defn import-skill-edn!
  "Import a skill from an EDN definition map.
   Stores content as raw string (always) and idoc (best-effort)."
  [skill-def]
  (let [{:keys [id name description content version tags
                source-format source-path source-url source-name
                import-warnings
                imported-from-openclaw?]} skill-def
        skill-name (or name (clojure.core/name id))]
    (when-not id
      (throw (ex-info "Skill definition must have an :id" {:def skill-def})))
    (when-not content
      (throw (ex-info "Skill definition must have :content" {:def skill-def})))
    (let [md-content (ensure-markdown-header content skill-name)
          base-skill {:id          id
                      :name        skill-name
                      :description (or description "")
                      :content     content
                      :version     (or version "0.1.0")
                      :tags        (or tags #{})
                      :source-format source-format
                      :source-path source-path
                      :source-url source-url
                      :source-name source-name
                      :import-warnings import-warnings
                      :imported-from-openclaw? imported-from-openclaw?}]
      ;; Try with idoc doc; fall back to without if markdown doesn't comply
      (try
        (db/install-skill! (assoc base-skill :doc md-content))
        (catch Exception e
          (if (str/includes? (str (.getMessage e)) "both content and subheaders")
            (do (log/info "Skill" skill-name "has mixed content/subheaders — storing without idoc index")
                (db/install-skill! base-skill))
            (throw e)))))
    (log/info "Imported skill:" skill-name)
    skill-def))

(defn import-skill-file!
  "Import a skill from a file. Supports:
   - .edn  — EDN map or vector of maps with :id, :content, etc.
   - .md   — markdown file; filename becomes the id"
  [path]
  (cond
    (str/ends-with? path ".edn")
    (let [data (edn/read-string (slurp path))]
      (if (vector? data)
        (mapv import-skill-edn! data)
        (import-skill-edn! data)))

    (str/ends-with? path ".md")
    (let [content  (slurp path)
          filename (-> path
                       (str/replace #".*/" "")
                       (str/replace #"\.md$" ""))
          id       (keyword filename)]
      (import-skill-edn! {:id      id
                           :name    filename
                           :content content}))

    :else
    (throw (ex-info "Unsupported skill file format. Use .edn or .md" {:path path}))))

;; ---------------------------------------------------------------------------
;; Skill search — FTS (full-text on :skill/content)
;; ---------------------------------------------------------------------------

(defn search-skills
  "Find enabled skills whose content matches the query via full-text search.
   Returns skill entity maps."
  [query & {:keys [top] :or {top 10}}]
  (when-not (str/blank? query)
    (try
      (let [db (d/db (db/conn))]
        (->> (d/fulltext-datoms db query)
             (filter #(= :skill/content (nth % 1)))
             (take top)
             (mapv (fn [datom] (db/entity (nth datom 0))))
             (filter :skill/enabled?)))
      (catch Exception e
        (log/debug "Skill FTS search failed:" (.getMessage e))
        []))))

;; ---------------------------------------------------------------------------
;; Skill search — idoc structural queries
;; ---------------------------------------------------------------------------

(defn match-skills
  "Find skills using idoc structural matching on their parsed document.
   `pattern` is a map matching the idoc heading structure.

   Headings become kebab-case keywords in the idoc:
     '## Tone' → :tone,  '# Email Drafting' → :email-drafting

   Examples:
     (match-skills {:? {:tone :?}})              — any skill with a :tone section
     (match-skills {:* \"Be professional.\"})     — value at any depth
     (match-skills {:code-review {:checklist :?}}) — exact path match"
  [pattern]
  (try
    (let [eids (db/q '[:find ?e
                        :in $ ?q
                        :where
                        [(idoc-match $ :skill/doc ?q) [[?e ?a ?v]]]
                        [?e :skill/enabled? true]]
                      pattern)]
      (mapv #(into {} (db/entity (first %))) eids))
    (catch Exception e
      (log/debug "Skill idoc-match failed:" (.getMessage e))
      [])))

;; ---------------------------------------------------------------------------
;; Section operations (via idoc get-in / patchIdoc)
;; ---------------------------------------------------------------------------

(defn skill-section
  "Extract a specific section from a skill's parsed document.
   Path elements are kebab-case keywords matching heading names.

   Examples:
     (skill-section :email-drafting :email-drafting :tone)
     (skill-section :code-review :code-review :checklist :style)"
  [skill-id & path]
  (when-let [skill (db/get-skill skill-id)]
    (when-let [doc (:skill/doc skill)]
      (get-in doc (vec path)))))

(defn skill-headings
  "List the top-level heading structure of a skill's idoc document.
   Returns a nested map of heading keywords, or nil if no idoc."
  [skill-id]
  (when-let [skill (db/get-skill skill-id)]
    (when-let [doc (:skill/doc skill)]
      (letfn [(structure [v]
                (if (map? v)
                  (into {} (map (fn [[k v]] [k (structure v)]) v))
                  :leaf))]
        (structure doc)))))

(defn patch-skill-section!
  "Update a specific section of a skill's idoc document.
   `ops` is a vector of patch operations.

   Examples:
     (patch-skill-section! :email-drafting
       [[:set [:email-drafting :tone] \"Updated tone.\"]])"
  [skill-id ops]
  (let [eid (ffirst (db/q '[:find ?e :in $ ?id :where [?e :skill/id ?id]] skill-id))]
    (when eid
      (try
        (db/transact! [[:db.fn/patchIdoc eid :skill/doc ops]])
        (log/debug "Patched skill section for" skill-id)
        true
        (catch Exception e
          (log/warn "Failed to patch skill" skill-id ":" (.getMessage e))
          false)))))

;; ---------------------------------------------------------------------------
;; Skill selection for context
;; ---------------------------------------------------------------------------

(defn all-enabled-skills
  "Return all enabled skills."
  []
  (filter :skill/enabled? (db/list-skills)))

(defn skills-for-context
  "Select skills relevant to the current context.

   Strategy (in priority order):
   1. FTS search on :skill/content using WM topics + entity names
   2. Tag matching on :skill/tags
   3. Fall back to all enabled skills

   FTS is the primary mechanism — it searches the full text of every skill's
   markdown content for keywords from the current conversation context."
  ([] (skills-for-context nil))
  ([wm-context]
   (if-let [topics (:topics wm-context)]
     (let [;; Build search query from topics + entity names
           entity-names (->> (:entities wm-context)
                             (map :name)
                             (remove nil?))
           search-query (str/join " " (cons topics entity-names))
           ;; 1. FTS search on skill content
           fts-matches (search-skills search-query :top 10)
           ;; 2. Tag matching as supplement
           keywords (->> (concat
                           (str/split (str/lower-case topics) #"[^\w]+")
                           (map str/lower-case entity-names))
                         (remove str/blank?)
                         (map keyword)
                         set)
           tag-matches (when (seq keywords)
                         (db/find-skills-by-tags keywords))
           ;; Combine, deduplicate by :skill/id
           combined (->> (concat fts-matches tag-matches)
                         (group-by :skill/id)
                         vals
                         (map first))]
       (if (seq combined)
         combined
         (all-enabled-skills)))
     (all-enabled-skills))))

(defn skills->prompt
  "Format selected skills into a prompt section for the LLM."
  [skills]
  (when (seq skills)
    (str "## Skills\n"
         "You have the following skills. Follow their instructions when relevant.\n\n"
         (str/join "\n\n"
                   (map (fn [s]
                          (str "### " (:skill/name s) "\n"
                               (:skill/content s)))
                        skills)))))
