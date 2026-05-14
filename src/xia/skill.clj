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
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [datalevin.core :as d]
            [xia.db :as db])
  (:import [java.math BigInteger]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util Date]))

(def trust-levels #{:user-authored :agent-authored :imported :system})
(def lifecycle-states #{:active :stale :archived})
(def usage-kinds #{:selected :injected :viewed :patched})
(def default-stale-days 90)

(declare archived?)

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

(defn- now
  []
  (Date.))

(defn content-sha256
  "Return a stable SHA-256 hex digest for skill content."
  [content]
  (let [digest (MessageDigest/getInstance "SHA-256")
        bytes  (.digest digest (.getBytes (str content) StandardCharsets/UTF_8))]
    (format "%064x" (BigInteger. 1 bytes))))

(defn- normalize-keyword
  [value]
  (cond
    (keyword? value) value
    (string? value)  (some-> value str/trim not-empty keyword)
    :else nil))

(defn- normalize-trust-level
  [value default-value]
  (let [trust (or (normalize-keyword value) default-value)]
    (if (contains? trust-levels trust)
      trust
      (throw (ex-info "Invalid skill trust level"
                      {:type :skill/invalid-trust-level
                       :trust-level value
                       :allowed (sort (map name trust-levels))})))))

(defn- normalize-lifecycle
  [value default-value]
  (let [lifecycle (or (normalize-keyword value) default-value)]
    (if (contains? lifecycle-states lifecycle)
      lifecycle
      (throw (ex-info "Invalid skill lifecycle"
                      {:type :skill/invalid-lifecycle
                       :lifecycle value
                       :allowed (sort (map name lifecycle-states))})))))

(defn- default-provenance
  [{:keys [source-format source-path source-url source-name imported-from-openclaw? trust-level]}]
  (cond-> {:origin (or source-format :xia-local)
           :created-by (case (or trust-level :user-authored)
                         :agent-authored :agent
                         :imported :import
                         :system :system
                         :user)}
    source-name (assoc :source-name source-name)
    source-path (assoc :source-path source-path)
    source-url (assoc :source-url source-url)
    imported-from-openclaw? (assoc :importer :openclaw)))

(defn- source-map
  [skill]
  {:format (or (:skill/source-format skill) (:source-format skill))
   :path (or (:skill/source-path skill) (:source-path skill))
   :url (or (:skill/source-url skill) (:source-url skill))
   :name (or (:skill/source-name skill) (:source-name skill))})

(defn- skill-content
  [skill]
  (or (:skill/content skill) (:content skill) ""))

;; ---------------------------------------------------------------------------
;; Skill import
;; ---------------------------------------------------------------------------

(defn import-skill-edn!
  "Import a skill from an EDN definition map.
   Stores content as raw string (always) and idoc (best-effort)."
  [skill-def]
  (let [{:keys [id name description content version tags
                source-format source-path source-url source-name
                provenance trust-level trust-note
                lifecycle lifecycle-reason
                import-warnings
                imported-from-openclaw?]} skill-def
        skill-name (or name (clojure.core/name id))]
    (when-not id
      (throw (ex-info "Skill definition must have an :id" {:def skill-def})))
    (when-not content
      (throw (ex-info "Skill definition must have :content" {:def skill-def})))
    (let [content-hash (or (:content-sha256 skill-def) (content-sha256 content))
          source-hash  (or (:source-sha256 skill-def) content-hash)
          trust*       (normalize-trust-level trust-level
                                              (if imported-from-openclaw?
                                                :imported
                                                :user-authored))
          lifecycle*   (normalize-lifecycle lifecycle :active)
          provenance*  (or provenance
                           (default-provenance {:source-format source-format
                                                :source-path source-path
                                                :source-url source-url
                                                :source-name source-name
                                                :imported-from-openclaw? imported-from-openclaw?
                                                :trust-level trust*}))
          md-content (ensure-markdown-header content skill-name)
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
                      :provenance provenance*
                      :content-sha256 content-hash
                      :source-sha256 source-hash
                      :trust-level trust*
                      :trust-note trust-note
                      :lifecycle lifecycle*
                      :lifecycle-reason lifecycle-reason
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

(defn save-skill!
  "Create or update a skill authored locally.
   Preserves source metadata for imported skills while allowing their content
   to be edited."
  [skill-def]
  (let [{:keys [id name description content version tags enabled?
                source-format source-path source-url source-name
                provenance trust-level trust-note lifecycle lifecycle-reason
                installed-at]} skill-def
        existing   (when id (db/get-skill id))
        skill-name (or name
                       (:skill/name existing)
                       (some-> id clojure.core/name))]
    (when-not id
      (throw (ex-info "Skill definition must have an :id" {:def skill-def})))
    (when-not (some? content)
      (throw (ex-info "Skill definition must have :content" {:def skill-def})))
    (let [source-format* (or source-format
                             (:skill/source-format existing)
                             :xia-local)
          source-path*   (or source-path (:skill/source-path existing))
          source-url*    (or source-url (:skill/source-url existing))
          source-name*   (or source-name (:skill/source-name existing))
          trust*         (normalize-trust-level trust-level
                                                (or (:skill/trust-level existing)
                                                    :user-authored))
          lifecycle*     (normalize-lifecycle lifecycle
                                              (or (:skill/lifecycle existing)
                                                  :active))
          content-hash   (content-sha256 content)
          provenance*    (or provenance
                             (:skill/provenance existing)
                             (default-provenance {:source-format source-format*
                                                  :source-path source-path*
                                                  :source-url source-url*
                                                  :source-name source-name*
                                                  :imported-from-openclaw? (:skill/imported-from-openclaw? existing)
                                                  :trust-level trust*}))
          base-skill {:id                     id
                      :name                   skill-name
                      :description            (or description (:skill/description existing) "")
                      :content                content
                      :version                (or version (:skill/version existing) "0.1.0")
                      :tags                   (if (some? tags) tags (or (:skill/tags existing) #{}))
                      :enabled?               (if (some? enabled?)
                                                enabled?
                                                (if (contains? existing :skill/enabled?)
                                                  (:skill/enabled? existing)
                                                  true))
                      :installed-at           installed-at
                      :source-format          source-format*
                      :source-path            source-path*
                      :source-url             source-url*
                      :source-name            source-name*
                      :provenance             provenance*
                      :content-sha256         content-hash
                      :source-sha256          (:skill/source-sha256 existing)
                      :trust-level            trust*
                      :trust-note             (or trust-note (:skill/trust-note existing))
                      :lifecycle              lifecycle*
                      :lifecycle-reason       (or lifecycle-reason (:skill/lifecycle-reason existing))
                      :import-warnings        (:skill/import-warnings existing)
                      :imported-from-openclaw? (:skill/imported-from-openclaw? existing)}]
      (try
        (db/save-skill! (assoc base-skill :doc (ensure-markdown-header content skill-name)))
        (catch Exception e
          (if (str/includes? (str (.getMessage e)) "both content and subheaders")
            (do
              (log/info "Skill" skill-name "has mixed content/subheaders — storing without idoc index")
              (db/save-skill! (assoc base-skill :clear-doc? true)))
            (throw e)))))
    (db/get-skill id)))

(defn import-skill-file!
  "Import a skill from a file. Supports:
   - .edn  — EDN map or vector of maps with :id, :content, etc.
   - .md   — markdown file; filename becomes the id"
  [path]
  (cond
    (str/ends-with? path ".edn")
    (let [data (edn/read-string (slurp path))]
      (if (vector? data)
        (mapv #(import-skill-edn!
                (merge {:source-format :xia-edn
                        :source-path (.getAbsolutePath (io/file path))
                        :source-name (.getName (io/file path))}
                       %))
              data)
        (import-skill-edn!
         (merge {:source-format :xia-edn
                 :source-path (.getAbsolutePath (io/file path))
                 :source-name (.getName (io/file path))}
                data))))

    (str/ends-with? path ".md")
    (let [content  (slurp path)
          file     (io/file path)
          filename (-> path
                       (str/replace #".*/" "")
                       (str/replace #"\.md$" ""))
          id       (keyword filename)]
      (import-skill-edn! {:id      id
                           :name    filename
                           :content content
                           :source-format :xia-md
                           :source-path (.getAbsolutePath file)
                           :source-name (.getName file)
                           :source-sha256 (content-sha256 content)}))

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
             (filter #(and (:skill/enabled? %)
                           (not (archived? %))))))
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
        (db/record-skill-usage! skill-id :patched)
        (log/debug "Patched skill section for" skill-id)
        true
        (catch Exception e
          (log/warn "Failed to patch skill" skill-id ":" (.getMessage e))
          false)))))

;; ---------------------------------------------------------------------------
;; Skill selection for context
;; ---------------------------------------------------------------------------

(defn record-usage!
  "Record a skill usage event.
   `usage-kind` is one of :selected, :injected, :viewed, or :patched."
  [skill-id usage-kind]
  (let [kind (normalize-keyword usage-kind)]
    (when-not (contains? usage-kinds kind)
      (throw (ex-info "Invalid skill usage kind"
                      {:type :skill/invalid-usage-kind
                       :usage-kind usage-kind
                       :allowed (sort (map name usage-kinds))})))
    (db/record-skill-usage! skill-id kind)))

(defn record-usages!
  [skills usage-kind]
  (doseq [skill skills
          :let [skill-id (:skill/id skill)]
          :when skill-id]
    (record-usage! skill-id usage-kind))
  skills)

(defn record-update-check!
  [skill-id update-check]
  (db/record-skill-update-check! skill-id update-check))

(defn archived?
  [skill]
  (= :archived (:skill/lifecycle skill)))

(defn all-enabled-skills
  "Return all enabled skills."
  []
  (filter #(and (:skill/enabled? %)
                (not (archived? %)))
          (db/list-skills)))

(def ^:private context-term-split-pattern #"[^\p{L}\p{N}]+")

(defn- context-terms
  [wm-context]
  (->> (concat [(or (:topics wm-context) "")]
               (map :name (:entities wm-context)))
       (mapcat #(str/split (str/lower-case (str %)) context-term-split-pattern))
       (remove #(or (str/blank? %)
                    (< (count %) 2)))
       set))

(defn- tag-keywords
  [terms]
  (->> terms
       (map keyword)
       set))

(defn- text-match-count
  [text terms]
  (let [haystack (str/lower-case (str text))]
    (reduce (fn [matches term]
              (if (str/includes? haystack term)
                (inc matches)
                matches))
            0
            terms)))

(defn- score-skill-relevance
  [skill {:keys [terms tag-terms fts-ranks]}]
  (let [skill-id         (:skill/id skill)
        tags             (->> (:skill/tags skill)
                              (map (comp keyword str/lower-case name))
                              set)
        tag-matches      (count (set/intersection tags tag-terms))
        name-matches     (text-match-count (:skill/name skill) terms)
        description-hits (text-match-count (:skill/description skill) terms)
        content-hits     (text-match-count (:skill/content skill) terms)
        fts-rank-score   (long (or (get fts-ranks skill-id) 0))]
    (+ (* 8 tag-matches)
       (* 5 name-matches)
       (* 2 description-hits)
       content-hits
       fts-rank-score)))

(defn- sort-skills-by-relevance
  [skills]
  (sort-by (fn [skill]
             [(- (double (or (:skill/relevance skill) 0.0)))
              (str/lower-case (or (:skill/name skill)
                                  (some-> (:skill/id skill) name)
                                  ""))])
           skills))

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
   (let [enabled-skills (vec (all-enabled-skills))
         terms          (context-terms wm-context)]
     (if (seq terms)
       (let [entity-names   (->> (:entities wm-context)
                                 (map :name)
                                 (remove nil?))
             search-query   (str/join " " (cons (or (:topics wm-context) "")
                                                entity-names))
             fts-matches    (search-skills search-query :top 10)
             fts-ranks      (into {}
                                  (map-indexed (fn [idx skill]
                                                 [(:skill/id skill)
                                                  (max 1 (- 12 idx))]))
                                  fts-matches)
             tag-terms      (tag-keywords terms)
             scored-skills  (->> enabled-skills
                                 (map (fn [skill]
                                        (assoc skill
                                               :skill/relevance
                                               (double
                                                (score-skill-relevance skill
                                                                       {:terms terms
                                                                        :tag-terms tag-terms
                                                                        :fts-ranks fts-ranks})))))
                                 sort-skills-by-relevance
                                 vec)
             relevant-skills (into []
                                   (filter #(pos? (double (or (:skill/relevance %) 0.0))))
                                   scored-skills)]
         (record-usages! (if (seq relevant-skills)
                           relevant-skills
                           enabled-skills)
                         :selected))
       (record-usages! enabled-skills :selected)))))

(defn skills->prompt
  "Format selected skills into a prompt section for the LLM."
  [skills]
  (when (seq skills)
    (record-usages! skills :injected)
    (str "## Skills\n"
         "You have the following skills. Follow their instructions when relevant.\n\n"
         (str/join "\n\n"
                   (map (fn [s]
                          (str "### " (:skill/name s) "\n"
                               (:skill/content s)))
                        skills)))))

;; ---------------------------------------------------------------------------
;; Safe update checks
;; ---------------------------------------------------------------------------

(defn- source-readable-file
  [skill]
  (when-let [path (:path (source-map skill))]
    (let [file (io/file path)]
      (when (and (.exists file) (.isFile file))
        file))))

(defn- source-content-for-check
  [skill file]
  (if (= :xia-edn (:skill/source-format skill))
    (let [data (edn/read-string (slurp file))
          skill-id (:skill/id skill)
          skill-def (if (vector? data)
                      (some #(when (= skill-id (:id %)) %) data)
                      data)]
      (or (:content skill-def)
          (throw (ex-info "Imported EDN source no longer contains this skill"
                          {:type :skill/source-missing-skill
                           :skill-id skill-id
                           :source-path (.getAbsolutePath file)}))))
    (slurp file)))

(defn- update-status
  [skill source-hash]
  (let [current-hash (content-sha256 (skill-content skill))
        imported-hash (:skill/source-sha256 skill)
        local-edits? (and imported-hash (not= current-hash imported-hash))]
    (cond
      local-edits?
      :local-edits

      (= source-hash current-hash)
      :current

      :else
      :update-available)))

(defn check-import-update!
  "Safely check a file-backed imported skill for upstream changes.
   This never applies content. It records only update-check metadata."
  [skill-id]
  (let [skill (or (db/get-skill skill-id)
                  (throw (ex-info "Skill not found"
                                  {:type :skill/not-found
                                   :skill-id skill-id})))
        file  (source-readable-file skill)]
    (if-not file
      (let [status :no-source]
        (record-update-check! (:skill/id skill) {:status status})
        {:status status
         :skill-id (:skill/id skill)
         :source (source-map skill)})
      (let [source-content (source-content-for-check skill file)
            source-hash    (content-sha256 source-content)
            status         (update-status skill source-hash)]
        (record-update-check! (:skill/id skill) {:status status
                                                 :source-sha256 source-hash})
        {:status status
         :skill-id (:skill/id skill)
         :source (source-map skill)
         :current_sha256 (content-sha256 (skill-content skill))
         :imported_sha256 (:skill/source-sha256 skill)
         :source_sha256 source-hash
         :safe_to_apply? (= status :update-available)}))))

;; ---------------------------------------------------------------------------
;; Curator
;; ---------------------------------------------------------------------------

(defn- millis-before
  [^Date date days]
  (when date
    (< (.getTime date)
       (- (.getTime (now)) (* (long days) 24 60 60 1000)))))

(defn- skill-stale?
  [skill stale-days]
  (and (not (archived? skill))
       (let [last-used (:skill/last-used-at skill)
             installed (:skill/installed-at skill)
             injected (long (or (:skill/injected-count skill) 0))
             selected (long (or (:skill/selected-count skill) 0))]
         (or (and last-used (millis-before last-used stale-days))
             (and (nil? last-used)
                  (zero? injected)
                  (zero? selected)
                  (millis-before installed stale-days))))))

(defn- stale-reason
  [skill stale-days]
  (if (:skill/last-used-at skill)
    (str "No recorded use in " stale-days " days.")
    (str "No recorded use since installation and older than " stale-days " days.")))

(defn- agent-authored?
  [skill]
  (or (= :agent-authored (:skill/trust-level skill))
      (= :agent (get-in skill [:skill/provenance :created-by]))))

(defn- skill-summary
  [skill]
  {:id (some-> (:skill/id skill) name)
   :name (:skill/name skill)
   :trust_level (some-> (:skill/trust-level skill) name)
   :lifecycle (some-> (:skill/lifecycle skill) name)
   :selected_count (long (or (:skill/selected-count skill) 0))
   :injected_count (long (or (:skill/injected-count skill) 0))
   :last_used_at (:skill/last-used-at skill)})

(defn- skill-name-terms
  [skill]
  (->> (str/split (str/lower-case (or (:skill/name skill)
                                      (some-> (:skill/id skill) name)
                                      ""))
                  context-term-split-pattern)
       (remove #(or (str/blank? %)
                    (< (count %) 3)))
       set))

(defn- consolidation-score
  [left right]
  (let [left-tags (set (:skill/tags left))
        right-tags (set (:skill/tags right))
        tag-overlap (set/intersection left-tags right-tags)
        name-overlap (set/intersection (skill-name-terms left)
                                       (skill-name-terms right))]
    (+ (* 3 (count tag-overlap))
       (count name-overlap))))

(defn- consolidation-suggestions
  [skills]
  (let [skills* (->> skills
                     (remove archived?)
                     (filter :skill/enabled?)
                     vec)]
    (->> (for [left skills*
               right skills*
               :let [left-id (:skill/id left)
                     right-id (:skill/id right)]
               :when (neg? (compare (name left-id) (name right-id)))
               :let [score (consolidation-score left right)]
               :when (>= score 3)]
           {:skill_ids [(name left-id) (name right-id)]
            :score score
            :reason "Overlapping names or tags; review for consolidation."})
         (sort-by (comp - :score))
         (take 20)
         vec)))

(defn curate-skills!
  "Run the skill curator.

   Marks stale skills. Stale agent-authored skills are archived by disabling
   them and setting lifecycle to :archived. User/imported skills are never
   archived automatically."
  ([] (curate-skills! {}))
  ([{:keys [stale-days archive-agent-authored?]
     :or {stale-days default-stale-days
          archive-agent-authored? true}}]
   (let [skills (db/list-skills)
         stale  (filterv #(skill-stale? % stale-days) skills)
         archived (atom [])]
     (doseq [skill stale
             :let [skill-id (:skill/id skill)
                   reason (stale-reason skill stale-days)]]
       (if (and archive-agent-authored? (agent-authored? skill))
         (do
           (db/update-skill-lifecycle! skill-id {:lifecycle :archived
                                                 :enabled? false
                                                 :reason reason
                                                 :archived-at (now)})
           (swap! archived conj skill))
         (db/update-skill-lifecycle! skill-id {:lifecycle :stale
                                               :reason reason})))
     {:stale (mapv skill-summary stale)
      :archived (mapv skill-summary @archived)
      :suggestions (consolidation-suggestions skills)})))
