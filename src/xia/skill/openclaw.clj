(ns xia.skill.openclaw
  "Import a safe subset of OpenClaw skills into Xia's prompt-only skill model."
  (:require [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.http-client :as http]
            [xia.skill :as skill])
  (:import [java.io ByteArrayInputStream File]
           [java.nio.file Files OpenOption Path]
           [java.nio.file.attribute FileAttribute]
           [java.util.zip ZipEntry ZipInputStream]))

(def ^:private supported-frontmatter-keys
  #{"name" "description" "version" "tags"})

(def ^:private ignored-frontmatter-keys
  #{"disable-model-invocation" "homepage" "metadata" "model" "user-invocable"})

(def ^:private rejected-frontmatter-keys
  #{"command-arg-mode" "command-dispatch" "command-tool"})

(def ^:private blocked-metadata-keys
  #{"api-key" "api_key" "apikey" "auth" "env" "environment" "install"
    "installer" "require" "requires" "secret" "secrets"})

(def ^:private supported-resource-extensions
  #{"csv" "json" "md" "txt" "yaml" "yml"})

(def ^:private max-download-bytes (* 4 1024 1024))
(def ^:private max-resource-bytes (* 16 1024))
(def ^:private max-total-resource-bytes (* 48 1024))

(def ^:private tool-aliases
  [{:id :browser
    :patterns [#"(?i)`browser`" #"(?i)\bbrowser tool\b"]
    :source-label "OpenClaw `browser`"
    :target-label "Xia browser tools (`browser-open`, `browser-navigate`, `browser-read-page`, `browser-click`, `browser-fill-form`, `browser-close`)"}
   {:id :search
    :patterns [#"(?i)`search`" #"(?i)`web[_-]?search`" #"(?i)\bsearch tool\b"]
    :source-label "OpenClaw `search`"
    :target-label "Xia `web-search`"}
   {:id :web
    :patterns [#"(?i)`web`" #"(?i)\bweb tool\b"]
    :source-label "OpenClaw `web`"
    :target-label "Xia web tools (`web-search`, `web-fetch`, `web-extract`)"}])

(defn- temp-dir
  [prefix]
  (.toFile (Files/createTempDirectory prefix (into-array FileAttribute []))))

(defn- delete-tree!
  [path]
  (let [root (io/file path)]
    (when (.exists ^File root)
      (doseq [file (reverse (file-seq root))]
        (Files/deleteIfExists (.toPath ^File file))))))

(defn- normalize-entry-name
  [s]
  (.replace ^String s File/separator "/"))

(defn- source-url?
  [source]
  (boolean (and (string? source)
                (re-matches #"https://.+" source))))

(defn- zip-path?
  [path]
  (and (string? path)
       (str/ends-with? (str/lower-case path) ".zip")))

(defn- basename
  [s]
  (some-> s io/file .getName))

(defn- strip-quotes
  [s]
  (let [s (str/trim (str s))]
    (if (and (>= (count s) 2)
             (or (and (str/starts-with? s "\"") (str/ends-with? s "\""))
                 (and (str/starts-with? s "'") (str/ends-with? s "'"))))
      (subs s 1 (dec (count s)))
      s)))

(defn- slugify
  [s]
  (let [slug (-> (or s "")
                 str/lower-case
                 (str/replace #"[^a-z0-9]+" "-")
                 (str/replace #"(^-+|-+$)" "")
                 (str/replace #"-{2,}" "-"))]
    (if (seq slug) slug "openclaw-skill")))

(defn- infer-skill-id
  [metadata source-name]
  (keyword (slugify (or (get metadata "name")
                        (some-> source-name
                                (str/replace #"\.zip$" "")
                                (str/replace #"(?i)^skill-" ""))
                        "openclaw-skill"))))

(defn- split-frontmatter
  [content]
  (if-let [[_ metadata body] (re-matches #"(?s)\A---\r?\n(.*?)\r?\n---\r?\n?(.*)\z" (or content ""))]
    {:frontmatter metadata
     :body body}
    {:frontmatter nil
     :body (or content "")}))

(defn- parse-tags
  [raw]
  (let [value (str/trim (str raw))]
    (cond
      (str/blank? value)
      []

      (and (str/starts-with? value "[") (str/ends-with? value "]"))
      (->> (str/split (subs value 1 (dec (count value))) #",")
           (map strip-quotes)
           (map str/trim)
           (remove str/blank?)
           vec)

      (str/includes? value ",")
      (->> (str/split value #",")
           (map strip-quotes)
           (map str/trim)
           (remove str/blank?)
           vec)

      :else
      [(strip-quotes value)])))

(defn- parse-frontmatter-value
  [k raw]
  (let [value (str/trim raw)]
    (case k
      "tags" (parse-tags value)
      "metadata" (if (str/blank? value)
                   {}
                   (json/read-json value))
      (strip-quotes value))))

(defn- parse-frontmatter
  [frontmatter]
  (reduce
   (fn [acc line]
     (let [line (str/trim line)]
       (if (str/blank? line)
         acc
         (if-let [[_ raw-k raw-v] (re-matches #"^([A-Za-z0-9._-]+):\s*(.*)$" line)]
           (let [k (str/lower-case raw-k)]
             (assoc acc k (parse-frontmatter-value k raw-v)))
           (throw (ex-info "Unsupported OpenClaw frontmatter line"
                           {:line line}))))))
    {}
    (str/split-lines (or frontmatter ""))))

(defn- nested-key-hits
  [value]
  (letfn [(walk [node]
            (cond
              (map? node)
              (mapcat (fn [[k v]]
                        (let [k* (-> k name str/lower-case)]
                          (concat (when (contains? blocked-metadata-keys k*)
                                    [k*])
                                  (walk v))))
                      node)

              (sequential? node)
              (mapcat walk node)

              :else
              []))]
    (vec (distinct (walk value)))))

(defn- validate-frontmatter
  [metadata strict?]
  (let [present-keys   (set (keys metadata))
        rejected-keys  (sort (set/intersection present-keys rejected-frontmatter-keys))
        unknown-keys   (sort (set/difference present-keys
                                             supported-frontmatter-keys
                                             ignored-frontmatter-keys
                                             rejected-frontmatter-keys))
        ignored-fields (sort (set/intersection present-keys ignored-frontmatter-keys))
        metadata-hits  (nested-key-hits (get metadata "metadata"))
        warnings       (vec (concat
                              (map #(str "Ignored unsupported frontmatter field `" % "`.") ignored-fields)
                              (when-not strict?
                                (map #(str "Ignored unknown frontmatter field `" % "`.") unknown-keys))))
        errors         (vec (concat
                              (map #(str "Unsupported OpenClaw frontmatter field `" % "`.") rejected-keys)
                              (map #(str "Blocked OpenClaw metadata key `" % "`.") metadata-hits)
                              (when strict?
                                (map #(str "Unknown OpenClaw frontmatter field `" % "`.") unknown-keys))))]
    {:warnings warnings
     :errors errors
     :ignored-fields ignored-fields}))

(defn- safe-target-path
  [^File root-file entry-name]
  (let [^Path root (.normalize (.toPath root-file))
        ^Path target (.normalize (.resolve root ^String entry-name))]
    (when-not (.startsWith target root)
      (throw (ex-info "Zip entry escapes destination root"
                      {:entry entry-name
                       :root (.getAbsolutePath root-file)})))
    target))

(defn- extract-zip-stream!
  [in ^File target-root]
  (with-open [zip (ZipInputStream. in)]
    (loop [^ZipEntry entry (.getNextEntry zip)]
      (when entry
        (let [^Path target (safe-target-path target-root (.getName entry))]
          (if (.isDirectory entry)
            (Files/createDirectories target (into-array FileAttribute []))
            (do
              (when-let [^Path parent (.getParent target)]
                (Files/createDirectories parent (into-array FileAttribute [])))
              (with-open [out (Files/newOutputStream target (make-array OpenOption 0))]
                (io/copy zip out)))))
        (.closeEntry zip)
        (recur (.getNextEntry zip))))))

(defn- unpack-zip!
  [source-in]
  (let [root (temp-dir "xia-openclaw-unpack")]
    (extract-zip-stream! source-in root)
    root))

(defn- skill-md-files
  [^File root]
  (->> (file-seq root)
       (filter #(.isFile ^File %))
       (filter #(= "SKILL.md" (.getName ^File %)))
       vec))

(defn- bundle-root!
  [^File unpacked-root]
  (let [skill-files (skill-md-files unpacked-root)]
    (cond
      (empty? skill-files)
      (throw (ex-info "OpenClaw bundle does not contain SKILL.md"
                      {:root (.getAbsolutePath unpacked-root)}))

      (> (count skill-files) 1)
      (throw (ex-info "OpenClaw bundle contains multiple SKILL.md files"
                      {:root (.getAbsolutePath unpacked-root)
                       :paths (mapv #(.getAbsolutePath ^File %) skill-files)}))

      :else
      (.getParentFile ^File (first skill-files)))))

(defn- acquire-source!
  [source]
  (cond
    (source-url? source)
    (let [resp (http/request {:url source
                              :method :get
                              :as :byte-array
                              :request-label "OpenClaw skill download"})
          status (:status resp)
          body   ^bytes (:body resp)]
      (when-not (<= 200 status 299)
        (throw (ex-info "Failed to download OpenClaw skill zip"
                        {:source-url source
                         :status status})))
      (when (> (alength body) max-download-bytes)
        (throw (ex-info "OpenClaw skill zip exceeds download limit"
                        {:source-url source
                         :size-bytes (alength body)
                         :limit-bytes max-download-bytes})))
      (when-not (zip-path? source)
        (throw (ex-info "Remote OpenClaw import only supports zip URLs"
                        {:source-url source})))
        (let [tmp-zip (File/createTempFile "xia-openclaw" ".zip")
            unpacked (do
                       (io/copy (ByteArrayInputStream. body) tmp-zip)
                       (with-open [in (io/input-stream tmp-zip)]
                         (unpack-zip! in)))]
        {:source-format :openclaw-zip-url
         :source-url source
         :source-name (or (basename source) (.getName tmp-zip))
         :bundle-root (bundle-root! unpacked)
         :cleanup #(do
                     (.delete tmp-zip)
                     (delete-tree! unpacked))}))

    :else
    (let [file (io/file source)]
      (cond
        (and (.isFile file) (= "SKILL.md" (.getName file)))
        {:source-format :openclaw-dir
         :source-path (.getAbsolutePath (.getParentFile file))
         :source-name (.getName (.getParentFile file))
         :bundle-root (.getParentFile file)
         :cleanup (constantly nil)}

        (.isDirectory file)
        (do
          (when-not (.exists (io/file file "SKILL.md"))
            (throw (ex-info "OpenClaw skill directory must contain SKILL.md"
                            {:source-path (.getAbsolutePath file)})))
          {:source-format :openclaw-dir
           :source-path (.getAbsolutePath file)
           :source-name (.getName file)
           :bundle-root file
           :cleanup (constantly nil)})

        (and (.isFile file) (zip-path? (.getName file)))
        (let [unpacked (with-open [in (io/input-stream file)]
                         (unpack-zip! in))]
          {:source-format :openclaw-zip
           :source-path (.getAbsolutePath file)
           :source-name (.getName file)
           :bundle-root (bundle-root! unpacked)
           :cleanup #(delete-tree! unpacked)})

        :else
        (throw (ex-info "OpenClaw import source must be a skill directory, SKILL.md, zip file, or zip URL"
                        {:source source}))))))

(defn- relative-path
  [^File root ^File file]
  (normalize-entry-name
    (str (.relativize (.toPath root) (.toPath file)))))

(defn- supported-resource-file?
  [^File file]
  (and (.isFile file)
       (not= "SKILL.md" (.getName file))
       (not (str/starts-with? (.getName file) "."))
       (contains? supported-resource-extensions
                  (some-> (.getName file)
                          (str/split #"\.")
                          last
                          str/lower-case))))

(defn- read-resource!
  [^File root ^File file]
  (let [relative (relative-path root file)
        size (.length file)]
    {:path relative
     :size-bytes size
     :content (slurp file)}))

(defn- collect-resources
  [^File root strict?]
  (let [resources (->> (file-seq root)
                       (filter supported-resource-file?)
                       (sort-by #(.getAbsolutePath ^File %))
                       (mapv #(read-resource! root %)))
        oversized (filterv #(> (:size-bytes %) max-resource-bytes) resources)
        total-bytes (reduce + 0 (map :size-bytes resources))
        errors (vec (concat
                      (map #(str "Bundled resource `" (:path %) "` exceeds the per-file size limit.") oversized)
                      (when (> total-bytes max-total-resource-bytes)
                        [(str "Bundled resources exceed the total size limit (" max-total-resource-bytes " bytes).")])))
        warnings (when-not strict?
                   (mapv (fn [resource]
                           (str "Skipped bundled resource `" (:path resource) "` because it exceeds the import limits."))
                         oversized))
        imported (if strict?
                   resources
                   (->> resources
                        (remove #(> (:size-bytes %) max-resource-bytes))
                        vec))]
    {:resources imported
     :errors (if strict? errors [])
     :warnings (or warnings [])
     :total-bytes total-bytes}))

(defn- detected-tool-aliases
  [text]
  (->> tool-aliases
       (filter (fn [{:keys [patterns]}]
                 (some #(re-find % (or text "")) patterns)))
       vec))

(defn- compatibility-preamble
  [aliases]
  (when (seq aliases)
    (str "## Xia Compatibility\n"
         "This skill was imported from OpenClaw. When it refers to OpenClaw tools, use these Xia equivalents:\n\n"
         (str/join "\n"
                   (map (fn [{:keys [source-label target-label]}]
                          (str "- " source-label " -> " target-label))
                        aliases)))))

(defn- resources-section
  [resources]
  (when (seq resources)
    (str "## Bundled Resources\n"
         "The following text resources were imported with this skill.\n\n"
         (str/join "\n\n"
                   (map (fn [{:keys [path content]}]
                          (str "- `" path "`\n\n"
                               "```text\n"
                               (str/trimr content)
                               "\n```"))
                        resources)))))

(defn- body-section
  [body]
  (let [body (str/trim body)]
    (when (seq body)
      (if (re-find #"(?m)^#\s+" body)
        body
        (str "## Instructions\n\n" body)))))

(defn- normalize-content
  [body aliases resources]
  (str/join "\n\n"
            (remove str/blank?
                    [(compatibility-preamble aliases)
                     (body-section body)
                     (resources-section resources)])))

(defn- report-status
  [warnings]
  (if (seq warnings)
    :imported-with-warnings
    :imported))

(defn import-openclaw-source!
  "Import an OpenClaw skill from a directory, a zip archive, or a zip URL.

   Options:
   - :strict?        default true
   - :source-url     overrides the stored source URL
  "
  [source & {:keys [strict?] :or {strict? true}}]
  (let [{:keys [source-format source-path source-url source-name bundle-root cleanup]} (acquire-source! source)]
    (try
      (let [skill-file (io/file bundle-root "SKILL.md")
            {:keys [frontmatter body]} (split-frontmatter (slurp skill-file))
            metadata (parse-frontmatter frontmatter)
            frontmatter-report (validate-frontmatter metadata strict?)
            resource-report (collect-resources bundle-root strict?)
            aliases (detected-tool-aliases body)
            warnings (vec (concat (:warnings frontmatter-report)
                                  (:warnings resource-report)))
            errors (vec (concat (:errors frontmatter-report)
                                (:errors resource-report)))
            skill-id (infer-skill-id metadata source-name)
            skill-name (or (get metadata "name")
                           (name skill-id))
            content (normalize-content body aliases (:resources resource-report))
            report {:status (report-status warnings)
                    :skill-id skill-id
                    :name skill-name
                    :warnings warnings
                    :ignored-fields (:ignored-fields frontmatter-report)
                    :resources (mapv #(select-keys % [:path :size-bytes]) (:resources resource-report))
                    :tool-aliases (mapv (fn [{:keys [id source-label target-label]}]
                                          {:id id
                                           :from source-label
                                           :to target-label})
                                        aliases)
                    :source {:format source-format
                             :path source-path
                             :url source-url
                             :name source-name}}]
        (when (seq errors)
          (throw (ex-info "OpenClaw skill import rejected" (assoc report :status :rejected :errors errors))))
        (skill/import-skill-edn!
          {:id skill-id
           :name skill-name
           :description (or (get metadata "description") "")
           :version (or (get metadata "version") "0.1.0")
           :tags (set (map (comp keyword slugify) (get metadata "tags")))
           :content content
           :source-format source-format
           :source-path source-path
           :source-url source-url
           :source-name source-name
           :import-warnings warnings
           :imported-from-openclaw? true})
        (log/info "Imported OpenClaw skill:" skill-name)
        report)
      (finally
        (cleanup)))))
