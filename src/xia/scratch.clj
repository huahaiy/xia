(ns xia.scratch
  "Scratch pad documents backed by a dedicated KV DBI inside Xia's LMDB env."
  (:require [clojure.string :as str]
            [datalevin.core :as d]
            [xia.crypto :as crypto]
            [xia.db :as db])
  (:import [datalevin.db DB]
           [datalevin.storage Store]
           [java.util Date UUID]))

(def ^:private scratch-dbi "xia/scratch")
(def ^:private default-title "Untitled scratch pad")
(def ^:private default-mime "text/plain")
(def ^:private allowed-scopes #{:session :global})

(defn- lmdb []
  (let [db-value (d/db (db/conn))]
    (.-lmdb ^Store (.-store ^DB db-value))))

(defn- ensure-dbi! []
  (d/open-dbi (lmdb) scratch-dbi {:key-size 128}))

(defn- normalize-pad-id [pad-id]
  (let [value (cond
                (instance? UUID pad-id) (str pad-id)
                (string? pad-id)        (str/trim pad-id)
                :else                   (str pad-id))]
    (when (str/blank? value)
      (throw (ex-info "Scratch pad id is blank" {:pad-id pad-id})))
    value))

(defn- normalize-session-id [session-id]
  (let [value (cond
                (nil? session-id)       nil
                (instance? UUID session-id) (str session-id)
                (string? session-id)    (str/trim session-id)
                :else                   (str session-id))]
    (when (and (some? value) (str/blank? value))
      (throw (ex-info "Scratch pad session id is blank" {:session-id session-id})))
    value))

(defn- normalize-scope [scope]
  (let [value (cond
                (keyword? scope) scope
                (string? scope)  (keyword (str/lower-case (str/trim scope)))
                (nil? scope)     nil
                :else            (keyword (str scope)))]
    (when-not (contains? allowed-scopes value)
      (throw (ex-info "Scratch pad scope must be :session or :global"
                      {:scope scope})))
    value))

(defn- scratch-aad [pad-id field]
  (str "scratch:" pad-id ":" (name field)))

(defn- encrypt-field [pad-id field value]
  (crypto/encrypt (or value "") (scratch-aad pad-id field)))

(defn- decrypt-field [pad-id field value]
  (let [plain (crypto/decrypt value (scratch-aad pad-id field))]
    (if (nil? plain) "" plain)))

(defn- encrypt-doc [doc]
  (let [pad-id (:id doc)]
    (-> doc
        (update :title #(encrypt-field pad-id :title %))
        (update :content #(encrypt-field pad-id :content %)))))

(defn- decrypt-doc [doc]
  (let [pad-id (:id doc)]
    (-> doc
        (update :title #(decrypt-field pad-id :title %))
        (update :content #(decrypt-field pad-id :content %)))))

(defn- read-doc [pad-id]
  (ensure-dbi!)
  (some-> (d/get-value (lmdb) scratch-dbi (normalize-pad-id pad-id) :string :data)
          decrypt-doc))

(defn- write-doc! [doc]
  (ensure-dbi!)
  (let [record (encrypt-doc doc)]
    (d/transact-kv (lmdb)
                   scratch-dbi
                   [[:put (:id record) record]]
                   :string
                   :data)
    doc))

(defn- delete-doc! [pad-id]
  (ensure-dbi!)
  (d/transact-kv (lmdb)
                 scratch-dbi
                 [[:del (normalize-pad-id pad-id)]]
                 :string))

(defn- all-docs []
  (ensure-dbi!)
  (->> (d/get-range (lmdb) scratch-dbi [:all] :string :data false)
       (map (fn [[_pad-id doc]] (decrypt-doc doc)))))

(defn- assert-version!
  [pad expected-version]
  (when (and (some? expected-version)
             (not= (long expected-version) (long (:version pad))))
    (throw (ex-info "Scratch pad version conflict"
                    {:type             :scratch/version-conflict
                     :pad-id           (:id pad)
                     :expected-version expected-version
                     :actual-version   (:version pad)}))))

(defn- metadata [pad]
  (select-keys pad [:id :scope :session-id :title :mime :version :created-at :updated-at]))

(defn list-pads
  "List scratch pads, optionally filtered by scope/session.

   Options:
   - :scope           one of :session or :global
   - :session-id      required when :scope = :session
   - :include-content? include the full content in each result"
  ([] (list-pads {}))
  ([{:keys [scope session-id include-content?]}]
   (let [scope*      (when (some? scope) (normalize-scope scope))
         session-id* (normalize-session-id session-id)]
     (when (and (= :session scope*) (nil? session-id*))
       (throw (ex-info "Session-scoped scratch pads require a session id"
                       {:scope scope*})))
     (->> (all-docs)
          (filter (fn [pad]
                    (and (if scope* (= scope* (:scope pad)) true)
                         (if (= :session (:scope pad))
                           (= session-id* (:session-id pad))
                           true))))
          (sort-by :updated-at #(compare %2 %1))
          (mapv #(if include-content? % (metadata %)))))))

(defn get-pad
  "Get a scratch pad by id."
  [pad-id]
  (read-doc pad-id))

(defn create-pad!
  "Create a scratch pad.

   Fields:
   - :scope        :session or :global (defaults to :session when session-id exists, else :global)
   - :session-id   required for :session scope
   - :title        optional
   - :content      optional
   - :mime         optional, defaults to text/plain"
  [{:keys [id scope session-id title content mime]}]
  (let [pad-id      (normalize-pad-id (or id (random-uuid)))
        session-id* (normalize-session-id session-id)
        scope*      (normalize-scope (or scope (if session-id* :session :global)))
        _           (when (and (= :session scope*) (nil? session-id*))
                      (throw (ex-info "Session-scoped scratch pads require a session id"
                                      {:scope scope*})))
        now         (Date.)
        pad         {:id         pad-id
                     :scope      scope*
                     :session-id session-id*
                     :title      (or title default-title)
                     :content    (or content "")
                     :mime       (or mime default-mime)
                     :version    1
                     :created-at now
                     :updated-at now}]
    (when (get-pad pad-id)
      (throw (ex-info "Scratch pad already exists"
                      {:type :scratch/already-exists
                       :pad-id pad-id})))
    (write-doc! pad)))

(defn save-pad!
  "Replace some or all fields of a scratch pad, using optimistic concurrency."
  [pad-id updates]
  (if-let [existing (get-pad pad-id)]
    (do
      (assert-version! existing (:expected-version updates))
      (let [updated (cond-> (assoc existing
                                   :version    (inc (long (:version existing)))
                                   :updated-at (Date.))
                      (contains? updates :title)
                      (assoc :title (or (:title updates) ""))
                      (contains? updates :content)
                      (assoc :content (or (:content updates) ""))
                      (contains? updates :mime)
                      (assoc :mime (or (:mime updates) default-mime)))]
        (write-doc! updated)))
    (throw (ex-info "Scratch pad not found"
                    {:type :scratch/not-found
                     :pad-id (normalize-pad-id pad-id)}))))

(defn delete-pad!
  "Delete a scratch pad."
  [pad-id]
  (if (get-pad pad-id)
    (do
      (delete-doc! pad-id)
      {:status "deleted" :id (normalize-pad-id pad-id)})
    (throw (ex-info "Scratch pad not found"
                    {:type :scratch/not-found
                     :pad-id (normalize-pad-id pad-id)}))))

(defn- nth-index-of [s match occurrence]
  (loop [from 0
         seen 1]
    (let [idx (.indexOf ^String s ^String match (int from))]
      (cond
        (neg? idx) nil
        (= seen occurrence) idx
        :else (recur (+ idx (count match)) (inc seen))))))

(defn- line-split [content]
  (str/split (or content "") #"\n" -1))

(defn- replace-line-range [content start-line end-line text]
  (let [start (long start-line)
        end   (long end-line)]
    (when (or (<= start 0) (<= end 0) (> start end))
      (throw (ex-info "replace-lines requires positive start/end lines with start <= end"
                      {:start-line start-line :end-line end-line})))
    (let [lines          (line-split content)
          line-count     (count lines)
          _              (when (> end line-count)
                           (throw (ex-info "replace-lines range exceeds document length"
                                           {:start-line start-line
                                            :end-line   end-line
                                            :line-count line-count})))
          replacement    (line-split (or text ""))
          before         (take (dec start) lines)
          after          (drop end lines)]
      (str/join "\n" (concat before replacement after)))))

(defn- insert-at-offset [content offset text]
  (let [offset* (long offset)
        current (or content "")]
    (when (or (neg? offset*) (> offset* (count current)))
      (throw (ex-info "insert-at offset is out of range"
                      {:offset offset :length (count current)})))
    (str (subs current 0 offset*) (or text "") (subs current offset*))))

(defn- replace-string-once [content match replacement occurrence]
  (when (str/blank? (or match ""))
    (throw (ex-info "replace-string requires a non-empty match string" {})))
  (let [occurrence* (long (or occurrence 1))]
    (when (<= occurrence* 0)
      (throw (ex-info "replace-string occurrence must be positive"
                      {:occurrence occurrence})))
    (if-let [idx (nth-index-of (or content "") match occurrence*)]
      (str (subs content 0 idx)
           (or replacement "")
           (subs content (+ idx (count match))))
      (throw (ex-info "replace-string match not found"
                      {:match match :occurrence occurrence*})))))

(defn- apply-edit [content {:keys [op text separator match replacement occurrence offset start-line end-line]
                            :as operation}]
  (case (cond
          (keyword? op) op
          (string? op)  (keyword (str/lower-case op))
          :else         op)
    :append
    (str (or content "") (or separator "") (or text ""))

    :insert-at
    (insert-at-offset content offset text)

    :replace-string
    (replace-string-once (or content "") match replacement occurrence)

    :replace-lines
    (replace-line-range (or content "") start-line end-line text)

    (throw (ex-info "Unsupported scratch pad edit operation"
                    {:operation operation}))))

(defn edit-pad!
  "Apply a targeted text edit to a scratch pad.

   Supported operations:
   - {:op :append :text \"...\" :separator \"\\n\"}
   - {:op :insert-at :offset 42 :text \"...\"}
   - {:op :replace-string :match \"old\" :replacement \"new\" :occurrence 1}
   - {:op :replace-lines :start-line 3 :end-line 5 :text \"...\"}"
  [pad-id {:keys [expected-version] :as operation}]
  (if-let [existing (get-pad pad-id)]
    (do
      (assert-version! existing expected-version)
      (write-doc! (-> existing
                      (assoc :content    (apply-edit (:content existing) operation)
                             :version    (inc (long (:version existing)))
                             :updated-at (Date.)))))
    (throw (ex-info "Scratch pad not found"
                    {:type :scratch/not-found
                     :pad-id (normalize-pad-id pad-id)}))))
