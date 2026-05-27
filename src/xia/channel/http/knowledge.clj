(ns xia.channel.http.knowledge
  "Knowledge HTTP handlers."
  (:require [xia.memory :as memory]
            [xia.util :as util]))

(def ^:private default-search-top 12)
(def ^:private max-search-top 25)

(defn- json-response
  [deps status body]
  ((:json-response deps) status body))

(defn- nonblank-str
  [deps value]
  ((:nonblank-str deps) value))

(defn- parse-optional-positive-long
  [deps value field-name]
  ((:parse-optional-positive-long deps) value field-name))

(defn- named-value->str
  [value]
  (cond
    (keyword? value) (name value)
    (symbol? value)  (name value)
    (some? value)    (str value)
    :else            nil))

(defn- node->body
  [node]
  (let [eid (or (:eid node) (:db/id node))]
    {:id   (some-> eid str)
     :eid  eid
     :name (or (:name node) (:kg.node/name node))
     :type (named-value->str (or (:type node) (:kg.node/type node)))}))

(defn- fact->body
  [deps fact]
  {:id         (some-> (:eid fact) str)
   :eid        (:eid fact)
   :node_id    (some-> (:node-eid fact) str)
   :node_eid   (:node-eid fact)
   :content    (:content fact)
   :confidence (:confidence fact)
   :utility    (:utility fact)
   :updated_at ((:instant->str deps) (:updated-at fact))})

(defn- parse-search-top
  [deps value]
  (-> (or (parse-optional-positive-long deps value "top")
          default-search-top)
      long
      (util/long-min max-search-top)
      int))

(defn- search-nodes
  [query top]
  (loop [results (concat (memory/find-node query)
                         (memory/search-nodes query :top top))
         acc     []
         seen    #{}]
    (if-let [node (first results)]
      (let [node-eid (:eid node)]
        (if (or (nil? node-eid)
                (contains? seen node-eid))
          (recur (rest results) acc seen)
          (recur (rest results)
                 (cond-> acc
                   (< (count acc) top) (conj node))
                 (conj seen node-eid))))
      acc)))

(defn handle-search-nodes
  [deps req]
  (let [params ((:parse-query-string deps) (:query-string req))
        query  (nonblank-str deps (get params "query"))
        top    (parse-search-top deps (get params "top"))]
    (if-not query
      (json-response deps 400 {:error "missing query"})
      (json-response deps 200 {:query query
                               :nodes (mapv node->body
                                            (search-nodes query top))}))))

(defn handle-list-node-facts
  [deps node-id]
  (if-let [node-eid (parse-optional-positive-long deps node-id "node id")]
    (if-let [node (some-> node-eid memory/get-node not-empty)]
      (json-response deps 200
                     {:node  (node->body (assoc node :db/id node-eid))
                      :facts (mapv (fn [fact]
                                     (fact->body deps (assoc fact :node-eid node-eid)))
                                   (memory/node-facts-with-eids node-eid))})
      (json-response deps 404 {:error "node not found"}))
    (json-response deps 400 {:error "invalid node id"})))

(defn handle-delete-fact
  [deps fact-id]
  (if-let [fact-eid (parse-optional-positive-long deps fact-id "fact id")]
    (if-let [forgotten (memory/forget-fact! fact-eid)]
      (json-response deps 200 {:status "forgotten"
                               :fact   (fact->body deps forgotten)})
      (json-response deps 404 {:error "fact not found"}))
    (json-response deps 400 {:error "invalid fact id"})))
