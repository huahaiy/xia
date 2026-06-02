(ns xia.board
  "Durable coordination board backed by Xia tasks.

   The first board slice intentionally uses the existing task schema. Board
   fields live under :task/meta {:board ...}, while task state/title/summary
   remain useful to the existing task history and event surfaces."
  (:require [clojure.string :as str]
            [xia.db :as db])
  (:import [java.util Date UUID]))

(def board-task-type :task)
(def legacy-board-task-type :board-card)
(def board-channel :board)

(def statuses #{:open :claimed :blocked :completed :cancelled})
(def terminal-statuses #{:completed :cancelled})
(def priorities #{:low :normal :high :urgent})

(defn- now
  []
  (Date.))

(defn- nonblank-str
  [value]
  (some-> value str str/trim not-empty))

(defn- normalize-keyword
  [value]
  (cond
    (keyword? value) value
    (string? value)  (some-> value nonblank-str keyword)
    :else nil))

(defn- normalize-status
  [value]
  (let [status (or (normalize-keyword value) :open)]
    (if (contains? statuses status)
      status
      (throw (ex-info "Invalid board card status"
                      {:type :board/invalid-status
                       :status value
                       :allowed-statuses (sort (map name statuses))})))))

(defn- normalize-priority
  [value]
  (let [priority (or (normalize-keyword value) :normal)]
    (if (contains? priorities priority)
      priority
      (throw (ex-info "Invalid board card priority"
                      {:type :board/invalid-priority
                       :priority value
                       :allowed-priorities (sort (map name priorities))})))))

(defn- normalize-uuid
  [value field]
  (cond
    (nil? value) nil
    (uuid? value) value
    (string? value)
    (try
      (UUID/fromString (str/trim value))
      (catch Exception e
        (throw (ex-info (str field " must be a UUID")
                        {:type :board/invalid-uuid
                         :field field
                         :value value}
                        e))))
    :else
    (throw (ex-info (str field " must be a UUID")
                    {:type :board/invalid-uuid
                     :field field
                     :value value}))))

(defn- normalize-limit
  [value default-value]
  (let [n (try
            (long (or value default-value))
            (catch Exception _
              default-value))]
    (max 1 (min 500 n))))

(defn- board-task?
  [task]
  (or (= legacy-board-task-type (:type task))
      (some? (get-in task [:meta :board]))))

(defn- require-card
  [card-id]
  (let [task-id (normalize-uuid card-id "card_id")
        task    (some-> task-id db/get-task)]
    (cond
      (nil? task)
      (throw (ex-info "Board card not found"
                      {:type :board/not-found
                       :card-id task-id}))

      (not (board-task? task))
      (throw (ex-info "Task is not a board card"
                      {:type :board/not-a-card
                       :card-id task-id}))

      :else task)))

(defn- board-meta
  [task]
  (or (get-in task [:meta :board]) {}))

(defn- comment-doc
  [{:keys [author text]}]
  (let [text* (or (nonblank-str text)
                  (throw (ex-info "Board comment text is required"
                                  {:type :board/missing-comment-text})))]
    (cond-> {:id (random-uuid)
             :at (now)
             :text text*}
      (nonblank-str author) (assoc :author (nonblank-str author)))))

(defn- assert-claim-token!
  [task supplied-token]
  (let [expected (nonblank-str (get-in task [:meta :board :claim-token]))
        supplied (nonblank-str supplied-token)]
    (when (and expected
               (not= expected supplied))
      (throw (ex-info "Board card claim token does not match"
                      {:type :board/claim-token-mismatch
                       :card-id (:id task)})))))

(defn card->body
  [task]
  (when task
    (let [meta* (board-meta task)]
      {:id (:id task)
       :title (:title task)
       :description (:summary task)
       :status (:state task)
       :priority (or (:priority meta*) :normal)
       :assignee (:assignee meta*)
       :claim-token (:claim-token meta*)
       :claimed-at (:claimed-at meta*)
       :heartbeat-at (:heartbeat-at meta*)
       :comments (vec (or (:comments meta*) []))
       :parent-id (:parent-id task)
       :created-at (:created-at task)
       :updated-at (:updated-at task)
       :finished-at (:finished-at task)})))

(defn create-card!
  [{:keys [id title description status priority assignee parent-id]}]
  (let [title* (or (nonblank-str title)
                   (throw (ex-info "Board card title is required"
                                   {:type :board/missing-title})))
        status* (normalize-status status)
        priority* (normalize-priority priority)
        assignee* (nonblank-str assignee)
        task-id (normalize-uuid id "id")
        parent-id* (normalize-uuid parent-id "parent_id")
        created-id (db/create-task!
                    (cond-> {:type board-task-type
                             :channel board-channel
                             :state status*
                             :title title*
                             :summary (or (nonblank-str description) "")
                             :contract {:kind :task
                                        :version 1
                                        :goal title*
                                        :spec {:kind :task
                                               :version 1
                                               :goal title*
                                               :steps [{:id :work
                                                        :kind :llm
                                                        :mode :agent
                                                        :prompt (or (nonblank-str description)
                                                                    title*)}]}}
                             :meta {:trigger {:kind :user}
                                    :execution {:mode :agent}
                                    :board (cond-> {:visible? true
                                                    :priority priority*
                                                    :comments []}
                                             assignee* (assoc :assignee assignee*))}}
                      task-id (assoc :id task-id)
                      parent-id* (assoc :parent-id parent-id*)))]
    (card->body (db/get-task created-id))))

(defn get-card
  [card-id]
  (card->body (require-card card-id)))

(defn list-cards
  ([] (list-cards {}))
  ([{:keys [status assignee include-terminal? limit]}]
   (let [status* (some-> status normalize-status)
         assignee* (nonblank-str assignee)
         limit* (normalize-limit limit 100)]
     (->> (db/list-tasks {:limit 1000})
          (filter board-task?)
          (filter (fn [task]
                    (let [state (:state task)]
                      (and (or include-terminal?
                               (not (contains? terminal-statuses state)))
                           (or (nil? status*) (= status* state))
                           (or (nil? assignee*)
                               (= assignee* (get-in task [:meta :board :assignee])))))))
          (take limit*)
          (mapv card->body)))))

(defn claim-card!
  [card-id {:keys [assignee claim-token]}]
  (let [task      (require-card card-id)
        token     (or (nonblank-str claim-token) (str (random-uuid)))
        assignee* (or (nonblank-str assignee)
                      (throw (ex-info "Board card assignee is required"
                                      {:type :board/missing-assignee})))
        timestamp (now)]
    (when (= :claimed (:state task))
      (assert-claim-token! task claim-token))
    (db/update-task!
     (:id task)
     {:state :claimed
      :meta (assoc-in (:meta task)
                      [:board]
                      (merge (board-meta task)
                             {:assignee assignee*
                              :claim-token token
                              :claimed-at timestamp
                              :heartbeat-at timestamp}))})
    (card->body (db/get-task (:id task)))))

(defn heartbeat-card!
  [card-id {:keys [claim-token]}]
  (let [task (require-card card-id)]
    (assert-claim-token! task claim-token)
    (db/update-task!
     (:id task)
     {:meta (assoc-in (:meta task)
                      [:board]
                      (assoc (board-meta task)
                             :heartbeat-at (now)))})
    (card->body (db/get-task (:id task)))))

(defn update-card!
  [card-id {:keys [title description status priority assignee claim-token]}]
  (let [task      (require-card card-id)
        status*   (when (some? status) (normalize-status status))
        priority* (when (some? priority) (normalize-priority priority))
        assignee* (when (some? assignee) (nonblank-str assignee))
        title*    (when (some? title)
                    (or (nonblank-str title)
                        (throw (ex-info "Board card title cannot be blank"
                                        {:type :board/blank-title}))))
        desc*     (when (some? description)
                    (or (nonblank-str description) ""))]
    (when (= :claimed (:state task))
      (assert-claim-token! task claim-token))
    (db/update-task!
     (:id task)
     (cond-> {:meta (assoc-in (:meta task)
                              [:board]
                              (cond-> (board-meta task)
                                priority* (assoc :priority priority*)
                                (some? assignee) (assoc :assignee assignee*)))}
       status* (assoc :state status*)
       title* (assoc :title title*)
       (some? description) (assoc :summary desc*)
       (and status* (contains? terminal-statuses status*)) (assoc :finished-at (now))
       (and status* (not (contains? terminal-statuses status*))) (assoc :finished-at nil)))
    (card->body (db/get-task (:id task)))))

(defn comment-card!
  [card-id comment]
  (let [task    (require-card card-id)
        comment* (comment-doc comment)
        meta*   (board-meta task)]
    (db/update-task!
     (:id task)
     {:meta (assoc-in (:meta task)
                      [:board]
                      (update meta* :comments (fnil conj []) comment*))})
    (card->body (db/get-task (:id task)))))
