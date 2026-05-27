(ns xia.channel.http.task-board
  "Task-board HTTP handlers."
  (:require [xia.board :as board]))

(defn- instant->str
  [deps value]
  ((:instant->str deps) value))

(defn- comment->body
  [deps comment]
  (cond-> {:id   (some-> (:id comment) str)
           :text (:text comment)}
    (:author comment) (assoc :author (:author comment))
    (:at comment) (assoc :at (instant->str deps (:at comment)))))

(defn- card->body
  [deps card]
  (let [status (:status card)]
    (cond-> {:id          (some-> (:id card) str)
             :type        (name board/board-task-type)
             :channel     (name board/board-channel)
             :state       (some-> status name)
             :status      (some-> status name)
             :priority    (some-> (:priority card) name)
             :title       (:title card)
             :description (:description card)
             :comments    (mapv #(comment->body deps %) (:comments card))
             :created_at  (instant->str deps (:created-at card))
             :updated_at  (instant->str deps (:updated-at card))}
      (:assignee card) (assoc :assignee (:assignee card))
      (:parent-id card) (assoc :parent_id (str (:parent-id card)))
      (:claimed-at card) (assoc :claimed_at (instant->str deps (:claimed-at card)))
      (:heartbeat-at card) (assoc :heartbeat_at (instant->str deps (:heartbeat-at card)))
      (:finished-at card) (assoc :finished_at (instant->str deps (:finished-at card))))))

(defn handle-board
  [deps]
  ((:json-response deps)
   200
   {:tasks (mapv #(card->body deps %)
                 (board/list-cards {:include-terminal? true
                                    :limit 200}))}))
