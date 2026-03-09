(ns xia.identity
  "Identity & soul — defines who this xia is.
   Stored in Datalevin so it travels with the DB."
  (:require [xia.db :as db]))

(def default-soul
  {:name        "Xia"
   :description "A portable personal AI assistant"
   :personality "You are a helpful, thoughtful personal assistant. You remember
                 things about the user and build on past interactions. You are
                 concise but warm."
   :guidelines  "- Be direct and helpful
                 - Remember user preferences
                 - Ask clarifying questions when needed
                 - Never fabricate information
                 - Respect user privacy"})

(defn init-identity!
  "Initialize identity with defaults if not already set."
  []
  (doseq [[k v] default-soul]
    (when-not (db/get-identity k)
      (db/set-identity! k v))))

(defn get-soul
  "Return the full soul/identity map."
  []
  (reduce (fn [m k]
            (if-let [v (db/get-identity k)]
              (assoc m k v)
              m))
          {}
          [:name :description :personality :guidelines]))

(defn set-soul! [k v]
  (db/set-identity! k v))

(defn system-prompt
  "Build the system prompt from identity.
   Knowledge and skills are appended separately by the agent."
  []
  (let [soul (get-soul)]
    (str "You are " (:name soul "Xia") ". "
         (:description soul "") "\n\n"
         "## Personality\n" (:personality soul "") "\n\n"
         "## Guidelines\n" (:guidelines soul "") "\n\n")))
