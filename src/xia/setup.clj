(ns xia.setup
  "First-run setup — interactive wizard that configures a new Xia instance."
  (:require [clojure.string :as str]
            [xia.db :as db]
            [xia.identity :as identity]))

(defn- prompt
  "Print a prompt and read a line. Returns default if input is empty."
  ([msg] (prompt msg nil))
  ([msg default & {:keys [mask?] :or {mask? false}}]
   (let [prompt-text (if default
                       (str msg " [" default "]: ")
                       (str msg ": "))
         input       (if mask?
                       (if-let [console (System/console)]
                         (String. (.readPassword console "%s" (into-array Object [prompt-text])))
                         (do (print (str prompt-text "(input visible): "))
                             (flush)
                             (or (read-line) "")))
                       (do (print prompt-text)
                           (flush)
                           (or (read-line) "")))
         trimmed     (str/trim input)]
     (if (empty? trimmed) default trimmed))))

(defn- setup-identity! []
  (println)
  (println "=== Identity ===")
  (let [name (prompt "What should I call myself?" "Xia")]
    (db/set-identity! :name name)
    (println (str "  → I am " name "."))))

(defn- setup-llm! []
  (println)
  (println "=== LLM Provider ===")
  (println "  Xia works with any OpenAI-compatible API.")
  (println "  Examples: OpenAI, Anthropic (via proxy), Ollama, Qwen, etc.")
  (println)
  (let [base-url (prompt "API base URL" "http://localhost:11434/v1")
        api-key  (prompt "API key (or 'none' for local)" "none" :mask? true)
        model    (prompt "Model name" "qwen2.5:7b")]
    (db/upsert-provider! {:id       :default
                          :name     "default"
                          :base-url base-url
                          :api-key  (if (= api-key "none") "" api-key)
                          :model    model
                          :default? true})
    (println (str "  → Using " model " at " base-url))))

(defn- setup-user! []
  (println)
  (println "=== About You ===")
  (let [user-name (prompt "Your name" "User")]
    (db/set-config! :user/name user-name)
    (println (str "  → Nice to meet you, " user-name "."))))

(defn needs-setup?
  "Check if this Xia instance needs first-run setup."
  []
  (nil? (db/get-config :setup/complete)))

(defn run-setup!
  "Run the interactive first-run setup."
  []
  (println)
  (println "╔══════════════════════════════════════╗")
  (println "║       Welcome to Xia — v0.1.0       ║")
  (println "║   Your portable personal assistant   ║")
  (println "╚══════════════════════════════════════╝")
  (println)
  (println "Let's get you set up. This only takes a minute.")

  (setup-identity!)
  (setup-llm!)
  (setup-user!)

  ;; Initialize identity defaults
  (identity/init-identity!)

  ;; Mark setup as complete
  (db/set-config! :setup/complete "true")

  (println)
  (println "Setup complete! Starting Xia...")
  (println))
