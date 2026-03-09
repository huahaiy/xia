(ns xia.setup
  "First-run setup — interactive wizard that configures a new xia instance."
  (:require [clojure.string :as str]
            [xia.db :as db]
            [xia.identity :as identity]))

(defn- prompt
  "Print a prompt and read a line. Returns default if input is empty."
  ([msg] (prompt msg nil))
  ([msg default]
   (if default
     (print (str msg " [" default "]: "))
     (print (str msg ": ")))
   (flush)
   (let [input (str/trim (or (read-line) ""))]
     (if (empty? input) default input))))

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
        api-key  (prompt "API key (or 'none' for local)" "none")
        model    (prompt "Model name" "qwen2.5:7b")]
    (db/transact! [{:llm.provider/id       :default
                    :llm.provider/name     "default"
                    :llm.provider/base-url base-url
                    :llm.provider/api-key  (if (= api-key "none") "" api-key)
                    :llm.provider/model    model
                    :llm.provider/default? true}])
    (println (str "  → Using " model " at " base-url))))

(defn- setup-user! []
  (println)
  (println "=== About You ===")
  (let [user-name (prompt "Your name" "User")]
    (db/set-config! :user/name user-name)
    (println (str "  → Nice to meet you, " user-name "."))))

(defn needs-setup?
  "Check if this xia instance needs first-run setup."
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
  (println "Setup complete! Starting xia...")
  (println))
