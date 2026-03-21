(ns xia.channel.terminal
  "Terminal channel — interactive REPL for talking to xia."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.db :as db]
            [xia.agent :as agent]
            [xia.skill :as skill]
            [xia.skill.openclaw :as openclaw-skill]
            [xia.tool :as tool]
            [xia.memory :as memory]
            [xia.identity :as identity]
            [xia.hippocampus :as hippo]
            [xia.prompt :as prompt]
            [xia.working-memory :as wm]
            [xia.context :as context]))

(defonce ^:private terminal-statuses (atom {}))

(defn- terminal-prompt
  "Prompt the user for input in the terminal.
   Supports masked input for passwords via java.io.Console."
  [label & {:keys [mask?] :or {mask? false}}]
  (if mask?
    ;; Use Console.readPassword for masked input (hides typing)
    (if-let [console (System/console)]
      (String. (.readPassword console "  %s: " (into-array Object [label])))
      ;; No console (e.g., running in IDE) — fall back to read-line with warning
      (do (print (str "  " label " (input visible): "))
          (flush)
          (or (read-line) "")))
    (do (print (str "  " label ": "))
        (flush)
        (or (read-line) ""))))

(defn- terminal-approval
  "Prompt the user to approve a privileged tool for the current session."
  [{:keys [tool-id tool-name description arguments reason]}]
  (println)
  (println (str "  approval required for tool " (or tool-name (name tool-id))))
  (when (seq description)
    (println (str "  " description)))
  (when (seq reason)
    (println (str "  reason: " reason)))
  (when (seq arguments)
    (println (str "  arguments: " (pr-str arguments))))
  (let [answer (-> (terminal-prompt "Allow for this session? [y/N]")
                   str/trim
                   str/lower-case)]
    (println)
    (#{"y" "yes"} answer)))

(defn- terminal-status
  [{:keys [session-id state message partial-content]}]
  (let [sid  (or (some-> session-id str) :default)
        display-message (or partial-content message)
        next {:state state :message display-message}]
    (cond
      (= :done state)
      (swap! terminal-statuses dissoc sid)

      (str/blank? display-message)
      nil

      (= next (get @terminal-statuses sid))
      nil

      :else
      (do
        (swap! terminal-statuses assoc sid next)
        (println (str "xia: " display-message))
        (flush)
        (when (= :error state)
          (swap! terminal-statuses dissoc sid))))))

(defn start!
  "Start the terminal REPL loop."
  []
  ;; Register the terminal prompt handler for interactive credential input
  (prompt/register-prompt! :terminal terminal-prompt)
  (prompt/register-approval! :terminal terminal-approval)
  (prompt/register-status! :terminal terminal-status)
  (let [session-id (db/create-session! :terminal)]
    ;; Initialize working memory with warm start
    (wm/ensure-wm! session-id)
    (println)
    (println (str "xia ready. Type your message (or /quit to exit, /help for commands)"))
    (println)
    (loop []
      (print "you> ")
      (flush)
      (when-let [input (read-line)]
        (let [trimmed (str/trim input)]
          (cond
            (or (= trimmed "/quit") (= trimmed "/exit"))
            (do (println "consolidating memories...")
                (let [topics (:topics (wm/get-wm session-id))]
                  (wm/snapshot! session-id)
                  (hippo/record-conversation! session-id :terminal :topics topics)
                  (wm/clear-wm! session-id))
                (println "goodbye.")
                (System/exit 0))

            (= trimmed "/help")
            (do (println)
                (println "Commands:")
                (println "  /quit              — exit xia")
                (println "  /skills            — list installed skills")
                (println "  /tools             — list installed tools")
                (println "  /memories          — show recent memories")
                (println "  /identity          — show current identity")
                (println "  /context           — show current working memory context")
                (println "  /compact           — summarize older messages to save tokens")
                (println "  /import-skill <f>  — import a skill (.md or .edn)")
                (println "  /import-openclaw-skill <src> — import an OpenClaw skill bundle (dir, .zip, or zip URL)")
                (println "  /import-tool <f>   — import a tool (.edn)")
                (println)
                (recur))

            (= trimmed "/skills")
            (do (let [skills (skill/all-enabled-skills)]
                  (if (seq skills)
                    (doseq [s skills]
                      (println (str "  " (name (:skill/id s))
                                    " — " (:skill/description s))))
                    (println "  (no skills installed)")))
                (println)
                (recur))

            (= trimmed "/tools")
            (do (let [tools (db/list-tools)]
                  (if (seq tools)
                    (doseq [t tools]
                      (println (str "  " (name (:tool/id t))
                                    " — " (:tool/description t))))
                    (println "  (no tools installed)")))
                (println)
                (recur))

            (= trimmed "/memories")
            (do (let [episodes (memory/recent-episodes 10)]
                  (if (seq episodes)
                    (doseq [ep episodes]
                      (println (str "  • " (:summary ep))))
                    (println "  (no memories yet)")))
                (println)
                (recur))

            (= trimmed "/identity")
            (do (let [soul (identity/get-soul)]
                  (doseq [[k v] soul]
                    (println (str "  " (name k) ": " v))))
                (println)
                (recur))

            (= trimmed "/context")
            (do (let [wm-ctx (wm/wm->context session-id)]
                  (if wm-ctx
                    (do (when (:topics wm-ctx)
                          (println (str "  Topic: " (:topics wm-ctx))))
                      (println)
                      (if (seq (:entities wm-ctx))
                        (do (println "  Active entities:")
                          (doseq [e (:entities wm-ctx)]
                            (println (str "    " (:name e)
                                          " (" (name (:type e)) ")"
                                          " [relevance=" (format "%.2f" (double (:relevance e)))
                                          (when (:pinned? e) " pinned")
                                          "]"))))
                        (println "  (no active entities)"))
                      (println)
                      (if (seq (:episodes wm-ctx))
                        (do (println "  Relevant episodes:")
                          (doseq [ep (:episodes wm-ctx)]
                            (println (str "    • " (:summary ep)))))
                        (println "  (no relevant episodes)"))
                      (println)
                      (let [prompt (context/assemble-system-prompt session-id)]
                        (println (str "  System prompt: ~"
                                      (context/estimate-tokens prompt)
                                      " tokens"))))
                    (println "  (no working memory active)")))
                (println)
                (recur))

            (= trimmed "/compact")
            (do (println "  compacting message history...")
                (let [messages (db/session-messages session-id)
                      msg-count (count messages)
                      tokens (->> messages (map #(context/estimate-tokens (:content %))) (reduce +))]
                  (println (str "  current: " msg-count " messages, ~" tokens " tokens"))
                  (if (> msg-count 4)
                    (let [compacted (context/compact-history
                                     (mapv (fn [{:keys [role content]}]
                                             {:role (name role) :content content})
                                           messages)
                                     (quot (long tokens) 2)
                                     {:workload :history-compaction})
                          new-tokens (->> compacted (map #(context/estimate-tokens (:content %))) (reduce +))]
                      (println (str "  compacted to ~" new-tokens " tokens (recap applied)")))
                    (println "  (too few messages to compact)")))
                (println)
                (recur))

            (str/starts-with? trimmed "/import-skill ")
            (do (let [path (subs trimmed 14)]
                  (try
                    (let [result (skill/import-skill-file! path)]
                      (println (str "  imported skill: "
                                    (if (vector? result)
                                      (str/join ", " (map :name result))
                                      (:name result)))))
                    (catch Exception e
                      (println (str "  error: " (.getMessage e))))))
                (println)
                (recur))

            (str/starts-with? trimmed "/import-openclaw-skill ")
            (do (let [source (subs trimmed 24)]
                  (try
                    (let [result (openclaw-skill/import-openclaw-source! source)]
                      (println (str "  imported OpenClaw skill: " (name (:skill-id result))))
                      (doseq [warning (:warnings result)]
                        (println (str "  warning: " warning))))
                    (catch Exception e
                      (println (str "  error: " (.getMessage e)))
                      (doseq [error (:errors (ex-data e))]
                        (println (str "  detail: " error))))))
                (println)
                (recur))

            (str/starts-with? trimmed "/import-tool ")
            (do (let [path (subs trimmed 13)]
                  (try
                    (let [result (tool/import-tool-file! path)]
                      (println (str "  imported tool: "
                                    (if (vector? result)
                                      (str/join ", " (map :name result))
                                      (:name result)))))
                    (catch Exception e
                      (println (str "  error: " (.getMessage e))))))
                (println)
                (recur))

            (empty? trimmed)
            (recur)

            :else
            (do (try
                  (let [response (agent/process-message session-id trimmed :channel :terminal)]
                    (println)
                    (println (str "xia> " response))
                    (println))
                  (catch Exception e
                    (log/error e "Error processing message")
                    (println (str "  error: " (.getMessage e)))
                    (println)))
                (recur))))))))
