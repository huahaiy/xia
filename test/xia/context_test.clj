(ns xia.context-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [datalevin.embedding :as emb]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.context :as ctx]
            [xia.working-memory :as wm]))

(use-fixtures :each th/with-test-db)

(defn- token-rich-text
  [prefix n]
  (str/join " " (map #(str prefix "-" %) (range n))))

;; ---------------------------------------------------------------------------
;; Token estimation
;; ---------------------------------------------------------------------------

(deftest test-estimate-tokens
  (testing "blank input"
    (is (= 0 (ctx/estimate-tokens "")))
    (is (= 0 (ctx/estimate-tokens nil))))

  (testing "uses the embedding provider token counter when available"
    (let [provider (reify emb/ITokenCounter
                     (token-count* [_ _item _opts]
                       42)
                     (truncate-item* [_ item _max-tokens _opts]
                       item))]
      (with-redefs [db/current-embedding-provider (constantly provider)]
        (is (= 42 (ctx/estimate-tokens "hello world!"))))))

  (testing "reuses cached token estimates for repeated text on the same provider"
    (#'xia.context/clear-token-estimate-cache!)
    (let [calls    (atom 0)
          provider (reify emb/ITokenCounter
                     (token-count* [_ _item _opts]
                       (swap! calls inc)
                       17)
                     (truncate-item* [_ item _max-tokens _opts]
                       item))]
      (with-redefs [db/current-embedding-provider (constantly provider)]
        (is (= 17 (ctx/estimate-tokens "repeat this")))
        (is (= 17 (ctx/estimate-tokens "repeat this")))
        (is (= 1 @calls)))))

  (testing "keeps cached token estimates scoped to the active provider"
    (#'xia.context/clear-token-estimate-cache!)
    (let [calls-a   (atom 0)
          calls-b   (atom 0)
          provider-a (reify emb/ITokenCounter
                       (token-count* [_ _item _opts]
                         (swap! calls-a inc)
                         11)
                       (truncate-item* [_ item _max-tokens _opts]
                         item))
          provider-b (reify emb/ITokenCounter
                       (token-count* [_ _item _opts]
                         (swap! calls-b inc)
                         23)
                       (truncate-item* [_ item _max-tokens _opts]
                         item))]
      (with-redefs [db/current-embedding-provider (constantly provider-a)]
        (is (= 11 (ctx/estimate-tokens "shared text")))
        (is (= 11 (ctx/estimate-tokens "shared text"))))
      (with-redefs [db/current-embedding-provider (constantly provider-b)]
        (is (= 23 (ctx/estimate-tokens "shared text")))
        (is (= 23 (ctx/estimate-tokens "shared text"))))
      (is (= 1 @calls-a))
      (is (= 1 @calls-b))))

  (testing "test provider counts lexical tokens instead of chars"
    (is (= 2 (ctx/estimate-tokens "hello world!")))
    (is (pos? (ctx/estimate-tokens "some text"))))

  (testing "fallback heuristic is still available when no provider exists"
    (with-redefs [db/current-embedding-provider (constantly nil)]
      (is (= 3 (ctx/estimate-tokens "hello world!")))
      (is (pos? (ctx/estimate-tokens "some text")))))

  (testing "CJK text is counted by the provider instead of the char heuristic"
    (let [text "你好世界"
          estimate (ctx/estimate-tokens text)]
      (is (pos? estimate))
      (is (= 1 estimate))))

  (testing "long code identifiers use model token counts"
    (let [identifier "veryLongIdentifierNameWithSeveralCamelCaseSegments"
          estimate   (ctx/estimate-tokens identifier)]
      (is (pos? estimate))
      (is (= 1 estimate))))

  (testing "path-like code spans use lexical segments rather than raw chars"
    (let [path     "src/xia/http_client/really_long_identifier_name.cljs"
          estimate (ctx/estimate-tokens path)]
      (is (pos? estimate))
      (is (= 9 estimate)))))

;; ---------------------------------------------------------------------------
;; flatten-props
;; ---------------------------------------------------------------------------

(deftest test-flatten-props
  (let [fp #'xia.context/flatten-props]
    (testing "flat properties"
      (is (= ["location: Seattle" "role: engineer"]
             (sort (fp {:location "Seattle" :role "engineer"})))))

    (testing "nested properties"
      (let [result (fp {:work {:company "Acme" :title "CTO"}})]
        (is (some #(= "work.company: Acme" %) result))
        (is (some #(= "work.title: CTO" %) result))))

    (testing "mixed flat and nested"
      (let [result (fp {:location "Seattle" :work {:company "Acme"}})]
        (is (some #(= "location: Seattle" %) result))
        (is (some #(= "work.company: Acme" %) result))))

    (testing "empty map"
      (is (= [] (fp {}))))

    (testing "deeply nested"
      (let [result (fp {:a {:b {:c "deep"}}})]
        (is (= ["a.b.c: deep"] result))))))

(deftest test-config-parsers-ignore-reader-eval
  (db/set-config! :context/budget "#=(+ 1 2)")
  (db/set-config! :context/history-budget "#=(+ 1 2)")
  (db/set-config! :context/recent-history-message-limit "#=(+ 1 2)")
  (is (= @#'xia.context/default-system-prompt-budget
         (#'xia.context/configured-system-prompt-budget)))
  (is (= 8000 (#'xia.context/configured-history-budget)))
  (is (= 24 (ctx/recent-history-message-limit-config))))

;; ---------------------------------------------------------------------------
;; render-entity
;; ---------------------------------------------------------------------------

(deftest test-render-entity
  (let [re #'xia.context/render-entity]
    (testing "entity with properties, facts, and edges"
      (let [line (re {:name       "Hong"
                      :type       :person
                      :properties {:location "Seattle" :role "engineer"}
                      :facts      [{:content "prefers vim" :confidence 1.0}]
                      :edges      {:outgoing [{:type :works-at :target "Acme"}]
                                   :incoming []}})]
        (is (str/starts-with? line "- Hong (person): "))
        (is (str/includes? line "location: Seattle"))
        (is (str/includes? line "role: engineer"))
        (is (str/includes? line "prefers vim"))
        (is (str/includes? line "works-at→Acme"))))

    (testing "entity with only properties"
      (let [line (re {:name       "Seattle"
                      :type       :place
                      :properties {:state "Washington"}
                      :facts      []
                      :edges      {:outgoing [] :incoming []}})]
        (is (str/includes? line "state: Washington"))))

    (testing "entity with no properties"
      (let [line (re {:name  "Clojure"
                      :type  :concept
                      :facts [{:content "functional language" :confidence 1.0}]
                      :edges {:outgoing [] :incoming []}})]
        (is (str/includes? line "functional language"))
        (is (not (str/includes? line "nil")))))

    (testing "entity with nil properties"
      (let [line (re {:name       "Bob"
                      :type       :person
                      :properties nil
                      :facts      [{:content "likes pizza" :confidence 0.8}]
                      :edges      {:outgoing [] :incoming []}})]
        (is (str/includes? line "likes pizza"))))

    (testing "entity with empty properties"
      (let [line (re {:name       "Carol"
                      :type       :person
                      :properties {}
                      :facts      [{:content "works remotely" :confidence 0.9}]
                      :edges      {:outgoing [] :incoming []}})]
        (is (str/includes? line "works remotely"))))

    (testing "facts below the prompt floor are filtered"
      (let [line (re {:name  "Test"
                      :type  :concept
                      :facts [{:content "high conf" :confidence 0.8}
                              {:content "floor conf" :confidence 0.1}
                              {:content "too low"    :confidence 0.05}]
                      :edges {:outgoing [] :incoming []}})]
        (is (str/includes? line "high conf"))
        (is (str/includes? line "floor conf"))
        (is (not (str/includes? line "too low")))))

    (testing "utility influences fact ordering"
      (let [^String line (re {:name  "Ranked"
                              :type  :concept
                              :facts [{:content "high confidence" :confidence 0.85 :utility 0.0}
                                      {:content "high utility" :confidence 0.7 :utility 1.0}]
                              :edges {:outgoing [] :incoming []}})]
        (is (< (.indexOf line "high utility")
               (.indexOf line "high confidence")))))

    (testing "entity with no detail"
      (let [line (re {:name "Empty"
                      :type :concept
                      :facts []
                      :edges {:outgoing [] :incoming []}})]
        (is (= "- Empty (concept)" line))))))

;; ---------------------------------------------------------------------------
;; render-entities (budget-aware)
;; ---------------------------------------------------------------------------

(deftest test-render-entities-budget
  (testing "renders within budget"
    (let [entities [{:name "A" :type :concept :facts [] :edges {:outgoing [] :incoming []}}
                    {:name "B" :type :concept :facts [] :edges {:outgoing [] :incoming []}}]
          result (ctx/render-entities entities 1000)]
      (is (str/includes? result "### Known"))
      (is (str/includes? result "- A"))
      (is (str/includes? result "- B"))))

  (testing "truncates when budget exceeded"
    (let [entities (mapv (fn [i]
                           {:name  (str "Entity" i)
                            :type  :concept
                            :facts [{:content (apply str (repeat 100 "x")) :confidence 1.0}]
                            :edges {:outgoing [] :incoming []}})
                         (range 20))
          result (ctx/render-entities entities 50)]
      ;; Should have some entities but not all 20
      (is (str/includes? result "### Known"))
      (is (< (count (re-seq #"- Entity" result)) 20))))

  (testing "sorts entities by relevance before applying the budget"
    (let [render-entity #'xia.context/render-entity
          low    {:name "Low" :type :concept :relevance 0.1
                  :facts [] :edges {:outgoing [] :incoming []}}
          high   {:name "Top" :type :concept :relevance 0.9
                  :facts [] :edges {:outgoing [] :incoming []}}
          budget (+ 8 (long (ctx/estimate-tokens (render-entity high))))
          result (ctx/render-entities [low high] budget)]
      (is (str/includes? result "- Top"))
      (is (not (str/includes? result "- Low")))))

  (testing "nil entities"
    (is (nil? (ctx/render-entities nil 1000))))

  (testing "empty entities"
    (is (nil? (ctx/render-entities [] 1000)))))

;; ---------------------------------------------------------------------------
;; render-episodes
;; ---------------------------------------------------------------------------

(deftest test-render-episodes
  (let [re #'xia.context/render-episodes]
    (testing "renders episodes with dates"
      (let [episodes [{:summary   "Discussed Clojure"
                       :timestamp (java.util.Date.)
                       :relevance 0.8}]
            result (re episodes 500)]
        (is (str/includes? result "### Recent"))
        (is (str/includes? result "Discussed Clojure"))))

    (testing "empty episodes"
      (is (nil? (re [] 500))))

    (testing "nil episodes"
      (is (nil? (re nil 500))))))

;; ---------------------------------------------------------------------------
;; render-local-docs
;; ---------------------------------------------------------------------------

(deftest test-render-local-docs
  (let [rd #'xia.context/render-local-docs]
    (testing "renders local documents with summary and matched chunks"
      (let [docs [{:name "paper.pdf"
                   :media-type "application/pdf"
                   :summary "This paper studies retrieval in scientific corpora."
                   :matched-chunks [{:summary "The relevant chunk covers scientific corpora and evaluation."}]
                   :relevance 0.8}]
            result (rd docs 500)]
        (is (str/includes? result "### Local Documents"))
        (is (str/includes? result "paper.pdf"))
        (is (str/includes? result "scientific corpora"))
        (is (str/includes? result "matches:"))))

    (testing "budget-aware truncation"
      (let [docs (mapv (fn [i]
                         (let [i* (long i)]
                         {:name (str "doc-" i ".txt")
                          :media-type "text/plain"
                          :preview (token-rich-text (str "preview" i) 40)
                          :relevance (- 1.0 (* i* 0.1))}))
                       (range 10))
            result (rd docs 80)]
        (is (str/includes? result "### Local Documents"))
        (is (< (count (re-seq #"- doc-" result)) 10))))

    (testing "empty docs"
      (is (nil? (rd [] 500))))))

;; ---------------------------------------------------------------------------
;; render-skills
;; ---------------------------------------------------------------------------

(deftest test-render-skills
  (let [rs #'xia.context/render-skills]
    (testing "renders skills"
      (let [skills [{:skill/name "email-drafting" :skill/content "Write emails professionally."}]
            result (rs skills 1000)]
        (is (str/includes? result "## Skills"))
        (is (str/includes? result "### email-drafting"))
        (is (str/includes? result "Write emails professionally."))))

    (testing "budget-aware truncation"
      (let [skills (mapv (fn [i]
                           {:skill/name    (str "skill-" i)
                            :skill/content (token-rich-text (str "skill" i) 80)})
                         (range 10))
            result (rs skills 200)]
        ;; Should not include all 10 skills
        (is (< (count (re-seq #"### skill-" result)) 10))))

    (testing "empty skills"
      (is (nil? (rs [] 1000))))))

;; ---------------------------------------------------------------------------
;; assemble-system-prompt (integration)
;; ---------------------------------------------------------------------------

(deftest test-provider-specific-system-prompt-budget
  (let [budget (#'xia.context/resolve-system-prompt-budget
                 {:llm.provider/system-prompt-budget 16000})]
    (is (= 16000 (:total budget)))
    (is (= 1200 (:identity budget)))
    (is (= 400 (:topic budget)))
    (is (= 6000 (:entities budget)))
    (is (= 1600 (:local-docs budget)))
    (is (= 2000 (:episodes budget)))
    (is (= 6000 (:skills budget)))))

(deftest test-configured-recent-history-message-limit
  (is (= 24 (#'xia.context/recent-history-message-limit {})))

  (db/set-config! :context/recent-history-message-limit "12")
  (is (= 12 (#'xia.context/recent-history-message-limit {})))

  (db/set-config! :context/recent-history-message-limit "2")
  (is (= 4 (#'xia.context/recent-history-message-limit {}))
      "Configured values below the floor should clamp to 4")

  (db/set-config! :context/recent-history-message-limit "not-edn")
  (is (= 24 (#'xia.context/recent-history-message-limit {}))
      "Invalid config should fall back to the default"))

(deftest test-assemble-system-prompt
  ;; Set up identity
  (db/set-identity! :name "TestXia")
  (db/set-identity! :personality "Helpful")
  (db/set-identity! :guidelines "Be nice")
  (db/set-identity! :description "A test assistant")

  ;; No WM active, no skills
  (let [sid    (db/create-session! :terminal)
        _      (wm/create-wm! sid)
        _      (swap! @#'xia.working-memory/wm-atom
                      assoc-in [sid :local-doc-refs]
                      [{:doc-id (random-uuid)
                        :name "notes.md"
                        :media-type "text/markdown"
                        :summary "Important local grounding material."
                        :preview "Important local grounding material."
                        :matched-chunks [{:summary "Grounding details for the current task."}]
                        :relevance 0.8}])
        prompt (ctx/assemble-system-prompt sid)]
    (testing "includes identity"
      (is (str/includes? prompt "TestXia")))

    (testing "is a string"
      (is (string? prompt)))

    (testing "includes local documents"
      (is (str/includes? prompt "### Local Documents"))
      (is (str/includes? prompt "notes.md")))

    (wm/clear-wm! sid)))

(deftest test-assemble-system-prompt-data-reuses-renderer-token-totals
  (let [sid            (db/create-session! :terminal)
        estimated-texts (atom [])]
    (with-redefs [xia.context/render-identity         (constantly "IDENTITY")
                  xia.context/render-topic            (constantly "TOPIC")
                  xia.context/render-entities-data    (fn [_ _]
                                                        {:content "ENTITY-SECTION"
                                                         :tokens  11
                                                         :used-fact-eids [7]})
                  xia.context/render-local-docs-data  (fn [_ _]
                                                        {:content "DOC-SECTION"
                                                         :tokens  13})
                  xia.context/render-episodes-data    (fn [_ _]
                                                        {:content "EPISODE-SECTION"
                                                         :tokens  17})
                  xia.context/render-skills-data      (fn [_ _]
                                                        {:content "SKILL-SECTION"
                                                         :tokens  19})
                  xia.context/estimate-tokens         (fn [text]
                                                        (swap! estimated-texts conj text)
                                                        (case text
                                                          "IDENTITY" 31
                                                          "TOPIC"    7
                                                          ""         0
                                                          (throw (ex-info "unexpected estimate"
                                                                          {:text text}))))
                  xia.working-memory/wm->context      (constantly {:topics :present
                                                                   :entities [:ignored]
                                                                   :local-docs [:ignored]
                                                                   :episodes [:ignored]
                                                                   :turn-count 0})
                  xia.skill/skills-for-context        (constantly [:ignored])]
      (let [result (ctx/assemble-system-prompt-data sid)]
        (is (= [7] (:used-fact-eids result)))
        (is (str/includes? (:prompt result) "ENTITY-SECTION"))
        (is (str/includes? (:prompt result) "DOC-SECTION"))
        (is (str/includes? (:prompt result) "EPISODE-SECTION"))
        (is (str/includes? (:prompt result) "SKILL-SECTION"))
        (is (= ["IDENTITY" "TOPIC"] @estimated-texts))))))

;; ---------------------------------------------------------------------------
;; compact-history
;; ---------------------------------------------------------------------------

(deftest test-compact-history
  (testing "short history passes through"
    (let [msgs [{:role :user :content "hi"}
                {:role :assistant :content "hello"}]
          result (ctx/compact-history msgs 10000)]
      (is (= msgs result))))

  (testing "few messages pass through regardless of budget"
    (let [msgs [{:role :user :content (apply str (repeat 5000 "x"))}
                {:role :assistant :content (apply str (repeat 5000 "x"))}]
          result (ctx/compact-history msgs 10)]
      (is (= msgs result) "Should not compact when <= 4 messages")))

  (testing "compacts long history via LLM"
    (let [msgs (vec (for [i (range 10)]
                      {:role    (if (even? i) :user :assistant)
                       :content (str "message " i " " (token-rich-text (str "m" i) 200))}))
          called? (atom false)]
      (with-redefs [xia.llm/chat-simple (fn [_]
                                          (reset! called? true)
                                          "Recap of earlier conversation.")]
        (let [result (ctx/compact-history msgs 100)]
          (is @called? "LLM should have been called for compaction")
          (is (< (count result) (count msgs)) "Should have fewer messages")))))

  (testing "forwards workload routing to the LLM"
    (let [msgs (vec (for [i (range 10)]
                      {:role    (if (even? i) :user :assistant)
                       :content (str "message " i " " (token-rich-text (str "m" i) 200))}))
          opts-seen (atom nil)]
      (with-redefs [xia.llm/chat-simple (fn [_messages & opts]
                                          (reset! opts-seen opts)
                                          "Recap of earlier conversation.")]
        (ctx/compact-history msgs 100 {:workload :history-compaction})
        (is (= [:workload :history-compaction] @opts-seen)))))

  (testing "fallback trimming without summary preserves chronological order"
    (let [msgs   (vec (for [i (range 8)]
                        {:role    (if (even? i) :user :assistant)
                         :content (str "message-" i)}))
          result (with-redefs [xia.context/estimate-tokens
                               (fn [text]
                                 (case text
                                   "message-0" 10
                                   "message-1" 10
                                   "message-2" 10
                                   "message-3" 10
                                   "message-4" 10
                                   "message-5" 10
                                   "message-6" 80
                                   "message-7" 10
                                   0))]
                   (ctx/compact-history msgs 100 {:allow-summary? false}))]
      (is (= ["message-4" "message-5" "message-6" "message-7"]
             (mapv :content result)))))

  (testing "includes tool usage and results in the compaction transcript"
    (let [tool-calls [{"id" "call_1"
                       "function" {"name" "web-search"
                                   "arguments" "{\"q\":\"weather in sf\"}"}}]
          msgs       [{:role "user"
                       :content (str "Please check the weather. " (token-rich-text "weather" 120))}
                      {:role "assistant"
                       :content "I will search for the latest forecast."
                       :tool_calls tool-calls}
                      {:role "tool"
                       :tool_call_id "call_1"
                       :content "{\"success?\":true,\"results\":[{\"title\":\"Forecast\"}]}"}
                      {:role "assistant"
                       :content "The forecast says it will rain later today."}
                      {:role "user"
                       :content (str "Thanks. " (token-rich-text "thanks" 120))}
                      {:role "assistant"
                       :content (str "You're welcome. " (token-rich-text "welcome" 120))}
                      {:role "user"
                       :content (str "Anything else? " (token-rich-text "else" 120))}
                      {:role "assistant"
                       :content (str "No. " (token-rich-text "no" 120))}]
          llm-input  (atom nil)]
      (with-redefs [xia.llm/chat-simple (fn [messages & _]
                                          (reset! llm-input messages)
                                          "Recap of earlier conversation.")]
        (ctx/compact-history msgs 100)
        (is (str/includes? (get-in @llm-input [0 :content])
                           "important tool usage"))
        (is (str/includes? (get-in @llm-input [1 :content])
                           "assistant requested tool[call_1]: web-search"))
        (is (str/includes? (get-in @llm-input [1 :content])
                           "args={\"q\":\"weather in sf\"}"))
        (is (str/includes? (get-in @llm-input [1 :content])
                           "tool result[call_1]: {\"success?\":true,\"results\":[{\"title\":\"Forecast\"}]}"))))))

;; ---------------------------------------------------------------------------
;; build-messages
;; ---------------------------------------------------------------------------

(deftest test-build-messages
  ;; Set up identity
  (db/set-identity! :name "TestXia")
  (db/set-identity! :description "Test")

  (let [sid (db/create-session! :terminal)]
    (db/add-message! sid :user "hello")
    (db/add-message! sid :assistant "hi there")

    (db/add-message! sid :user "what's up?")

    (let [msgs (ctx/build-messages sid)]
      (testing "starts with system message"
        (is (= "system" (:role (first msgs)))))

      (testing "preserves the current user turn exactly once"
        (let [user-msgs (filter #(= "user" (:role %)) msgs)]
          (is (= 2 (count user-msgs)))
          (is (= "what's up?" (:content (last user-msgs))))))

      (testing "includes history"
        (is (>= (count msgs) 3))))))

(deftest test-build-messages-serializes-structured-tool-results-for-llm
  (db/set-identity! :name "TestXia")
  (db/set-identity! :description "Test")
  (let [sid        (db/create-session! :terminal)
        tool-calls [{"id"       "call_1"
                     "function" {"name"      "web-fetch"
                                 "arguments" "{\"url\":\"https://example.com\"}"}}]]
    (db/add-message! sid :assistant "Fetching page" :tool-calls tool-calls)
    (db/add-message! sid :tool nil
                     :tool-result {"status" 200
                                   "title" "Example Domain"}
                     :tool-id "call_1")
    (with-redefs [xia.context/assemble-system-prompt (fn [_sid _opts] "system")
                  xia.context/compact-history        (fn [messages _budget & _] messages)]
      (let [msgs     (ctx/build-messages sid)
            tool-msg (last msgs)]
        (is (= "tool" (:role tool-msg)))
        (is (= "call_1" (:tool_call_id tool-msg)))
        (is (= "{\"status\":200,\"title\":\"Example Domain\"}"
               (:content tool-msg)))))))

(deftest test-build-messages-restores-missing-tool-call-id-from-assistant-call
  (db/set-identity! :name "TestXia")
  (db/set-identity! :description "Test")
  (let [sid        (db/create-session! :terminal)
        tool-calls [{"id"       "call_1"
                     "function" {"name"      "web-fetch"
                                 "arguments" "{\"url\":\"https://example.com\"}"}}]]
    (db/add-message! sid :assistant "Fetching page" :tool-calls tool-calls)
    (db/add-message! sid :tool nil
                     :tool-result {"status" 200
                                   "title" "Example Domain"})
    (with-redefs [xia.context/assemble-system-prompt (fn [_sid _opts] "system")
                  xia.context/compact-history        (fn [messages _budget & _] messages)]
      (let [msgs     (ctx/build-messages sid)
            tool-msg (last msgs)]
        (is (= "tool" (:role tool-msg)))
        (is (= "call_1" (:tool_call_id tool-msg)))
        (is (= "{\"status\":200,\"title\":\"Example Domain\"}"
               (:content tool-msg)))))))

(deftest test-build-messages-data-returns-used-fact-eids
  (let [sid (db/create-session! :terminal)]
    (with-redefs [xia.context/assemble-system-prompt-data (fn [_sid _opts]
                                                            {:prompt "system"
                                                             :used-fact-eids [1 2 3]})
                  xia.context/compact-history             (fn [messages _budget & _]
                                                            messages)]
      (let [result (#'xia.context/build-messages-data
                     sid
                     {:provider {:llm.provider/id :default}})]
        (is (map? result))
        (is (vector? (:messages result)))
        (is (= [1 2 3] (:used-fact-eids result)))))))

(deftest test-build-messages-uses-provider-history-budget
  (db/set-identity! :name "TestXia")
  (db/set-identity! :description "Test")
  (db/upsert-provider! {:id             :openai
                        :name           "OpenAI"
                        :base-url       "https://api.openai.com/v1"
                        :model          "gpt-5"
                        :default?       true
                        :history-budget 12345})
  (let [sid             (db/create-session! :terminal)
        captured-budget (atom nil)]
    (doseq [i (range 6)]
      (db/add-message! sid
                       (if (even? i) :user :assistant)
                       (apply str "message " i " " (repeat 300 "x"))))
    (with-redefs [xia.context/compact-history
                  (fn [messages budget & _]
                    (reset! captured-budget budget)
                    messages)]
      (ctx/build-messages sid))
    (is (= 12345 @captured-budget))))

(deftest test-build-messages-data-reuses-session-history-recap
  (let [sid       (db/create-session! :terminal)
        llm-calls (atom [])]
    (db/set-config! :context/recent-history-message-limit "4")
    (doseq [i (range 6)]
      (db/add-message! sid
                       (if (even? i) :user :assistant)
                       (str "message " i " " (token-rich-text (str "m" i) 60))))
    (with-redefs [xia.context/assemble-system-prompt-data
                  (fn [_sid _opts]
                    {:prompt "system"
                     :used-fact-eids []})
                  xia.llm/chat-simple
                  (fn [messages & _]
                    (swap! llm-calls conj messages)
                    (str "recap-" (count @llm-calls)))]
      (let [result (#'xia.context/build-messages-data
                    sid
                    {:provider {:llm.provider/id :default}})
            recap  (db/session-history-recap sid)]
        (is (= 1 (count @llm-calls)))
        (is (= 2 (:message-count recap)))
        (is (= "system" (get-in result [:messages 0 :role])))
        (is (str/includes? (get-in result [:messages 1 :content]) "recap-1")))

      (#'xia.context/build-messages-data
       sid
       {:provider {:llm.provider/id :default}})
      (is (= 1 (count @llm-calls))
          "Stored recap should be reused while the recent window is unchanged")

      (db/add-message! sid :user (str "message 6 " (token-rich-text "m6" 60)))
      (#'xia.context/build-messages-data
       sid
       {:provider {:llm.provider/id :default}})
      (is (= 2 (count @llm-calls)))
      (is (= 3 (:message-count (db/session-history-recap sid))))
      (is (str/includes? (get-in (last @llm-calls) [1 :content])
                         "Newly archived messages:"))
      (is (str/includes? (get-in (last @llm-calls) [1 :content])
                         "message 2"))
      (is (not (str/includes? (get-in (last @llm-calls) [1 :content])
                              "message 0"))
          "Incremental recap updates should only archive the newly evicted message"))))

(deftest test-build-messages-data-reuses-system-prompt-across-unchanged-wm
  (let [sid           (db/create-session! :terminal)
        prompt-calls  (atom 0)
        history-calls (atom 0)]
    (wm/create-wm! sid)
    (swap! @#'xia.working-memory/wm-atom assoc-in [sid :topics] "billing emails")
    (with-redefs [xia.context/assemble-system-prompt-data
                  (fn [_sid _opts]
                    (swap! prompt-calls inc)
                    {:prompt "system"
                     :used-fact-eids [1]})
                  xia.context/build-history-with-session-recap
                  (fn [_sid _opts]
                    (swap! history-calls inc)
                    {:messages []
                     :history-recap-updated? false})]
      (let [first-result (#'xia.context/build-messages-data
                          sid
                          {:provider {:llm.provider/id :default}})
            second-result (#'xia.context/build-messages-data
                           sid
                           {:provider {:llm.provider/id :default}
                            :system-prompt-cache-entry (:system-prompt-cache-entry first-result)})]
        (is (= 1 @prompt-calls))
        (is (= 2 @history-calls))
        (is (= "system" (get-in first-result [:messages 0 :content])))
        (is (= "system" (get-in second-result [:messages 0 :content])))
        (is (= [1] (:used-fact-eids second-result)))))))

(deftest test-build-messages-data-invalidates-system-prompt-cache-when-wm-changes
  (let [sid          (db/create-session! :terminal)
        prompt-calls (atom 0)]
    (wm/create-wm! sid)
    (swap! @#'xia.working-memory/wm-atom assoc-in [sid :topics] "billing emails")
    (with-redefs [xia.context/assemble-system-prompt-data
                  (fn [_sid _opts]
                    (swap! prompt-calls inc)
                    {:prompt (str "system-" @prompt-calls)
                     :used-fact-eids []})
                  xia.context/build-history-with-session-recap
                  (fn [_sid _opts]
                    {:messages []
                     :history-recap-updated? false})]
      (let [first-result (#'xia.context/build-messages-data
                          sid
                          {:provider {:llm.provider/id :default}})
            _ (swap! @#'xia.working-memory/wm-atom assoc-in [sid :topics] "calendar scheduling")
            second-result (#'xia.context/build-messages-data
                           sid
                           {:provider {:llm.provider/id :default}
                            :system-prompt-cache-entry (:system-prompt-cache-entry first-result)})]
        (is (= 2 @prompt-calls))
        (is (= "system-1" (get-in first-result [:messages 0 :content])))
        (is (= "system-2" (get-in second-result [:messages 0 :content])))))))

(deftest test-build-history-with-session-recap-does-not-load-full-metadata
  (let [sid (db/create-session! :terminal)]
    (doseq [i (range 6)]
      (db/add-message! sid
                       (if (even? i) :user :assistant)
                       (str "message " i " " (token-rich-text (str "m" i) 40))))
    (with-redefs [xia.db/session-message-metadata
                  (fn [& _]
                    (throw (ex-info "full metadata load should not be used" {})))
                  xia.context/summarize-history-text
                  (fn [& _] "recap-windowed")]
      (let [result (#'xia.context/build-history-with-session-recap
                    sid
                    {:recent-message-limit 4})
            message-texts (map :content (:messages result))]
        (is (true? (:history-recap-updated? result)))
        (is (some #(str/includes? % "recap-windowed") message-texts))))))

(deftest test-build-messages-data-preserves-archived-tool-recap
  (let [sid        (db/create-session! :terminal)
        llm-calls  (atom 0)
        tool-calls [{"id" "call_1"
                     "function" {"name" "web-search"
                                 "arguments" "{\"q\":\"weather in sf\"}"}}]]
    (db/set-config! :context/recent-history-message-limit "4")
    (db/add-message! sid :user "Check the weather in San Francisco.")
    (db/add-message! sid :assistant "I will look that up." :tool-calls tool-calls)
    (db/add-message! sid :tool nil
                     :tool-result {"success?" true
                                   "results" [{"title" "Forecast"
                                               "summary" "Rain later today"}]}
                     :tool-id "call_1")
    (db/add-message! sid :assistant "It will rain later today.")
    (db/add-message! sid :user (str "Anything else? " (token-rich-text "else" 60)))
    (db/add-message! sid :assistant (str "No. " (token-rich-text "no" 60)))
    (db/add-message! sid :user (str "Thanks. " (token-rich-text "thanks" 60)))
    (db/add-message! sid :assistant (str "You're welcome. " (token-rich-text "welcome" 60)))
    (with-redefs [xia.context/assemble-system-prompt-data
                  (fn [_sid _opts]
                    {:prompt "system"
                     :used-fact-eids []})
                  xia.llm/chat-simple
                  (fn [_messages & _]
                    (swap! llm-calls inc)
                    "Earlier conversation recap.")]
      (let [result        (#'xia.context/build-messages-data
                           sid
                           {:provider {:llm.provider/id :default}})
            message-texts (map :content (:messages result))]
        (is (= 1 @llm-calls))
        (is (some #(str/includes? % "Archived tool execution recap") message-texts))
        (is (some #(str/includes? % "web-search[call_1]") message-texts))
        (is (some #(str/includes? % "\"title\":\"Forecast\"") message-texts)))
      (with-redefs [xia.llm/chat-simple
                    (fn [& _]
                      (throw (ex-info "should not be called" {})))]
        (let [result        (#'xia.context/build-messages-data
                             sid
                             {:provider {:llm.provider/id :default}})
              message-texts (map :content (:messages result))]
          (is (some #(str/includes? % "Archived tool execution recap") message-texts))
          (is (some #(str/includes? % "web-search[call_1]") message-texts)))))))

(deftest test-build-messages-data-does-at-most-one-summary-call-per-build
  (let [sid       (db/create-session! :terminal)
        llm-calls (atom [])]
    (db/set-config! :context/recent-history-message-limit "4")
    (doseq [i (range 10)]
      (db/add-message! sid
                       (if (even? i) :user :assistant)
                       (str "message " i " " (token-rich-text (str "m" i) 120))))
    (with-redefs [xia.context/assemble-system-prompt-data
                  (fn [_sid _opts]
                    {:prompt "system"
                     :used-fact-eids []})
                  xia.llm/chat-simple
                  (fn [messages & _]
                    (swap! llm-calls conj messages)
                    (str "recap-" (count @llm-calls)))]
      (with-redefs-fn {#'xia.context/resolve-history-budget (constantly 100)}
        (fn []
          (let [result        (#'xia.context/build-messages-data
                               sid
                               {:provider {:llm.provider/id :default}})
                message-texts (map :content (:messages result))]
            (is (= 1 (count @llm-calls)))
            (is (some #(str/includes? % "recap-1") message-texts))
            (is (= "system" (get-in result [:messages 0 :role])))))))))

(deftest test-compact-history-preserves-existing-recap-messages
  (let [tool-recap {:role "system"
                    :content "[Archived tool execution recap:\n- web-search[call_1] args={\"q\":\"weather\"} => {\"success?\":true}\nUse this to avoid repeating tool calls unless the task has changed or fresher data is required.]"}
        convo-recap {:role "system"
                     :content "[Conversation recap: Earlier messages were summarized.]"}
        history     (into [tool-recap convo-recap]
                          (for [i (range 8)]
                            {:role (if (even? i) "user" "assistant")
                             :content (str "message " i " " (token-rich-text (str "m" i) 120))}))]
    (with-redefs [xia.llm/chat-simple (fn [& _] "fresh recap")]
      (let [result (ctx/compact-history history 400)]
        (is (= tool-recap (first result)))
        (is (= convo-recap (second result)))))))
