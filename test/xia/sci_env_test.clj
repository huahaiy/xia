(ns xia.sci-env-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.db :as db]
            [xia.sci-env :as sci-env]
            [xia.test-helpers :refer [seed-node! with-test-db]]))

(use-fixtures :each with-test-db)

(defn- thrown-by
  [f]
  (try
    (f)
    nil
    (catch Throwable t
      t)))

(def ^:private expected-sci-api-matrix
  '{xia.agent
    {:allowed #{run-branch-tasks}}

    xia.artifact
    {:allowed #{create-artifact!
                create-scratch-pad-from-artifact!
                delete-artifact!
                get-artifact
                list-artifacts
                list-visible-artifacts
                read-artifact
                read-visible-artifact
                search-artifacts
                search-visible-artifacts}
     :hidden #{get-session-artifact}}

    xia.board
    {:allowed #{claim-card!
                comment-card!
                create-card!
                get-card
                heartbeat-card!
                list-cards
                update-card!}
     :hidden #{card->body}}

    xia.browser
    {:allowed #{bootstrap-runtime!
                click
                close-session
                fill-form
                install-browser-deps!
                list-sessions
                list-sites
                login
                login-interactive
                navigate
                open-session
                query-elements
                read-page
                release-session
                runtime-status
                screenshot
                wait-for-page}
     :hidden #{browser-runtime-status clone-session}}

    xia.calendar
    {:allowed #{create-event
                delete-event
                find-availability
                list-calendars
                list-events
                read-event
                update-event}}

    xia.cron
    {:allowed #{describe}
     :hidden #{validate!}}

    xia.db
    {:allowed #{get-config q set-config!}
     :hidden #{connect! get-site-cred list-site-creds}}

    xia.email
    {:allowed #{delete-draft
                delete-message
                list-drafts
                list-labels
                list-messages
                read-draft
                read-message
                save-draft
                send-draft
                send-message
                update-message}}

    xia.instance-supervisor
    {:allowed #{instance-management-enabled?
                instance-status
                list-managed-instances
                start-instance!
                stop-instance!}
     :hidden #{configure! instance-command}}

    xia.local-doc
    {:allowed #{list-docs
                list-visible-docs
                read-doc
                read-visible-doc
                search-docs
                search-visible-docs}
     :hidden #{get-doc get-session-doc save-upload!}}

    xia.memory
    {:allowed #{find-node
                node-edges
                node-facts
                node-properties
                query-nodes-by-property
                recall-knowledge
                recent-episodes
                search-episodes
                search-facts
                search-nodes}
     :blocked #{add-edge!
                add-fact!
                add-node!
                record-episode!
                remove-node-property!
                set-node-property!}
     :hidden #{get-node update-fact-utility!}}

    xia.memory-edit
    {:allowed #{correct-fact!}}

    xia.peer
    {:allowed #{chat list-peers}}

    xia.pipeline
    {:allowed #{run!}
     :hidden #{run-pipeline!}}

    xia.schedule
    {:allowed #{create-schedule!
                get-schedule
                list-schedules
                pause-schedule!
                remove-schedule!
                resume-schedule!
                schedule-history
                update-schedule!}
     :hidden #{claim-schedule-run! record-run! schedule-task-id}}

    xia.sci-env
    {:allowed #{check-timeout!}
     :hidden #{eval-string sci-api-manifest}}

    xia.scratch
    {:allowed #{create-pad! delete-pad! edit-pad! get-pad list-pads save-pad!}}

    xia.service
    {:allowed #{list-services request}
     :hidden #{effective-rate-limit-per-minute}}

    xia.skill
    {:allowed #{check-import-update!
                curate-skills!
                match-skills
                patch-skill-section!
                search-skills
                skill-headings
                skill-section}
     :hidden #{import-skill-file! save-skill!}}

    xia.web
    {:allowed #{extract-data fetch-page search-web}
     :hidden #{extract-readable-html}}

    xia.working-memory
    {:allowed #{get-wm pin! unpin! wm->context}
     :hidden #{clear-wm! update-wm!}}

    xia.workspace
    {:allowed #{get-item
                import-item-as-artifact!
                import-item-as-local-doc!
                list-items
                publish-artifact!
                publish-local-doc!
                read-item
                write-note!}
     :hidden #{workspace-dir}}})

(def ^:private expected-denied-symbols
  '#{all-ns
     clojure.java.io/delete-file
     clojure.java.io/file
     clojure.java.io/input-stream
     clojure.java.io/output-stream
     clojure.java.io/reader
     clojure.java.io/resource
     clojure.java.io/writer
     clojure.java.shell/sh
     clojure.java.shell/with-sh-dir
     clojure.java.shell/with-sh-env
     clojure.repl/source
     clojure.repl/source-fn
     find-ns
     find-var
     load-file
     load-reader
     ns-aliases
     ns-interns
     ns-map
     ns-publics
     ns-refers
     ns-resolve
     requiring-resolve
     resolve
     slurp
     spit
     the-ns})

(defn- expected-api-entries
  []
  (->> expected-sci-api-matrix
       (mapcat (fn [[ns-sym {:keys [allowed blocked]}]]
                 (concat
                  (map (fn [api-name]
                         {:symbol (symbol (str ns-sym) (name api-name))
                          :access :allowed})
                       allowed)
                  (map (fn [api-name]
                         {:symbol (symbol (str ns-sym) (name api-name))
                          :access :blocked})
                       blocked))))
       (sort-by (comp str :symbol))
       vec))

(deftest sci-sandbox-blocks-dangerous-host-access
  (let [{:keys [denied-symbols]} (sci-env/sci-api-manifest)]
    (is (= expected-denied-symbols (set denied-symbols)))
    (doseq [denied-symbol denied-symbols]
      (testing (str denied-symbol)
        (is (some? (thrown-by
                    #(sci-env/eval-string (str "(" denied-symbol ")")))))))))

(deftest sci-api-matrix-matches-the-complete-production-exposure-surface
  (let [expected (expected-api-entries)
        actual   (:apis (sci-env/sci-api-manifest))]
    (is (= expected actual))
    (doseq [{:keys [symbol access]} actual]
      (testing (str symbol " is explicitly " (name access))
        (is (true? (sci-env/eval-string (str "(ifn? " symbol ")"))))
        (is (some? (thrown-by
                    #(sci-env/eval-string
                      (str "(resolve '" symbol ")")))))
        (when (= :blocked access)
          (is (some? (thrown-by
                      #(sci-env/eval-string (str "(" symbol ")"))))))))))

(deftest sci-api-matrix-hides-reviewed-host-functions-outside-the-allowlist
  (doseq [[ns-sym {:keys [hidden]}] expected-sci-api-matrix
          hidden-name hidden]
    (let [hidden-symbol (symbol (str ns-sym) (name hidden-name))]
      (testing (str hidden-symbol)
        (is (some? (ns-resolve ns-sym hidden-name))
            "reviewed hidden symbol must exist in the host namespace")
        (is (some? (thrown-by
                    #(sci-env/eval-string (str hidden-symbol)))))))))

(deftest sci-exposed-db-namespace-uses-secret-safe-wrappers
  (seed-node! "SCI matrix visible node" "concept")
  (sci-env/eval-string
   "(xia.db/set-config! :user/theme \"dark\")")
  (db/set-config! :secret/sci-token "hidden")

  (is (= "dark"
         (sci-env/eval-string "(xia.db/get-config :user/theme)")))
  (is (contains?
       (sci-env/eval-string
        "(xia.db/q '[:find ?name :where [?e :kg.node/name ?name]])")
       ["SCI matrix visible node"]))
  (is (some? (thrown-by
              #(sci-env/eval-string "(xia.db/get-config :secret/sci-token)"))))
  (is (some? (thrown-by
              #(sci-env/eval-string "(xia.db/set-config! :secret/sci-token \"leak\")"))))
  (is (some? (thrown-by
              #(sci-env/eval-string
                "(xia.db/q '[:find ?v :where [?e :llm.provider/api-key ?v]])")))))

(deftest sci-memory-mutations-remain-blocked-while-read-api-is-callable
  (seed-node! "SCI matrix memory node" "concept")
  (is (= "SCI matrix memory node"
         (-> (sci-env/eval-string
              "(xia.memory/find-node \"matrix memory\")")
             first
             :name)))
  (doseq [api-name (get-in expected-sci-api-matrix
                           ['xia.memory :blocked])]
    (let [api-symbol (symbol "xia.memory" (name api-name))]
      (testing (str api-symbol)
        (is (some? (thrown-by
                    #(sci-env/eval-string (str "(" api-symbol ")")))))))))
