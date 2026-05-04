(ns xia.tool-smoke-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.artifact :as artifact]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.test-helpers :refer [minimal-pdf-base64 with-test-db]]
            [xia.tool :as tool]))

(use-fixtures :each with-test-db)

(deftest safe-tool-runs-without-approval
  (db/install-tool! {:id          :safe-tool
                     :name        "safe-tool"
                     :description "Safe tool"
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :safe-tool)
  (is (= {"status" "ok"}
         (tool/execute-tool :safe-tool {} {:channel :scheduler}))))

(deftest privileged-tool-blocks-without-approval-handler
  (db/install-tool! {:id          :privileged-tool
                     :name        "privileged-tool"
                     :description "Privileged tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :privileged-tool)
  (let [result (tool/execute-tool :privileged-tool {} {:channel :scheduler})]
    (is (= "Tool privileged-tool blocked: No approval handler available for current channel"
           (:error result)))))

(deftest privileged-tool-approval-is-cached-per-session
  (db/install-tool! {:id          :session-tool
                     :name        "session-tool"
                     :description "Session tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :session-tool)
  (let [calls      (atom 0)
        session-id (random-uuid)]
    (prompt/register-approval! :terminal
                               (fn [_]
                                 (swap! calls inc)
                                 true))
    (try
      (is (= {"status" "ok"}
             (tool/execute-tool :session-tool {} {:channel :terminal
                                                  :session-id session-id})))
      (is (= {"status" "ok"}
             (tool/execute-tool :session-tool {} {:channel :terminal
                                                  :session-id session-id})))
      (is (= 1 @calls))
      (finally
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)))))

(deftest ensure-bundled-tools-installs-core-default-set
  (let [count (tool/ensure-bundled-tools!)]
    (is (pos? count))
    (is (= :branch-tasks (:tool/id (db/get-tool :branch-tasks))))
    (is (= :peer-list (:tool/id (db/get-tool :peer-list))))
    (is (= :artifact-create (:tool/id (db/get-tool :artifact-create))))
    (is (= :browser-open (:tool/id (db/get-tool :browser-open))))
    (is (= :calendar-event-create (:tool/id (db/get-tool :calendar-event-create))))
    (is (= :local-doc-search (:tool/id (db/get-tool :local-doc-search))))))

(deftest ensure-bundled-tools-refreshes-bundled-approval-policy
  (db/install-tool! {:id          :email-list
                     :name        "email-list"
                     :description "Old email list"
                     :approval    :session
                     :handler     "(fn [_] {})"})
  (tool/ensure-bundled-tools!)
  (is (= :auto (:tool/approval (db/get-tool :email-list)))))

(deftest ensure-bundled-tools-refreshes-bundled-parameters-and-handler
  (db/install-tool! {:id          :email-read
                     :name        "email-read"
                     :description "Old email read"
                     :approval    :auto
                     :parameters  {"type" "object"
                                   "properties" {}}
                     :handler     "(fn [_] {})"})
  (tool/ensure-bundled-tools!)
  (is (contains? (get-in (db/get-tool :email-read)
                         [:tool/parameters "properties"])
                 "save_attachments"))
  (is (re-find #"save-attachments\?"
               (:tool/handler (db/get-tool :email-read)))))

(deftest artifact-create-tool-supports-binary-pdf-artifacts
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :artifact-create)
  (let [session-id (db/create-session! :terminal)
        result     (tool/execute-tool :artifact-create
                                      {"name" "found.pdf"
                                       "kind" "pdf"
                                       "media_type" "application/pdf"
                                       "bytes_base64" (minimal-pdf-base64 "artifact pdf")}
                                      {:channel :terminal
                                       :session-id session-id})
        download   (artifact/visible-artifact-download-data (:id result))
        read-back  (artifact/read-visible-artifact (:id result))]
    (is (= :pdf (:kind result)))
    (is (= "found.pdf" (:name result)))
    (is (= "application/pdf" (:media-type result)))
    (is (= "found.pdf" (:name download)))
    (is (= "application/pdf" (:media-type download)))
    (is (str/starts-with? (String. ^bytes (:bytes download)) "%PDF"))
    (is (true? (:text-available? read-back)))
    (is (str/includes? (:text read-back) "artifact pdf"))))
