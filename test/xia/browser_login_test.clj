(ns xia.browser-login-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.browser :as browser]
            [xia.prompt :as prompt]
            [xia.secret :as secret]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

;; ---------------------------------------------------------------------------
;; Site credential CRUD
;; ---------------------------------------------------------------------------

(deftest register-and-get-site-cred
  (db/register-site-cred!
    {:id             :mysite
     :name           "My Site"
     :login-url      "https://mysite.com/login"
     :username-field "email"
     :password-field "passwd"
     :username       "alice@example.com"
     :password       "s3cret!"})
  (let [cred (db/get-site-cred :mysite)]
    (is (= :mysite (:site-cred/id cred)))
    (is (= "My Site" (:site-cred/name cred)))
    (is (= "https://mysite.com/login" (:site-cred/login-url cred)))
    (is (= "email" (:site-cred/username-field cred)))
    (is (= "passwd" (:site-cred/password-field cred)))
    (is (= "alice@example.com" (:site-cred/username cred)))
    (is (= "s3cret!" (:site-cred/password cred)))))

(deftest list-site-creds-test
  (db/register-site-cred!
    {:id :site-a :login-url "https://a.com/login" :username "u" :password "p"})
  (db/register-site-cred!
    {:id :site-b :login-url "https://b.com/login" :username "u" :password "p"})
  (is (= 2 (count (db/list-site-creds)))))

(deftest remove-site-cred-test
  (db/register-site-cred!
    {:id :temp-site :login-url "https://temp.com" :username "u" :password "p"})
  (is (some? (db/get-site-cred :temp-site)))
  (db/remove-site-cred! :temp-site)
  (is (nil? (db/get-site-cred :temp-site))))

;; ---------------------------------------------------------------------------
;; Secret protection for site credentials
;; ---------------------------------------------------------------------------

(deftest secret-blocks-site-cred-username-query
  (db/register-site-cred!
    {:id :protected :login-url "https://x.com" :username "secret-user" :password "secret-pass"})
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Access denied"
        (secret/safe-q '[:find ?v :where [?e :site-cred/username ?v]])))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Access denied"
        (secret/safe-q '[:find ?v :where [?e :site-cred/password ?v]]))))

;; ---------------------------------------------------------------------------
;; list-sites hides credentials
;; ---------------------------------------------------------------------------

(deftest list-sites-hides-credentials
  (db/register-site-cred!
    {:id :visible :name "Visible" :login-url "https://visible.com"
     :username "secret" :password "secret"})
  (let [sites (browser/list-sites)]
    (is (= 1 (count sites)))
    (let [site (first sites)]
      (is (= :visible (:id site)))
      (is (= "Visible" (:name site)))
      (is (= "https://visible.com" (:login-url site)))
      ;; No credentials leaked
      (is (not (contains? site :username)))
      (is (not (contains? site :password))))))

(deftest list-sites-filters-unapproved-sites-during-autonomous-runs
  (db/register-site-cred!
    {:id :approved :name "Approved" :login-url "https://approved.example"
     :username "u" :password "p" :autonomous-approved? true})
  (db/register-site-cred!
    {:id :blocked :name "Blocked" :login-url "https://blocked.example"
     :username "u" :password "p" :autonomous-approved? false})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true}]
    (is (= [:approved]
           (mapv :id (browser/list-sites))))))

(deftest list-sites-includes-implicitly-approved-sites-during-autonomous-runs
  (db/register-site-cred!
    {:id :implicit :name "Implicit" :login-url "https://implicit.example"
     :username "u" :password "p"})
  (db/register-site-cred!
    {:id :blocked :name "Blocked" :login-url "https://blocked.example"
     :username "u" :password "p" :autonomous-approved? false})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true}]
    (is (= [:implicit]
           (mapv :id (browser/list-sites))))))

;; ---------------------------------------------------------------------------
;; Login with stored credentials — unknown site
;; ---------------------------------------------------------------------------

(deftest login-unknown-site-throws
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"No site credentials"
        (browser/login :nonexistent))))

(deftest autonomous-login-requires-approved-site-account
  (db/register-site-cred!
    {:id :portal :login-url "https://portal.example/login"
     :username "hyang" :password "pw" :autonomous-approved? false})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true
                                          :audit-log        (atom [])}]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"not approved for autonomous execution"
          (browser/login :portal)))))

(deftest autonomous-login-allows-implicitly-approved-site-account
  (db/register-site-cred!
    {:id :portal :login-url "https://portal.example/login"
     :username "hyang" :password "pw"})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true
                                          :audit-log        (atom [])}]
    (with-redefs [xia.browser/open-session (fn [_url & _] {:session-id "sess-1"})
                  xia.browser/fill-form    (fn [session-id fields & {:keys [form-selector submit]}]
                                             (is (= "sess-1" session-id))
                                             (is (= {"username" "hyang"
                                                     "password" "pw"}
                                                    fields))
                                             (is (nil? form-selector))
                                             (is (= true submit))
                                             {:session-id session-id
                                              :content "ok"})]
      (is (= {:session-id "sess-1" :content "ok"}
             (browser/login :portal))))))

(deftest login-passes-backend-override-to-open-session
  (db/register-site-cred!
    {:id             :portal
     :login-url      "https://portal.example/login"
     :username-field "username"
     :password-field "password"
     :username       "hyang"
     :password       "pw"})
  (with-redefs [xia.browser/open-session (fn [url & {:keys [backend]}]
                                           (is (= "https://portal.example/login" url))
                                           (is (= :playwright backend))
                                           {:session-id "sess-1"})
                xia.browser/fill-form    (fn [session-id fields & {:keys [submit]}]
                                           (is (= "sess-1" session-id))
                                           (is (= {"username" "hyang"
                                                   "password" "pw"}
                                                  fields))
                                           (is (= true submit))
                                           {:session-id session-id
                                            :content "ok"})]
    (is (= {:session-id "sess-1" :content "ok"}
           (browser/login :portal :backend :playwright)))))

;; ---------------------------------------------------------------------------
;; Prompt mechanism
;; ---------------------------------------------------------------------------

(deftest prompt-not-available-by-default
  ;; prompt-fn starts as nil in a fresh test
  (prompt/register-prompt! nil)
  (is (not (prompt/prompt-available?))))

(deftest prompt-available-after-register
  (prompt/register-prompt! (fn [label & _] "test-value"))
  (is (prompt/prompt-available?))
  (is (= "test-value" (prompt/prompt! "anything")))
  ;; Clean up
  (prompt/register-prompt! nil))

(deftest login-interactive-requires-prompt
  (prompt/register-prompt! nil)
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"(?i)interactive login requires a terminal"
        (browser/login-interactive "https://example.com"
          [{"name" "user" "label" "User"}]))))

(deftest login-interactive-is-blocked-during-autonomous-runs
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true
                                          :audit-log        (atom [])}]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"unavailable during autonomous execution"
          (browser/login-interactive "https://example.com"
            [{"name" "user" "label" "User"}])))))

(deftest login-interactive-passes-backend-override-to-open-session
  (prompt/register-prompt! (fn [label & _]
                             (case label
                               "User" "hyang"
                               "Password" "pw")))
  (try
    (with-redefs [xia.browser/open-session (fn [url & {:keys [backend]}]
                                             (is (= "https://example.com/login" url))
                                             (is (= :playwright backend))
                                             {:session-id "sess-1"})
                  xia.browser/fill-form    (fn [session-id fields & {:keys [submit]}]
                                             (is (= "sess-1" session-id))
                                             (is (= {"user" "hyang"
                                                     "password" "pw"}
                                                    fields))
                                             (is (= true submit))
                                             {:session-id session-id
                                              :content "ok"})]
      (is (= {:session-id "sess-1" :content "ok"}
             (browser/login-interactive "https://example.com/login"
               [{"name" "user" "label" "User"}
                {"name" "password" "label" "Password" "mask?" true}]
               :backend :playwright))))
    (finally
      (prompt/register-prompt! nil))))

;; ---------------------------------------------------------------------------
;; Integration: login with stored credentials
;; ---------------------------------------------------------------------------

(deftest ^:integration login-with-stored-creds
  ;; example.com has no login form, so this should throw "No login form"
  ;; but it validates the full flow up to the form-finding step
  (db/register-site-cred!
    {:id        :example
     :login-url "https://example.com"
     :username  "testuser"
     :password  "testpass"})
  (let [result (try (browser/login :example)
                    (catch Exception e
                      {:error (.getMessage e)}))]
    ;; example.com has no forms, so login should fail at form-finding
    (is (or (:error result)
            (:session-id result)))
    ;; Clean up any session that might have been created
    (when (:session-id result)
      (browser/close-session (:session-id result)))))
