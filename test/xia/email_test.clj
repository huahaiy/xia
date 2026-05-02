(ns xia.email-test
  (:require [clojure.test :refer :all]
            [xia.artifact :as artifact]
            [xia.db :as db]
            [xia.email :as email]
            [xia.test-helpers :refer [minimal-pdf-bytes with-test-db]]
            [xia.working-memory :as wm])
  (:import [java.util Arrays Base64]))

(use-fixtures :each with-test-db)

(defn- unsupported
  [& _]
  (throw (ex-info "unsupported test backend operation" {})))

(defn- attachment-backend
  [bytes observed-opts]
  (reify email/EmailBackend
    (backend-key [_] :test)
    (backend-label [_] "Test Mail")
    (backend-default-service-id [_] :test-mail)
    (supports-service? [_ _] true)
    (auto-detect-service-id [_] :test-mail)
    (backend-list-labels [_ _ _] (unsupported))
    (backend-list-messages [_ _ _] (unsupported))
    (backend-read-message [_ service-id message-id opts]
      (reset! observed-opts opts)
      {:service-id (name service-id)
       :id message-id
       :subject "PDF attachment"
       :attachments [(cond-> {:part-id "1"
                              :filename "found.pdf"
                              :mime-type "application/pdf"
                              :size-bytes (alength ^bytes bytes)
                              :attachment-id "att-1"
                              :inline? false}
                       (:include-attachment-data? opts)
                       (assoc :content-available? true
                              :content-included? true
                              :content-bytes-base64 (.encodeToString (Base64/getEncoder)
                                                                      ^bytes bytes)))]})
    (backend-send-message [_ _ _ _ _ _] (unsupported))
    (backend-delete-message [_ _ _ _] (unsupported))
    (backend-update-message [_ _ _ _] (unsupported))
    (backend-list-drafts [_ _ _] (unsupported))
    (backend-read-draft [_ _ _ _] (unsupported))
    (backend-save-draft [_ _ _ _ _ _] (unsupported))
    (backend-send-draft [_ _ _ _] (unsupported))
    (backend-delete-draft [_ _ _ _] (unsupported))))

(deftest read-message-can-save-binary-attachments-as-artifacts-without-inlining
  (let [session-id    (db/create-session! :terminal)
        pdf-bytes     (minimal-pdf-bytes "email attachment")
        observed-opts (atom nil)]
    (with-redefs-fn {#'email/resolve-email-target
                     (fn [_]
                       {:backend (attachment-backend pdf-bytes observed-opts)
                        :service-id :test-mail})}
      (fn []
        (binding [wm/*session-id* session-id]
          (let [message     (email/read-message "msg-1" :save-attachments? true)
                attachment  (first (:attachments message))
                artifact-id (:artifact-id attachment)
                download    (artifact/visible-artifact-download-data artifact-id)]
            (is (true? (:include-attachment-data? @observed-opts)))
            (is (= "saved" (:artifact-status attachment)))
            (is (some? artifact-id))
            (is (nil? (:content-bytes-base64 attachment)))
            (is (= [{:artifact-id artifact-id
                     :artifact-name "found.pdf"
                     :filename "found.pdf"
                     :mime-type "application/pdf"
                     :size-bytes (alength ^bytes pdf-bytes)}]
                   (:saved-attachment-artifacts message)))
            (is (= "found.pdf" (:name download)))
            (is (= "application/pdf" (:media-type download)))
            (is (Arrays/equals ^bytes pdf-bytes ^bytes (:bytes download)))))))))
