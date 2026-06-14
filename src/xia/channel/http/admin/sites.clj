(ns xia.channel.http.admin.sites
  "Site credential admin HTTP handlers."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.autonomous.access :as autonomous-access]
            [xia.channel.http.admin.common :as common]
            [xia.db :as db]
            [xia.runtime-overlay :as runtime-overlay])
  (:import [java.net URI]))

(defn- infer-site-id
  [data]
  (let [name-text  (common/nonblank-str (get data "name"))
        login-url  (common/nonblank-str (get data "login_url"))
        url-base   (when login-url
                     (try
                       (let [uri  (URI. login-url)
                             host (some-> (.getHost uri)
                                          (str/replace #"^www\." ""))
                             path (some-> (.getPath uri) common/nonblank-str)]
                         (common/normalize-id-segment
                          (str/join "-" (filter some? [host path]))))
                       (catch Exception _
                         (common/normalize-id-segment login-url))))
        base       (or (common/normalize-id-segment name-text)
                       url-base
                       "site")
        used-ids   (map :site-cred/id (db/list-site-creds))]
    (common/next-available-id base used-ids)))

(defn site->admin-body
  [site]
  {:id                  (some-> (:site-cred/id site) name)
   :runtime_source      (when-let [site-id (:site-cred/id site)]
                          (name (runtime-overlay/entity-source :site-cred site-id)))
   :name                (:site-cred/name site)
   :login_url           (:site-cred/login-url site)
   :username_field      (:site-cred/username-field site)
   :password_field      (:site-cred/password-field site)
   :form_selector       (:site-cred/form-selector site)
   :extra_fields        (:site-cred/extra-fields site)
   :autonomous_approved (boolean (autonomous-access/site-autonomous-approved? site))
   :username_configured (boolean (common/nonblank-str (:site-cred/username site)))
   :password_configured (boolean (common/nonblank-str (:site-cred/password site)))})

(defn- parse-extra-fields
  [value]
  (let [text (common/nonblank-str value)]
    (when text
      (try
        (json/write-json-str (json/read-json text))
        (catch Exception _
          (throw (ex-info "extra_fields must be valid JSON"
                          {:field "extra_fields"})))))))

(defn handle-save-site
  [deps req]
  (try
    (let [data                 (or (common/read-body deps req) {})
          site-id              (if-let [id-text (common/nonblank-str (get data "id"))]
                                 (common/parse-keyword-id id-text "id")
                                 (infer-site-id data))
          existing             (db/get-site-cred site-id)
          login-url            (common/nonblank-str (get data "login_url"))
          name                 (or (common/nonblank-str (get data "name"))
                                   (name site-id))
          username-field       (or (common/nonblank-str (get data "username_field"))
                                   "username")
          password-field       (or (common/nonblank-str (get data "password_field"))
                                   "password")
          username             (or (common/nonblank-str (get data "username"))
                                   (:site-cred/username existing)
                                   "")
          password             (or (common/nonblank-str (get data "password"))
                                   (:site-cred/password existing)
                                   "")
          form-selector        (common/nonblank-str (get data "form_selector"))
          extra-fields         (parse-extra-fields (get data "extra_fields"))
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))]
      (when-not login-url
        (throw (ex-info "missing 'login_url' field" {:field "login_url"})))
      (db/save-site-cred! {:id                     site-id
                           :name                   name
                           :login-url              login-url
                           :username-field         username-field
                           :password-field         password-field
                           :username               username
                           :password               password
                           :form-selector          form-selector
                           :extra-fields           extra-fields
                           :autonomous-approved?   autonomous-approved?})
      (common/json-response deps 200 {:site (site->admin-body (db/get-site-cred site-id))}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-delete-site
  [deps site-id]
  (try
    (let [site-key (common/parse-keyword-id site-id "site_id")]
      (if (db/get-site-cred site-key)
        (do
          (db/remove-site-cred! site-key)
          (common/json-response deps 200 {:status "deleted"
                                          :site_id (name site-key)}))
        (common/json-response deps 404 {:error "site credential not found"})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))
