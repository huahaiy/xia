(ns xia.channel.http.common
  "Dependency-backed helpers shared by HTTP handler namespaces.")

(defn json-response
  [deps status body]
  ((:json-response deps) status body))

(defn exception-response
  [deps throwable]
  ((:exception-response deps) throwable))

(defn instant->str
  [deps value]
  ((:instant->str deps) value))

(defn date->millis
  [deps value]
  ((:date->millis deps) value))

(defn read-body
  [deps req]
  ((:read-body deps) req))

(defn read-body-bytes
  [deps body]
  ((:read-body-bytes deps) body))

(defn request-header
  [deps req header-name]
  ((:request-header deps) req header-name))

(defn request-base-url
  [deps req]
  ((:request-base-url deps) req))

(defn multipart-form-request?
  [deps req]
  ((:multipart-form-request? deps) req))

(defn download-response
  [deps name media-type bytes]
  ((:download-response deps) name media-type bytes))

(defn parse-session-id
  [deps session-id]
  ((:parse-session-id deps) session-id))

(defn session-exists?
  [deps session-id]
  ((:session-exists? deps) session-id))

(defn touch-rest-session!
  [deps session-id]
  ((:touch-rest-session! deps) session-id))

(defn nonblank-str
  [deps value]
  ((:nonblank-str deps) value))

(defn parse-keyword-id
  [deps value field-name]
  ((:parse-keyword-id deps) value field-name))

(defn parse-optional-positive-long
  [deps value field-name]
  ((:parse-optional-positive-long deps) value field-name))

(defn parse-query-string
  [deps query-string]
  ((:parse-query-string deps) query-string))

(defn truncate-text
  [deps value limit]
  ((:truncate-text deps) value limit))

(defn throwable-message
  [deps throwable]
  ((:throwable-message deps) throwable))

(defn with-existing-session
  [deps session-id f]
  (cond
    (nil? (parse-session-id deps session-id))
    (json-response deps 400 {:error "invalid session id"})

    (not (session-exists? deps session-id))
    (json-response deps 404 {:error "session not found"})

    :else
    (f)))
