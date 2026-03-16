(ns xia.browser.htmlunit
  "HtmlUnit backend adapter for xia.browser."
  (:require [xia.browser.backend :as backend]))

(defrecord HtmlUnitBackend [id ops]
  backend/BrowserBackend
  (backend-id [_]
    id)
  (runtime-status* [_]
    ((:runtime-status ops)))
  (bootstrap-runtime!* [_ opts]
    ((:bootstrap-runtime ops) opts))
  (install-browser-deps!* [_ opts]
    ((:install-browser-deps ops) opts))
  (open-session* [_ url opts]
    ((:open-session ops) url opts))
  (navigate* [_ session-id url]
    ((:navigate ops) session-id url))
  (click* [_ session-id selector]
    ((:click ops) session-id selector))
  (fill-form* [_ session-id fields opts]
    ((:fill-form ops) session-id fields opts))
  (read-page* [_ session-id]
    ((:read-page ops) session-id))
  (wait-for-page* [_ session-id opts]
    ((:wait-for-page ops) session-id opts))
  (close-session* [_ session-id]
    ((:close-session ops) session-id))
  (close-all-sessions!* [_]
    ((:close-all-sessions ops)))
  (list-sessions* [_]
    ((:list-sessions ops))))

(defn create-backend
  "Create the HtmlUnit backend from an operation map supplied by xia.browser."
  [ops]
  (->HtmlUnitBackend (:id ops) ops))
