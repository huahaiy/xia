(ns xia.browser-session-options-test
  (:require [clojure.test :refer :all]
            [xia.browser :as browser]
            [xia.browser.backend :as browser.backend]))

(deftest open-session-forwards-headless-override
  (let [captured (atom nil)
        stub-backend
        (reify browser.backend/BrowserBackend
          (backend-id [_] :playwright)
          (runtime-status* [_] {:backend :playwright})
          (bootstrap-runtime!* [_ _opts] nil)
          (install-browser-deps!* [_ _opts] nil)
          (open-session* [_ url opts]
            (reset! captured {:url url :opts opts})
            {:session-id "browser-session-1"
             :backend :playwright
             :url url
             :title "Stub"})
          (navigate* [_ _session-id _url] nil)
          (click* [_ _session-id _selector] nil)
          (fill-selector* [_ _session-id _selector _value _opts] nil)
          (fill-form* [_ _session-id _fields _opts] nil)
          (read-page* [_ _session-id] nil)
          (query-elements* [_ _session-id _opts] nil)
          (screenshot* [_ _session-id _opts] nil)
          (wait-for-page* [_ _session-id _opts] nil)
          (close-session* [_ _session-id] nil)
          (close-all-sessions!* [_] nil)
          (list-sessions* [_] []))]
    (with-redefs [xia.browser/resolve-open-backend-id (constantly :playwright)
                  xia.browser/backend-by-id (constantly stub-backend)]
      (browser/open-session "https://example.com" :headless false)
      (is (= {:url "https://example.com"
              :opts {:js true
                     :storage-state nil
                     :headless false}}
             @captured)))))
