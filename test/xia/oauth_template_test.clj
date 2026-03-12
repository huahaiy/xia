(ns xia.oauth-template-test
  (:require [clojure.test :refer :all]
            [xia.oauth-template :as oauth-template]))

(deftest bundled-oauth-templates-include-common-providers
  (let [templates (oauth-template/list-templates)
        ids       (set (map :id templates))
        github    (oauth-template/get-template :github)]
    (is (contains? ids :github))
    (is (contains? ids :google))
    (is (contains? ids :microsoft))
    (is (= "https://api.github.com" (:api-base-url github)))
    (is (= "github" (:service-id github)))))
