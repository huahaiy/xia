(ns xia.service-test
  (:require [clojure.test :refer :all]
            [xia.service :as service]))

(deftest gmail-services-use-a-higher-default-rate-limit
  (is (= service/gmail-rate-limit-per-minute
         (service/effective-rate-limit-per-minute
          {:service/base-url "https://gmail.googleapis.com"})))
  (is (= service/gmail-rate-limit-per-minute
         (service/effective-rate-limit-per-minute
          {:base-url "https://gmail.googleapis.com/"})))
  (is (= 42
         (service/effective-rate-limit-per-minute
          {:service/base-url "https://gmail.googleapis.com"
           :service/rate-limit-per-minute 42})))
  (is (= service/default-rate-limit-per-minute
         (service/effective-rate-limit-per-minute
          {:service/base-url "https://api.github.com"}))))
