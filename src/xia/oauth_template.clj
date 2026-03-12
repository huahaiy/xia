(ns xia.oauth-template
  "Resource-backed OAuth provider templates for the admin UI."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(def ^:private template-resource "oauth_providers.edn")

(defonce ^:private templates
  (delay
    (with-open [reader (-> template-resource io/resource slurp java.io.StringReader. java.io.PushbackReader.)]
      (vec (edn/read reader)))))

(defn list-templates
  []
  @templates)

(defn get-template
  [template-id]
  (first (filter #(= template-id (:id %)) @templates)))
