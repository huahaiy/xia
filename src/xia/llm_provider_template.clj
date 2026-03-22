(ns xia.llm-provider-template
  "Resource-backed LLM provider templates for onboarding and admin UI."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(def ^:private template-resource "llm_provider_templates.edn")

(defonce ^:private templates
  (delay
    (with-open [reader (-> template-resource
                           io/resource
                           slurp
                           java.io.StringReader.
                           java.io.PushbackReader.)]
      (vec (edn/read reader)))))

(defn list-templates
  []
  @templates)

(defn get-template
  [template-id]
  (first (filter #(= template-id (:id %)) @templates)))
