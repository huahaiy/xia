(ns xia.sci-env
  "SCI execution environment for tool handlers.
   GraalVM native-image cannot eval Clojure code, so all user-provided
   tool handlers run inside SCI (Small Clojure Interpreter).

   This module configures the SCI context with the namespaces and vars
   that tool code is allowed to access."
  (:require [sci.core :as sci]
            [xia.db :as db]
            [xia.memory :as memory]
            [xia.skill :as skill]
            [xia.working-memory :as wm]))

(def ^:private xia-memory-ns
  {'record-episode!          memory/record-episode!
   'add-node!                memory/add-node!
   'find-node                memory/find-node
   'add-edge!                memory/add-edge!
   'add-fact!                memory/add-fact!
   'node-facts               memory/node-facts
   'node-edges               memory/node-edges
   'recent-episodes          memory/recent-episodes
   'search-nodes             memory/search-nodes
   'search-facts             memory/search-facts
   'search-episodes          memory/search-episodes
   'recall-knowledge         memory/recall-knowledge
   'node-properties          memory/node-properties
   'set-node-property!       memory/set-node-property!
   'remove-node-property!    memory/remove-node-property!
   'query-nodes-by-property  memory/query-nodes-by-property})

(def ^:private xia-wm-ns
  {'get-wm      wm/get-wm
   'wm->context wm/wm->context
   'pin!        wm/pin!
   'unpin!      wm/unpin!})

(def ^:private xia-db-ns
  {'get-config  db/get-config
   'set-config! db/set-config!
   'q           db/q})

(def ^:private xia-skill-ns
  {'search-skills       skill/search-skills
   'match-skills        skill/match-skills
   'skill-section       skill/skill-section
   'skill-headings      skill/skill-headings
   'patch-skill-section! skill/patch-skill-section!})

(defn make-ctx
  "Create a SCI evaluation context with xia APIs available."
  []
  (sci/init
    {:namespaces {'xia.memory         xia-memory-ns
                  'xia.working-memory xia-wm-ns
                  'xia.skill          xia-skill-ns
                  'xia.db             xia-db-ns}
     :classes    {'System System
                  'java.util.Date java.util.Date
                  'java.util.UUID java.util.UUID}}))

(defonce ^:private default-ctx (delay (make-ctx)))

(defn eval-string
  "Evaluate a string of Clojure code in the SCI sandbox."
  [code-str]
  (sci/eval-string* @default-ctx code-str))
