(ns xia.sci-env
  "SCI execution environment for tool handlers.
   GraalVM native-image cannot eval Clojure code, so all user-provided
   tool handlers run inside SCI (Small Clojure Interpreter).

   This module configures the SCI context with the namespaces and vars
   that tool code is allowed to access.

   SECURITY: Tool handlers run untrusted code. All DB access goes through
   xia.secret safe wrappers that block access to credentials and secrets.
   The sandbox also restricts all local file system access, ensuring tool
   code cannot interact with the host beyond the database directory."
  (:require [sci.core :as sci]
            [xia.browser :as browser]
            [xia.cron :as cron]
            [xia.memory :as memory]
            [xia.scratch :as scratch]
            [xia.schedule :as schedule]
            [xia.secret :as secret]
            [xia.service :as service]
            [xia.skill :as skill]
            [xia.web :as web]
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
  {'get-config  secret/safe-get-config
   'set-config! secret/safe-set-config!
   'q           secret/safe-q})

(def ^:private xia-scratch-ns
  {'list-pads    scratch/list-pads
   'get-pad      scratch/get-pad
   'create-pad!  scratch/create-pad!
   'save-pad!    scratch/save-pad!
   'edit-pad!    scratch/edit-pad!
   'delete-pad!  scratch/delete-pad!})

(def ^:private xia-service-ns
  {'request       service/request
   'list-services service/list-services})

(def ^:private xia-web-ns
  {'fetch-page   web/fetch-page
   'search-web   web/search-web
   'extract-data web/extract-data})

(def ^:private xia-browser-ns
  {'open-session      browser/open-session
   'navigate          browser/navigate
   'click             browser/click
   'fill-form         browser/fill-form
   'read-page         browser/read-page
   'wait-for-page     browser/wait-for-page
   'close-session     browser/close-session
   'list-sessions     browser/list-sessions
   'login             browser/login
   'login-interactive browser/login-interactive
   'list-sites        browser/list-sites})

(def ^:private xia-schedule-ns
  {'create-schedule!  schedule/create-schedule!
   'get-schedule      schedule/get-schedule
   'list-schedules    schedule/list-schedules
   'update-schedule!  schedule/update-schedule!
   'remove-schedule!  schedule/remove-schedule!
   'pause-schedule!   schedule/pause-schedule!
   'resume-schedule!  schedule/resume-schedule!
   'schedule-history  schedule/safe-schedule-history})

(def ^:private xia-cron-ns
  {'describe cron/describe})

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
                  'xia.schedule       xia-schedule-ns
                  'xia.scratch        xia-scratch-ns
                  'xia.cron           xia-cron-ns
                  'xia.service        xia-service-ns
                  'xia.web            xia-web-ns
                  'xia.browser        xia-browser-ns
                  'xia.db             xia-db-ns}
     :classes    {'java.util.Date java.util.Date
                  'java.util.UUID java.util.UUID}}))

(defonce ^:private default-ctx (delay (make-ctx)))

(defn eval-string
  "Evaluate a string of Clojure code in the SCI sandbox."
  [code-str]
  (sci/eval-string* @default-ctx code-str))
