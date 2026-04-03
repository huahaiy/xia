(ns xia.db.task
  "Task, turn, and item persistence helpers."
  (:import [java.util UUID]))

(defn- q*
  [deps query & inputs]
  (apply (:q deps) query inputs))

(defn- transact!*
  [deps tx-data]
  ((:transact! deps) tx-data))

(defn- raw-entity*
  [deps eid]
  ((:raw-entity deps) eid))

(defn- decrypt-entity*
  [deps entity-map]
  ((:decrypt-entity deps) entity-map))

(defn- entity-created-at*
  [deps entity-map]
  ((:entity-created-at deps) entity-map))

(defn- entity-updated-at*
  [deps entity-map]
  ((:entity-updated-at deps) entity-map))

(defn- now
  []
  (java.util.Date.))

(defn- empty->nil
  [value]
  (when-not (= "" value) value))

(defn- session-eid
  [deps session-id]
  (when session-id
    (ffirst (q* deps '[:find ?e :in $ ?sid
                       :where [?e :session/id ?sid]]
                session-id))))

(defn- task-eid
  [deps task-id]
  (when task-id
    (ffirst (q* deps '[:find ?e :in $ ?tid
                       :where [?e :task/id ?tid]]
                task-id))))

(defn- turn-eid
  [deps turn-id]
  (when turn-id
    (ffirst (q* deps '[:find ?e :in $ ?tid
                       :where [?e :task.turn/id ?tid]]
                turn-id))))

(defn- task-session-id
  [deps task-eid*]
  (when task-eid*
    (ffirst (q* deps '[:find ?sid :in $ ?task
                       :where
                       [?task :task/session ?session]
                       [?session :session/id ?sid]]
                task-eid*))))

(defn- session-link-eid
  [deps task-eid* session-eid*]
  (when (and task-eid* session-eid*)
    (ffirst (q* deps '[:find ?link :in $ ?task ?session
                       :where
                       [?link :task.session-link/task ?task]
                       [?link :task.session-link/session ?session]]
                task-eid*
                session-eid*))))

(defn- task-session-links*
  [deps task-eid*]
  (when task-eid*
    (->> (q* deps '[:find ?sid ?role ?created-at ?updated-at
                    :in $ ?task
                    :where
                    [?link :task.session-link/task ?task]
                    [?link :task.session-link/session ?session]
                    [?session :session/id ?sid]
                    [?link :task.session-link/role ?role]
                    [?link :task.session-link/created-at ?created-at]
                    [?link :task.session-link/updated-at ?updated-at]]
                 task-eid*)
         (map (fn [[sid role created-at updated-at]]
                {:session-id sid
                 :role role
                 :created-at created-at
                 :updated-at updated-at}))
         (sort-by (juxt #(or (:created-at %) (:updated-at %))
                        :session-id))
         vec)))

(defn- attach-task-session!*
  [deps task-eid* session-eid* role]
  (when (and task-eid* session-eid*)
    (let [timestamp (now)]
      (if-let [link-eid (session-link-eid deps task-eid* session-eid*)]
        (transact!* deps [{:db/id link-eid
                           :task.session-link/role (or role :attached)
                           :task.session-link/updated-at timestamp}])
        (transact!* deps [{:task.session-link/task task-eid*
                           :task.session-link/session session-eid*
                           :task.session-link/role (or role :attached)
                           :task.session-link/created-at timestamp
                           :task.session-link/updated-at timestamp}])))))

(defn- turn-task-id
  [deps turn-eid*]
  (when turn-eid*
    (ffirst (q* deps '[:find ?task-id :in $ ?turn
                       :where
                       [?turn :task.turn/task ?task]
                       [?task :task/id ?task-id]]
                turn-eid*))))

(defn- count-task-turns
  [deps task-eid*]
  (long (or (ffirst (q* deps '[:find (count ?turn) :in $ ?task
                               :where [?turn :task.turn/task ?task]]
                          task-eid*))
            0)))

(defn- count-turn-items
  [deps turn-eid*]
  (long (or (ffirst (q* deps '[:find (count ?item) :in $ ?turn
                               :where [?item :task.item/turn ?turn]]
                          turn-eid*))
            0)))

(defn- touch-task!
  [deps task-eid*]
  (when task-eid*
    (transact!* deps [{:db/id task-eid*
                       :task/updated-at (now)}])))

(defn create-task!
  "Create a durable task.

   `:session-id` is the task's current execution session, not the full set of
   sessions linked to the task over time. Historical and attached sessions are
   retained separately in `:session-links`."
  [deps {:keys [id session-id session-role parent-id channel type state title summary contract
                stop-reason error meta autonomy-state current-turn-id started-at finished-at]}]
  (let [task-id      (or id (random-uuid))
        created-at   (now)
        session-eid* (when session-id
                       (or (session-eid deps session-id)
                           (throw (ex-info "Cannot create task: session not found"
                                           {:session-id session-id}))))]
    (transact!*
     deps
     [(cond-> {:task/id task-id
               :task/type (or type :interactive)
               :task/state (or state :running)
               :task/title (or title "")
               :task/created-at created-at
               :task/updated-at created-at}
        session-eid* (assoc :task/session session-eid*)
        parent-id (assoc :task/parent-id parent-id)
        channel (assoc :task/channel channel)
        (some? summary) (assoc :task/summary summary)
        (some? contract) (assoc :task/contract contract)
        stop-reason (assoc :task/stop-reason stop-reason)
        (some? error) (assoc :task/error error)
        (some? meta) (assoc :task/meta meta)
        (some? autonomy-state) (assoc :task/autonomy-state autonomy-state)
        current-turn-id (assoc :task/current-turn-id current-turn-id)
        (some? started-at) (assoc :task/started-at started-at)
        (some? finished-at) (assoc :task/finished-at finished-at))])
    (attach-task-session!* deps (task-eid deps task-id) session-eid* (or session-role :origin))
    task-id))

(defn update-task!
  "Update a durable task.

   When `:session-id` is supplied, it rebinds the task's current execution
   session. Prior sessions remain attached through task-session links."
  [deps task-id {:keys [session-id session-role parent-id channel type state title summary contract stop-reason error
                        meta autonomy-state current-turn-id started-at finished-at]
                 :as attrs}]
  (when-let [eid (task-eid deps task-id)]
    (let [entity-map    (raw-entity* deps eid)
          current-session-eid
          (let [value (:task/session entity-map)]
            (cond
              (map? value) (:db/id value)
              (number? value) value
              :else value))
          session-eid*  (when (contains? attrs :session-id)
                          (when session-id
                            (or (session-eid deps session-id)
                                (throw (ex-info "Cannot update task: session not found"
                                                {:task-id task-id
                                                 :session-id session-id})))))
          retracts   (cond-> []
                       (and (contains? attrs :session-id)
                            current-session-eid
                            (not= session-eid* current-session-eid))
                       (conj [:db/retract eid :task/session current-session-eid])
                       (and (contains? attrs :stop-reason)
                            (nil? stop-reason)
                            (contains? entity-map :task/stop-reason))
                       (conj [:db/retract eid :task/stop-reason (:task/stop-reason entity-map)])
                       (and (contains? attrs :error)
                            (nil? error)
                            (contains? entity-map :task/error))
                       (conj [:db/retract eid :task/error (:task/error entity-map)])
                       (and (contains? attrs :contract)
                            (nil? contract)
                            (contains? entity-map :task/contract))
                       (conj [:db/retract eid :task/contract (:task/contract entity-map)])
                       (and (contains? attrs :meta)
                            (nil? meta)
                            (contains? entity-map :task/meta))
                       (conj [:db/retract eid :task/meta (:task/meta entity-map)])
                       (and (contains? attrs :autonomy-state)
                            (nil? autonomy-state)
                            (contains? entity-map :task/autonomy-state))
                       (conj [:db/retract eid :task/autonomy-state (:task/autonomy-state entity-map)])
                       (and (contains? attrs :current-turn-id)
                            (nil? current-turn-id)
                            (contains? entity-map :task/current-turn-id))
                       (conj [:db/retract eid :task/current-turn-id (:task/current-turn-id entity-map)])
                       (and (contains? attrs :finished-at)
                            (nil? finished-at)
                            (contains? entity-map :task/finished-at))
                       (conj [:db/retract eid :task/finished-at (:task/finished-at entity-map)]))]
      (transact!*
       deps
       (into retracts
             [(cond-> {:db/id eid
                       :task/updated-at (now)}
                session-eid* (assoc :task/session session-eid*)
                parent-id (assoc :task/parent-id parent-id)
                channel (assoc :task/channel channel)
                type (assoc :task/type type)
                state (assoc :task/state state)
                (some? title) (assoc :task/title title)
                (some? summary) (assoc :task/summary summary)
                (some? contract) (assoc :task/contract contract)
                stop-reason (assoc :task/stop-reason stop-reason)
                (some? error) (assoc :task/error error)
                (some? meta) (assoc :task/meta meta)
                (some? autonomy-state) (assoc :task/autonomy-state autonomy-state)
                current-turn-id (assoc :task/current-turn-id current-turn-id)
                (some? started-at) (assoc :task/started-at started-at)
                (some? finished-at) (assoc :task/finished-at finished-at))]))
      (attach-task-session!* deps eid session-eid* (or session-role :attached))
    true)))

(defn get-task
  [deps task-id]
  (when-let [eid (task-eid deps task-id)]
    (let [entity-map (decrypt-entity* deps (raw-entity* deps eid))]
      {:id             (:task/id entity-map)
       :session-id     (task-session-id deps eid)
       :session-links  (task-session-links* deps eid)
       :parent-id      (:task/parent-id entity-map)
       :channel        (:task/channel entity-map)
       :type           (:task/type entity-map)
       :state          (:task/state entity-map)
       :title          (empty->nil (:task/title entity-map))
       :summary        (empty->nil (:task/summary entity-map))
       :contract       (:task/contract entity-map)
       :stop-reason    (:task/stop-reason entity-map)
       :error          (empty->nil (:task/error entity-map))
       :meta           (:task/meta entity-map)
       :autonomy-state (:task/autonomy-state entity-map)
       :current-turn-id (:task/current-turn-id entity-map)
       :created-at     (or (:task/created-at entity-map)
                           (entity-created-at* deps entity-map))
       :updated-at     (or (:task/updated-at entity-map)
                           (entity-updated-at* deps entity-map))
       :started-at     (:task/started-at entity-map)
       :finished-at    (:task/finished-at entity-map)})))

(defn list-tasks
  [deps & [{:keys [session-id limit] :or {limit 100}}]]
  (let [rows (if session-id
               (q* deps '[:find ?task-id ?updated-at
                          :in $ ?sid
                          :where
                          [?session :session/id ?sid]
                          [?link :task.session-link/session ?session]
                          [?link :task.session-link/task ?task]
                          [?task :task/id ?task-id]
                          [(get-else $ ?task :task/updated-at 0) ?updated-at]]
                   session-id)
               (q* deps '[:find ?task-id ?updated-at
                          :where
                          [?task :task/id ?task-id]
                          [(get-else $ ?task :task/updated-at 0) ?updated-at]]))]
    (->> rows
         (sort-by second #(compare %2 %1))
         (take (long limit))
         (mapv (fn [[task-id _]]
                 (get-task deps task-id))))))

(defn- session-link-role
  [task session-id]
  (some (fn [{:keys [role] :as link}]
          (when (= session-id (:session-id link))
            role))
        (:session-links task)))

(defn- current-session-task-state
  [task]
  (or (:state task)
      (get-in task [:meta :runtime :state])))

(defn- current-session-task-state-rank
  [task]
  (case (current-session-task-state task)
    :running 0
    :waiting_input 0
    :waiting_approval 0
    :paused 1
    :resumable 1
    :failed 2
    :completed 4
    :cancelled 4
    3))

(defn- current-session-task-rank
  [session-id task]
  (let [updated-at (or (:updated-at task) (:created-at task))
        updated-ms (if (instance? java.util.Date updated-at)
                     (.getTime ^java.util.Date updated-at)
                     0)
        role       (session-link-role task session-id)]
    [(if (:current-turn-id task) 0 1)
     (current-session-task-state-rank task)
     (if (= session-id (:session-id task)) 0 1)
     (case role
       :resumed 0
       :origin 1
       :attached 2
       3)
     (- updated-ms)]))

(defn current-session-task
  "Return the ambient task for a linked session.

   A task may have many linked sessions, but only one current execution session
   stored on the task itself. This helper resolves the most relevant linked task
   for a session while keeping `task.session-id` as the execution-session
   source of truth."
  [deps session-id]
  (first
   (sort-by #(current-session-task-rank session-id %)
            compare
            (list-tasks deps {:session-id session-id}))))

(defn attach-task-session!
  [deps task-id session-id & [{:keys [role primary?]
                               :or {role :attached
                                    primary? false}}]]
  (when-let [task-eid* (task-eid deps task-id)]
    (let [session-eid* (or (session-eid deps session-id)
                           (throw (ex-info "Cannot attach task session: session not found"
                                           {:task-id task-id
                                            :session-id session-id})))]
      (attach-task-session!* deps task-eid* session-eid* role)
      (when primary?
        (update-task! deps task-id {:session-id session-id
                                    :session-role role}))
      (get-task deps task-id))))

(defn task-session-links
  [deps task-id]
  (when-let [task-eid* (task-eid deps task-id)]
    (task-session-links* deps task-eid*)))

(defn start-task-turn!
  [deps task-id {:keys [id operation state input summary meta interrupting-turn-id]}]
  (let [task-eid* (or (task-eid deps task-id)
                      (throw (ex-info "Cannot start task turn: task not found"
                                      {:task-id task-id})))
        turn-id    (or id (random-uuid))
        created-at (now)
        turn-index (inc (count-task-turns deps task-eid*))]
    (transact!*
     deps
     [(cond-> {:task.turn/id turn-id
               :task.turn/task task-eid*
               :task.turn/index turn-index
               :task.turn/operation (or operation :start)
               :task.turn/state (or state :running)
               :task.turn/input (or input "")
               :task.turn/summary (or summary "")
               :task.turn/created-at created-at
               :task.turn/updated-at created-at
               :task.turn/started-at created-at}
        (some? meta) (assoc :task.turn/meta meta)
        interrupting-turn-id (assoc :task.turn/interrupting-turn-id interrupting-turn-id))
      {:db/id task-eid*
       :task/state :running
       :task/current-turn-id turn-id
       :task/updated-at created-at
       :task/started-at (or (:task/started-at (raw-entity* deps task-eid*))
                            created-at)}])
    turn-id))

(defn- turn-entity->body
  [deps task-id entity-map]
  {:id                   (:task.turn/id entity-map)
   :task-id              task-id
   :index                (:task.turn/index entity-map)
   :operation            (:task.turn/operation entity-map)
   :state                (:task.turn/state entity-map)
   :input                (empty->nil (:task.turn/input entity-map))
   :summary              (empty->nil (:task.turn/summary entity-map))
   :error                (empty->nil (:task.turn/error entity-map))
   :meta                 (:task.turn/meta entity-map)
   :interrupting-turn-id (:task.turn/interrupting-turn-id entity-map)
   :created-at           (or (:task.turn/created-at entity-map)
                             (entity-created-at* deps entity-map))
   :updated-at           (or (:task.turn/updated-at entity-map)
                             (entity-updated-at* deps entity-map))
   :started-at           (:task.turn/started-at entity-map)
   :finished-at          (:task.turn/finished-at entity-map)})

(defn update-task-turn!
  [deps turn-id {:keys [state input summary error meta finished-at]}]
  (when-let [eid (turn-eid deps turn-id)]
    (let [task-id    (turn-task-id deps eid)
          task-eid*  (task-eid deps task-id)
          task-entity (raw-entity* deps task-eid*)
          finished-at* (or finished-at
                           (when (#{:completed :failed :cancelled} state)
                             (now)))
          clear-current-turn? (and (#{:completed :failed :cancelled} state)
                                   (= turn-id (:task/current-turn-id task-entity)))]
      (transact!*
       deps
       (cond-> [(cond-> {:db/id eid
                         :task.turn/updated-at (now)}
                  state (assoc :task.turn/state state)
                  (some? input) (assoc :task.turn/input input)
                  (some? summary) (assoc :task.turn/summary summary)
                  (some? error) (assoc :task.turn/error error)
                  (some? meta) (assoc :task.turn/meta meta)
                  (some? finished-at*) (assoc :task.turn/finished-at finished-at*))]
         clear-current-turn?
         (conj [:db/retract task-eid* :task/current-turn-id turn-id])))
      (touch-task! deps task-eid*)
      true)))

(defn task-turns
  [deps task-id]
  (if-let [task-eid* (task-eid deps task-id)]
    (->> (q* deps '[:find ?turn-id ?index ?created-at
                    :in $ ?task
                    :where
                    [?turn :task.turn/task ?task]
                    [?turn :task.turn/id ?turn-id]
                    [?turn :task.turn/index ?index]
                    [(get-else $ ?turn :task.turn/created-at 0) ?created-at]]
              task-eid*)
         (sort-by second)
         (mapv (fn [[turn-id _ _]]
                 (let [eid        (turn-eid deps turn-id)
                       entity-map (decrypt-entity* deps (raw-entity* deps eid))]
                   (turn-entity->body deps task-id entity-map)))))
    []))

(defn get-task-turn
  [deps turn-id]
  (when-let [eid (turn-eid deps turn-id)]
    (let [task-id    (turn-task-id deps eid)
          entity-map (decrypt-entity* deps (raw-entity* deps eid))]
      {:id                   (:task.turn/id entity-map)
       :task-id              task-id
       :index                (:task.turn/index entity-map)
       :operation            (:task.turn/operation entity-map)
       :state                (:task.turn/state entity-map)
       :input                (empty->nil (:task.turn/input entity-map))
       :summary              (empty->nil (:task.turn/summary entity-map))
       :error                (empty->nil (:task.turn/error entity-map))
       :meta                 (:task.turn/meta entity-map)
       :interrupting-turn-id (:task.turn/interrupting-turn-id entity-map)
       :created-at           (or (:task.turn/created-at entity-map)
                                 (entity-created-at* deps entity-map))
       :updated-at           (or (:task.turn/updated-at entity-map)
                                 (entity-updated-at* deps entity-map))
       :started-at           (:task.turn/started-at entity-map)
       :finished-at          (:task.turn/finished-at entity-map)})))

(defn add-task-item!
  [deps turn-id {:keys [id type status role summary data message-id llm-call-id
                        tool-id tool-call-id]}]
  (let [turn-eid* (or (turn-eid deps turn-id)
                      (throw (ex-info "Cannot add task item: turn not found"
                                      {:turn-id turn-id})))
        item-id   (or id (random-uuid))
        created-at (now)
        item-index (inc (count-turn-items deps turn-eid*))
        task-id   (turn-task-id deps turn-eid*)
        task-eid* (task-eid deps task-id)]
    (transact!*
     deps
     [(cond-> {:task.item/id item-id
               :task.item/turn turn-eid*
               :task.item/index item-index
               :task.item/type type
               :task.item/created-at created-at}
        status (assoc :task.item/status status)
        role (assoc :task.item/role role)
        (some? summary) (assoc :task.item/summary summary)
        (some? data) (assoc :task.item/data data)
        message-id (assoc :task.item/message-id message-id)
        llm-call-id (assoc :task.item/llm-call-id llm-call-id)
        tool-id (assoc :task.item/tool-id tool-id)
        tool-call-id (assoc :task.item/tool-call-id tool-call-id))])
    (touch-task! deps task-eid*)
    item-id))

(defn- item-entity->body
  [deps turn-id entity-map]
  {:id           (:task.item/id entity-map)
   :turn-id      turn-id
   :index        (:task.item/index entity-map)
   :type         (:task.item/type entity-map)
   :status       (:task.item/status entity-map)
   :role         (:task.item/role entity-map)
   :summary      (empty->nil (:task.item/summary entity-map))
   :data         (:task.item/data entity-map)
   :message-id   (:task.item/message-id entity-map)
   :llm-call-id  (:task.item/llm-call-id entity-map)
   :tool-id      (empty->nil (:task.item/tool-id entity-map))
   :tool-call-id (empty->nil (:task.item/tool-call-id entity-map))
   :created-at   (or (:task.item/created-at entity-map)
                     (entity-created-at* deps entity-map))})

(defn turn-items
  [deps turn-id]
  (if-let [turn-eid* (turn-eid deps turn-id)]
    (->> (q* deps '[:find ?item-id ?index
                    :in $ ?turn
                    :where
                    [?item :task.item/turn ?turn]
                    [?item :task.item/id ?item-id]
                    [?item :task.item/index ?index]]
              turn-eid*)
         (sort-by second)
         (mapv (fn [[item-id _]]
                 (let [eid        (ffirst (q* deps '[:find ?e :in $ ?iid
                                                     :where [?e :task.item/id ?iid]]
                                               item-id))
                       entity-map (decrypt-entity* deps (raw-entity* deps eid))]
                   (item-entity->body deps turn-id entity-map)))))
    []))

(defn task-history-data
  "Batch-load task turns and task items for the supplied task ids.

   Returns a map of task-id -> {:turns [...], :items [...]} suitable for
   compact history inspection and rendering."
  [deps task-ids]
  (let [task-ids* (vec (remove nil? task-ids))]
    (if (empty? task-ids*)
      {}
      (let [turn-rows (->> (q* deps '[:find ?task-id ?turn ?index
                                      :in $ [?task-id ...]
                                      :where
                                      [?task :task/id ?task-id]
                                      [?turn :task.turn/task ?task]
                                      [?turn :task.turn/index ?index]]
                                task-ids*)
                           (sort-by (juxt first #(nth % 2))))
            turns-by-task
            (reduce (fn [acc [task-id turn-eid _]]
                      (let [entity-map (decrypt-entity* deps (raw-entity* deps turn-eid))
                            turn       (turn-entity->body deps task-id entity-map)]
                        (update acc task-id (fnil conj []) turn)))
                    {}
                    turn-rows)
            turn-ids*  (mapv :id (mapcat val turns-by-task))
            item-rows  (if (empty? turn-ids*)
                         []
                         (->> (q* deps '[:find ?turn-id ?item ?index
                                         :in $ [?turn-id ...]
                                         :where
                                         [?turn :task.turn/id ?turn-id]
                                         [?item :task.item/turn ?turn]
                                         [?item :task.item/index ?index]]
                                   turn-ids*)
                              (sort-by (juxt first #(nth % 2)))))
            items-by-turn
            (reduce (fn [acc [turn-id item-eid _]]
                      (let [entity-map (decrypt-entity* deps (raw-entity* deps item-eid))
                            item       (item-entity->body deps turn-id entity-map)]
                        (update acc turn-id (fnil conj []) item)))
                    {}
                    item-rows)]
        (into {}
              (map (fn [task-id]
                     (let [turns (vec (get turns-by-task task-id []))
                           items (->> turns
                                      (mapcat #(get items-by-turn (:id %) []))
                                      (sort-by (juxt :created-at :turn-id :index) compare)
                                      vec)]
                       [task-id {:turns turns
                                 :items items}])))
              task-ids*)))))

(defn get-task-item
  [deps item-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?iid
                                    :where [?e :task.item/id ?iid]]
                             item-id))]
    (let [entity-map (decrypt-entity* deps (raw-entity* deps eid))
          turn-eid*  (:task.item/turn entity-map)
          turn-id    (when turn-eid*
                       (:task.turn/id (raw-entity* deps turn-eid*)))]
      {:id           (:task.item/id entity-map)
       :turn-id      turn-id
       :index        (:task.item/index entity-map)
       :type         (:task.item/type entity-map)
       :status       (:task.item/status entity-map)
       :role         (:task.item/role entity-map)
       :summary      (empty->nil (:task.item/summary entity-map))
       :data         (:task.item/data entity-map)
       :message-id   (:task.item/message-id entity-map)
       :llm-call-id  (:task.item/llm-call-id entity-map)
       :tool-id      (empty->nil (:task.item/tool-id entity-map))
       :tool-call-id (empty->nil (:task.item/tool-call-id entity-map))
       :created-at   (or (:task.item/created-at entity-map)
                         (entity-created-at* deps entity-map))})))
