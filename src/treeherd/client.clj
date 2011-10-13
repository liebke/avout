(ns treeherd.client
  (:import (org.apache.zookeeper ZooKeeper
                                 CreateMode
                                 Watcher
                                 ZooDefs$Ids
                                 AsyncCallback$StringCallback
                                 AsyncCallback$VoidCallback
                                 AsyncCallback$StatCallback
                                 AsyncCallback$Children2Callback
                                 Watcher$Event$KeeperState
                                 Watcher$Event$EventType)
           (org.apache.zookeeper.data Stat)))


(def acls {:open_acl_unsafe ZooDefs$Ids/OPEN_ACL_UNSAFE ;; This is a completely open ACL
          :anyone_id_unsafe ZooDefs$Ids/ANYONE_ID_UNSAFE ;; This Id represents anyone
          :auth_ids ZooDefs$Ids/AUTH_IDS ;; This Id is only usable to set ACLs
          :creator_all_acl ZooDefs$Ids/CREATOR_ALL_ACL ;; This ACL gives the creators authentication id's all permissions
          :read_all_acl ZooDefs$Ids/READ_ACL_UNSAFE ;; This ACL gives the world the ability to read
          })

(def create-modes { ;; The znode will not be automatically deleted upon client's disconnect
                   {:persistent? true, :sequential? false} CreateMode/PERSISTENT
                   ;; The znode will be deleted upon the client's disconnect, and its name will be appended with a monotonically increasing number
                   {:persistent? false, :sequential? true} CreateMode/EPHEMERAL_SEQUENTIAL
                   ;; The znode will be deleted upon the client's disconnect
                   {:persistent? false, :sequential? false} CreateMode/EPHEMERAL
                   ;; The znode will not be automatically deleted upon client's disconnect, and its name will be appended with a monotonically increasing number
                   {:persistent? true, :sequential? true} CreateMode/PERSISTENT_SEQUENTIAL})

(defn stat-to-map
  ([stat]
     ;;(long czxid, long mzxid, long ctime, long mtime, int version, int cversion, int aversion, long ephemeralOwner, int dataLength, int numChildren, long pzxid)
     (when stat
       {:czxid (.getCzxid stat)
        :mzxid (.getMzxid stat)
        :ctime (.getCtime stat)
        :mtime (.getMtime stat)
        :version (.getVersion stat)
        :cversion (.getCversion stat)
        :aversion (.getAversion stat)
        :ephemeralOwner (.getEphemeralOwner stat)
        :dataLength (.getDataLength stat)
        :numChildren (.getNumChildren stat)
        :pzxid (.getPzxid stat)})))

(defn event-to-map
  ([event]
     (when event
       {:event-type (keyword (.name (.getType event)))
        :keeper-state (keyword (.name (.getState event)))
        :path (.getPath event)})))

(def event-types
  (into #{} (map #(keyword (.name %)) (Watcher$Event$EventType/values))))

(def keeper-states
  (into #{} (map #(keyword (.name %)) (Watcher$Event$KeeperState/values))))

(defn make-watcher
  ([handler]
     (reify Watcher
       (process [this event]
         (handler (event-to-map event))))))

(defn string-callback
  ([handler]
     (reify AsyncCallback$StringCallback
       (processResult [this return-code path context name]
         (handler {:return-code return-code
                   :path path
                   :context context
                   :name name})))))

(defn children-callback
  ([handler]
     (reify AsyncCallback$Children2Callback
       (processResult [this return-code path context children stat]
         (handler {:return-code return-code
                   :path path
                   :context context
                   :children (seq children)
                   :stat (stat-to-map stat)})))))

(defn void-callback
  ([handler]
     (reify AsyncCallback$VoidCallback
       (processResult [this return-code path context]
         (handler {:return-code return-code
                   :path path
                   :context context})))))

(defn promise-callback
  ([prom callback-fn]
     (fn [{:keys [return-code path context name] :as result}]
       (deliver prom result)
       (when callback-fn
         (callback-fn result)))))

;; Public DSL

(defn client
  ([connection-string timeout-msec watcher-fn]
     (ZooKeeper. connection-string timeout-msec (watcher watcher-fn))))

(defn exists
  ([client path]
     (exists client path false))
  ([client path watcher-fn]
     (stat-to-map (.exists client path (watcher watcher-fn)))))

(defn create
  " Creates a node, returning either the node's name, or a promise with a result map if the done asynchronously. If an error occurs, create will return false.

  Options:

    :persistent? indicates if the node should be persistent
    :sequential? indicates if the node should be sequential
    :data data to associate with the node
    :acl access control keyword, see the acls map
    :async? indicates that the create should occur asynchronously, a promise will be returned
    :callback indicates that the create should occur asynchronously and that this function should be called when it does, a promise will also be returned


  Example:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" 120 #(println \"event received: \" %)))

    (defn callback [result]
      (println \"got callback result: \" result))

    ;; first delete the baz node if it exists
    (delete-all treeherd \"/baz\")
    ;; now create a persistent parent node, /baz, and two child nodes
    (def p0 (create treeherd \"/baz\" :callback callback :persistent? true))
    @p0
    (def p1 (create treeherd \"/baz/1\" :callback callback))
    @p1
    (def p2 (create treeherd \"/baz/2-\" :async? true :sequential? true))
    @p2
    (create treeherd \"/baz/3\")

"
  ([client path & {:keys [data acl persistent? sequential? context callback async?]
                   :or {persistent? false
                        sequential? false
                        acl :open_acl_unsafe
                        context path
                        async? false}}]
     (if (or async? callback)
       (let [prom (promise)]
         (try
           (.create client path data (acls acl)
                    (create-modes {:persistent? persistent?, :sequential? sequential?})
                    (string-callback (promise-callback prom callback))
                    context)
           (catch Exception _ (deliver prom false)))
         prom)
       (try
         (.create client path data (acls acl)
                  (create-modes {:persistent? persistent?, :sequential? sequential?}))
         (catch Exception _ false)))))

(defn delete
  "Deletes the given node, if it exists

  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" 120 #(println \"event received: \" %)))

    (defn callback [result]
      (println \"got callback result: \" result))

    (create treeherd \"/foo\" :persistent? true)
    (create treeherd \"/bar\" :persistent? true)

    (delete treeherd \"/foo\")
    (def p0 (delete treeherd \"/bar\" :callback callback))
    @p0
"
  ([client path & {:keys [version async? callback context]
                   :or {version -1
                        async? false
                        context path}}]
     (if (or async? callback)
       (let [prom (promise)]
         (try
           (.delete client path version (void-callback (promise-callback prom callback)) context)
           (catch Exception _ (deliver prom false)))
         prom)
       (try
         (do
           (.delete client path version)
           true)
         (catch Exception _ false)))))

(defn children
  "
  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" 120 #(println \"event received: \" %)))

    (defn callback [result]
      (println \"got callback result: \" result))

    (delete-all treeherd \"/foo\")
    (create treeherd \"/foo\" :persistent? true)
    (repeatedly 5 #(create treeherd \"/foo/child-\" :sequential? true))

    (children treeherd \"/foo\")
    (def p0 (children treeherd \"/foo\" :async? true))
    @p0
    (def p1 (children treeherd \"/foo\" :callback callback))
    @p1
    (def p2 (children treeherd \"/foo\" :async? true :watch? true))
    @p2
    (def p3 (children treeherd \"/foo\" :async? true :watcher #(println \"watched event: \" %)))
    @p3

"
  ([client path & {:keys [watcher watch? async? callback context]
                   :or {watch? false
                        async? false
                        context path}}]
     (if (or async? callback)
       (let [prom (promise)]
         (try
           (if watcher
             (seq (.getChildren client path (make-watcher watcher)
                                (children-callback (promise-callback prom callback)) context))
             (seq (.getChildren client path watch?
                                (children-callback (promise-callback prom callback)) context)))
           (catch Exception _ (deliver prom false)))
         prom)
       (try
         (if watcher
           (seq (.getChildren client path (make-watcher watcher)))
           (seq (.getChildren client path watch?)))
         (catch Exception _ false)))))

(defn delete-all
  ([client path]
     (doseq [child (or (children client path) nil)]
       (delete-all client (str path "/" child)))
     (delete client path)))

