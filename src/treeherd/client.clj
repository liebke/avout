(ns treeherd.client
  (:import (org.apache.zookeeper ZooKeeper
                                 CreateMode
                                 Watcher
                                 ZooDefs$Ids
                                 ZooDefs$Perms
                                 AsyncCallback$StringCallback
                                 AsyncCallback$VoidCallback
                                 AsyncCallback$StatCallback
                                 AsyncCallback$StatCallback
                                 AsyncCallback$Children2Callback
                                 AsyncCallback$DataCallback
                                 AsyncCallback$ACLCallback
                                 Watcher$Event$KeeperState
                                 Watcher$Event$EventType)
           (org.apache.zookeeper.data Stat
                                      Id
                                      ACL)
           (org.apache.commons.codec.digest DigestUtils)
           (org.apache.commons.codec.binary Base64))
  (:require [clojure.string :as s]))

(def ^:dynamic *perms* {:write ZooDefs$Perms/WRITE
                        :read ZooDefs$Perms/READ
                        :delete ZooDefs$Perms/DELETE
                        :create ZooDefs$Perms/CREATE
                        :admin ZooDefs$Perms/ADMIN})

(defn perm-or
  "
  Examples:

    (use 'treeherd.client)
    (perm-or *perms* :read :write :create)
"
  ([perms & perm-keys]
     (apply bit-or (vals (select-keys perms perm-keys)))))

(def acls {:open-acl-unsafe ZooDefs$Ids/OPEN_ACL_UNSAFE ;; This is a completely open ACL
          :anyone-id-unsafe ZooDefs$Ids/ANYONE_ID_UNSAFE ;; This Id represents anyone
          :auth-ids ZooDefs$Ids/AUTH_IDS ;; This Id is only usable to set ACLs
          :creator-all-acl ZooDefs$Ids/CREATOR_ALL_ACL ;; This ACL gives the creators authentication id's all permissions
          :read-all-acl ZooDefs$Ids/READ_ACL_UNSAFE ;; This ACL gives the world the ability to read
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

;; Watcher

(defn make-watcher
  ([handler]
     (reify Watcher
       (process [this event]
         (handler (event-to-map event))))))

;; Callbacks

(defn string-callback
  ([handler]
     (reify AsyncCallback$StringCallback
       (processResult [this return-code path context name]
         (handler {:return-code return-code
                   :path path
                   :context context
                   :name name})))))

(defn stat-callback
  ([handler]
     (reify AsyncCallback$StatCallback
       (processResult [this return-code path context stat]
         (handler {:return-code return-code
                   :path path
                   :context context
                   :stat (stat-to-map stat)})))))

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

(defn data-callback
  ([handler]
     (reify AsyncCallback$DataCallback
       (processResult [this return-code path context data stat]
         (handler {:return-code return-code
                   :path path
                   :context context
                   :data data
                   :stat (stat-to-map stat)})))))

(defn acl-callback
  ([handler]
     (reify AsyncCallback$ACLCallback
       (processResult [this return-code path context acl stat]
         (handler {:return-code return-code
                   :path path
                   :context context
                   :acl (seq acl)
                   :stat (stat-to-map stat)})))))

(defn promise-callback
  ([prom callback-fn]
     (fn [{:keys [return-code path context name] :as result}]
       (deliver prom result)
       (when callback-fn
         (callback-fn result)))))

;; Public DSL

(defn client
  "Returns a ZooKeeper client."
  ([connection-string & {:keys [timeout-msec watcher]
                         :or {timeout-msec 1000}}]
     (ZooKeeper. connection-string timeout-msec (when watcher (make-watcher watcher)))))

(defn exists
  "
  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" :wacher #(println \"event received: \" %)))

    (defn callback [result]
      (println \"got callback result: \" result))

    (exists treeherd \"/yadda\" :watch? true)
    (create treeherd \"/yadda\")
    (exists treeherd \"/yadda\")
    (def p0 (exists treeherd \"/yadda\" :async? true))
    @p0
    (def p1 (exists treeherd \"/yadda\" :callback callback))
    @p1
"
  ([client path & {:keys [watcher watch? async? callback context]
                   :or {watch? false
                        async? false
                        context path}}]
     (if (or async? callback)
       (let [prom (promise)]
         (.exists client path (if watcher (make-watcher watcher) watch?)
                  (stat-callback (promise-callback prom callback)) context)
         prom)
       (stat-to-map (.exists client path (if watcher (make-watcher watcher) watch?))))))

(defn create
  " Creates a node, returning either the node's name, or a promise with a result map if the done asynchronously. If an error occurs, create will return false.

  Options:

    :persistent? indicates if the node should be persistent
    :sequential? indicates if the node should be sequential
    :data data to associate with the node
    :acl access control, see the acls map
    :async? indicates that the create should occur asynchronously, a promise will be returned
    :callback indicates that the create should occur asynchronously and that this function should be called when it does, a promise will also be returned


  Example:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" :watcher #(println \"event received: \" %)))

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
                        acl (acls :open-acl-unsafe)
                        context path
                        async? false}}]
     (if (or async? callback)
       (let [prom (promise)]
         (try
           (.create client path data acl
                    (create-modes {:persistent? persistent?, :sequential? sequential?})
                    (string-callback (promise-callback prom callback))
                    context)
           (catch Exception e (do (println e) (deliver prom false))))
         prom)
       (try
         (.create client path data acl
                  (create-modes {:persistent? persistent?, :sequential? sequential?}))
         (catch Exception e (do (println e) false))))))

(defn delete
  "Deletes the given node, if it exists

  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" :watch #(println \"event received: \" %)))

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
    (def treeherd (client \"127.0.0.1:2181\" :watcher #(println \"event received: \" %)))

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
           (seq (.getChildren client path
                              (if watcher (make-watcher watcher) watch?)
                              (children-callback (promise-callback prom callback)) context))
           (catch Exception _ (deliver prom false)))
         prom)
       (try
         (seq (.getChildren client path (if watcher (make-watcher watcher) watch?)))
         (catch Exception _ false)))))

(defn delete-all
  "Deletes a node and all of its children."
  ([client path & options]
     (doseq [child (or (children client path) nil)]
       (apply delete-all client (str path "/" child) options))
     (apply delete client path options)))

(defn create-all
  "Create a node and all of its parents. The last node will be ephemeral,
   and its parents will be persistent. Option, like :persistent? :sequential?,
   :acl, will only be applied to the last child node.

  Examples:
  (delete-all treeherd \"/foo\")
  (create-all treeherd \"/foo/bar/baz\" :persistent? true)
  (create-all treeherd \"/foo/bar/baz/n-\" :sequential? true)


"
  ([client path & options]
     (loop [parent "" [child & children] (rest (s/split path #"/"))]
       (if child
         (let [node (str parent "/" child)]
           (if (exists client node)
             (recur node children)
             (recur (if (seq children)
                      (create client node :persistent? true)
                      (apply create client node options))
                    children)))
         parent))))

(defn data
  "Returns byte array of data from given node.

  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" :watcher #(println \"event received: \" %)))

    (defn callback [result]
      (println \"got callback result: \" result))

    (delete-all treeherd \"/foo\")
    (create treeherd \"/foo\" :persistent? true :data (.getBytes \"Hello World\"))
    (def result (data treeherd \"/foo\"))
    (String. (:data result))
    (:stat result)

    (def p0 (data treeherd \"/foo\" :async? true))
    @p0
    (String. (:data @p0))

    (def p1 (data treeherd \"/foo\" :watch? true :callback callback))
    @p1
    (String. (:data @p1))

    (create treeherd \"/foobar\" :persistent? true :data (.getBytes (pr-str {:a 1, :b 2, :c 3})))
    (read-string (String. (:data (data treeherd \"/foobar\"))))

"
  ([client path & {:keys [watcher watch? async? callback context]
                   :or {watch? false
                        async? false
                        context path}}]
     (let [stat (Stat.)]
       (if (or async? callback)
        (let [prom (promise)]
          (.getData client path (if watcher (make-watcher watcher) watch?)
                    (data-callback (promise-callback prom callback)) context)
          prom)
        {:data (.getData client path (if watcher (make-watcher watcher) watch?) stat)
         :stat (stat-to-map stat)}))))

(defn set-data
  "

  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" :watcher #(println \"event received: \" %)))

    (defn callback [result]
      (println \"got callback result: \" result))

    (delete-all treeherd \"/foo\")
    (create treeherd \"/foo\" :persistent? true)

    (set-data treeherd \"/foo\" (.getBytes \"Hello World\") 0)
    (String. (:data (data treeherd \"/foo\")))


    (def p0 (set-data treeherd \"/foo\" (.getBytes \"New Data\") 0 :async? true))
    @p0
    (String. (:data (data treeherd \"/foo\")))

    (def p1 (set-data treeherd \"/foo\" (.getBytes \"Even Newer Data\") 1 :callback callback))
    @p1
    (String. (:data (data treeherd \"/foo\")))

"
  ([client path data version & {:keys [async? callback context]
                                :or {async? false
                                     context path}}]
     (if (or async? callback)
       (let [prom (promise)]
         (try
           (.setData client path data version
                     (stat-callback (promise-callback prom callback)) context)
           (catch Exception e (do (println e) (deliver prom false))))
         prom)
       (try
         (.setData client path data version)
         (catch Exception e (do (println e) false))))))


;; ACL

(defn hash-password
  " Returns a base64 encoded string of a SHA-1 digest of the given password string.

  Examples:

    (hash-password \"secret\")

"
  ([password]
     (Base64/encodeBase64String (DigestUtils/sha password))))

(defn get-acl
 "
  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" :watcher #(println \"event received: \" %)))
    (add-auth-info treeherd \"digest\" \"david:secret\")

    (defn callback [result]
      (println \"got callback result: \" result))

    (delete-all treeherd \"/foo\")
    (create treeherd \"/foo\" :acl [(acl \"auth\" \"\" :read :write :create :delete)])
    (get-acl treeherd \"/foo\")

    (def p0 (get-acl treeherd \"/foo\" :async? true))

    (def p1 (get-acl treeherd \"/foo\" :callback callback))

"
  ([client path & {:keys [async? callback context]
                   :or {async? false
                        context path}}]
     (let [stat (Stat.)]
       (if (or async? callback)
         (let [prom (promise)]
           (try
             (.getACL client path stat (acl-callback (promise-callback prom callback)) context)
             (catch Exception _ (deliver prom false)))
         prom)
         {:acl (seq (.getACL client path stat))
          :stat (stat-to-map stat)}))))

(defn add-auth-info
  "Add auth info to connection."
  ([client scheme auth]
     (.addAuthInfo client scheme (if (string? auth) (.getBytes auth) auth))))

(defn acl-id
  ([scheme id-value]
     (Id. scheme id-value)))

(defn acl
  "
  Examples:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" :watcher #(println \"event received: \" %)))

    (def open-acl-unsafe (acl \"world\" \"anyone\" :read :create :delete :admin :write))
    (create treeherd \"/mynode\" :acl [open-acl-unsafe])

    (def ip-acl (acl \"ip\" \"127.0.0.1\" :read :create :delete :admin :write))
    (create treeherd \"/mynode2\" :acl [ip-acl])

    (add-auth-info treeherd \"digest\" \"david:secret\")

    ;; works
    ;; same as (acls :creator-all-acl)
    (def auth-acl (acl \"auth\" \"\" :read :create :delete :admin :write))
    (create treeherd \"/mynode4\" :acl [auth-acl])
    (data treeherd \"/mynode4\")

    ;; change auth-info
    (add-auth-info treeherd \"digest\" \"edgar:secret\")
    (data treeherd \"/mynode4\")

    ;; doesn't works
    (def digest-acl (acl  \"digest\" (str \"david:\" (hash-password \"secret\")) :read :create :delete :admin :write))
    (create treeherd \"/mynode3\" :acl [digest-acl])
    (data treeherd \"/mynode3\")

"
  ([scheme id-value perm & more-perms]
     (ACL. (apply perm-or *perms* perm more-perms) (acl-id scheme id-value))))






