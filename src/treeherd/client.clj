(ns treeherd.client
  (:import (org.apache.zookeeper ZooKeeper
                                 CreateMode
                                 Watcher
                                 ZooDefs$Ids
                                 AsyncCallback$StringCallback
                                 AsyncCallback$StatCallback)))


(def acls {:open_acl_unsafe ZooDefs$Ids/OPEN_ACL_UNSAFE ;; This is a completely open ACL
          :anyone_id_unsafe ZooDefs$Ids/ANYONE_ID_UNSAFE ;; This Id represents anyone
          :auth_ids ZooDefs$Ids/AUTH_IDS ;; This Id is only usable to set ACLs
          :creator_all_acl ZooDefs$Ids/CREATOR_ALL_ACL ;; This ACL gives the creators authentication id's all permissions
          :read_all_acl ZooDefs$Ids/READ_ACL_UNSAFE ;; This ACL gives the world the ability to read
          })

(def create-modes {:persistent CreateMode/PERSISTENT ;; The znode will not be automatically deleted upon client's disconnect
                  :ephemeral_sequential CreateMode/EPHEMERAL_SEQUENTIAL ;; The znode will be deleted upon the client's disconnect, and its name will be appended with a monotonically increasing number
                  :ephemeral CreateMode/EPHEMERAL ;; The znode will be deleted upon the client's disconnect
                  :persistent_sequential CreateMode/PERSISTENT_SEQUENTIAL ;; The znode will not be automatically deleted upon client's disconnect, and its name will be appended with a monotonically increasing number
                  })
(defn watcher
  ([handler]
     (reify Watcher (process [this event] (handler event)))))

(defn string-callback
  ([handler]
     (reify AsyncCallback$StringCallback
       (processResult [this result-code path contex name]
         (handler result-code path contex name)))))

(defn client
  ([connection-string timeout watcher-fn]
     (ZooKeeper. connection-string timeout (watcher watcher-fn))))

(defn exists
  ([client name]
     (exists client name false))
  ([client name watcher-fn]
     (.exists client name (watcher watcher-fn))))

(defn sync-create
  " Creates a node and returns nil if the node already exists
  Example:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" 120 #(println \"event received: \" %)))
    (create treeherd \"/foo\" :acl :open_acl_unsafe :create-mode :persistent)
    (create treeherd \"/foo/bar\")
"
  ([client name & {:keys [data acl create-mode]
                   :or {create-mode :ephemeral
                        acl :open_acl_unsafe}}]
     (if-not (exists client name)
       (.create client name data (acls acl) (create-modes create-mode)))))

(defn promise-callback
  ([prom callback-fn]
     (fn [return-code path context name]
       (deliver prom {:return-code return-code
                      :path path
                      :context context
                      :name name})
       (when callback-fn
         (callback-fn return-code path context name)))))

(defn create
  "
  Example:

    (use 'treeherd.client)
    (def treeherd (client \"127.0.0.1:2181\" 120 #(println \"event received: \" %)))

    (defn callback [return-code path context name]
      (println \"got callback: rc=\" return-code \" path=\" path \", context=\" context \", name=\" name))

    (def p0 (create treeherd \"/baz\" :callback callback :create-mode :persistent))
    (def p1 (create treeherd \"/baz/1\" :callback callback))
    (def p2 (create treeherd \"/baz/2\"))

"
  ([client name & {:keys [data acl create-mode context callback async?]
                            :or {create-mode :ephemeral
                                 acl :open_acl_unsafe
                                 context name
                                 async? true}}]
     (let [p (promise)]
       (if (exists client name)
         (deliver p nil)
         (.create client name data (acls acl)
                  (create-modes create-mode)
                  (string-callback (promise-callback p callback))
                  context))
       p)))

(defn delete
  ([client name]
     (delete client name -1))
  ([client name version]
     (.delete client name version)))

(defn children
  ([client name]
     (children client name false))
  ([client name watcher-fn]
     (seq (.getChildren client name (watcher watcher-fn)))))