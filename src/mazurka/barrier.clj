(ns mazurka.barrier
  (:require [zookeeper :as zk])
  (:import (java.net InetAddress)))

;; http://zookeeper.apache.org/doc/r3.3.3/api/index.html
;; http://wiki.apache.org/hadoop/ZooKeeper/Tutorial
;; http://zookeeper.apache.org/doc/r3.3.3/recipes.html
;; http://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html
;; http://highscalability.com/blog/2008/7/15/zookeeper-a-reliable-scalable-distributed-coordination-syste.html


(defn host-name
  ([] (-> (InetAddress/getLocalHost) .getCanonicalHostName)))

(defn leave-barrier
  "
  Examples:

    (use 'zookeeper)
    (use 'mazurka.barrier)
    (def client (client \"127.0.0.1:2181\"))

    ;; start a three process barrier and set leave-on-completion? to false
    ;; then manually leave as follows:

    (leave-barrier client \"/barrier\")

    (leave-barrier client \"/barrier\" :proc-name \"proc2\")

    (leave-barrier client \"/barrier\" :proc-name \"proc3\")

"
  ([client barrier-node & {:keys [proc-name] :or {proc-name (host-name)}}]
     (zk/delete client (str barrier-node "/" proc-name))
     (future
       (let [mutex (Object.)
             watcher (fn [event] (locking mutex (.notify mutex)))]
         (locking mutex
           (loop []
             (if (> (count (zk/children client barrier-node :watcher watcher)) 0)
               (do
                 (.wait mutex)
                 (recur))
               true)))))))

(defn enter-barrier
  "
  Examples:

    (use 'zookeeper)
    (use 'mazurka.barrier)
    (def client (client \"127.0.0.1:2181\"))
    (defn make-processor [i] (fn [] (println (str \"process \" i \" is running \")) i))
    (delete-all client \"/barrier\")

    (def f0 (enter-barrier client \"/barrier\" 3 (make-processor 0)))

    (def f1 (enter-barrier client \"/barrier\" 3 (make-processor 1) :proc-name \"proc2\"))

    (def f2 (enter-barrier client \"/barrier\" 3 (make-processor 2) :proc-name \"proc3\"))

    @f0
    @f1
    @f2

   ;; and the barrier will have no child nodes, since leave-on-completion? defaults to true
   (children client \"/barrier\")

"
  ([client barrier-node N f & {:keys [proc-name leave-on-completion?]
                               :or {proc-name (host-name)
                                    leave-on-completion? true}}]
     (zk/create-all client (str barrier-node "/" proc-name))
     (future
       (let [mutex (Object.)
             watcher (fn [event] (locking mutex (.notify mutex)))]
         (locking mutex
           (loop []
             (if (>= (count (zk/children client barrier-node :watcher watcher)) N)
               (let [result (f)]
                   (when leave-on-completion?
                     (leave-barrier client barrier-node :proc-name proc-name))
                   result)
               (do
                 (.wait mutex)
                 (recur)))))))))

