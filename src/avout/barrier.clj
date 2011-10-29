(ns avout.barrier
  (:require [zookeeper :as zk]
            [zookeeper.util :as zutil])
  (:import (java.net InetAddress)))

;; http://zookeeper.apache.org/doc/r3.3.3/api/index.html
;; http://wiki.apache.org/hadoop/ZooKeeper/Tutorial
;; http://zookeeper.apache.org/doc/r3.3.3/recipes.html
;; http://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html
;; http://highscalability.com/blog/2008/7/15/zookeeper-a-reliable-scalable-distributed-coordination-syste.html


(defn host-name
  ([] (.getCanonicalHostName (InetAddress/getLocalHost))))

(defn exit-barrier
  ([client & {:keys [barrier-node proc-name]
              :or {barrier-node "/barrier"
                   proc-name (host-name)}}]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))]
       (zk/delete client (str barrier-node "/ready"))
       (locking mutex
         (loop []
           (when-let [children (seq (sort (or (zk/children client barrier-node) nil)))]
             (cond
               ;; the last node deletes itself and the barrier node, letting all the processes exit
               (= (count children) 1)
                 (zk/delete-all client barrier-node)
               ;; first node watches the second, waiting for it to be deleted
               (= proc-name (first children))
                 (do (when (zk/exists client
                                      (str barrier-node "/" (second children))
                                      :watcher watcher)
                       (.wait mutex))
                     (recur))
               ;; rest of the nodes delete their own node, and then watch the
               ;; first node, waiting for it to be deleted
               :else
                 (do (zk/delete client (str barrier-node "/" proc-name))
                     (when (zk/exists client
                                      (str barrier-node "/" (first children))
                                      :watcher watcher)
                       (.wait mutex))
                     (recur)))))))))

(defn enter-barrier
"
  Examples:

    (use 'zookeeper)
    (use 'avout.barrier)
    (def client (client \"127.0.0.1:2181\"))

    (enter-barrier client 2 #(println \"First process is running\"))

    ;; From another REPL, execute the following

    (use 'zookeeper)
    (use 'avout.barrier)
    (def client (client \"127.0.0.1:2181\"))

    (enter-barrier client 2 #(println \"Second process is running\") :proc-name \"node2\")

"
  ([client n f & {:keys [barrier-node proc-name double-barrier?]
                  :or {barrier-node "/barrier"
                       proc-name (host-name)
                       double-barrier? true}}]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))]
       (locking mutex
         (zk/create-all client (str barrier-node "/" proc-name))
         (if (>= (count (zk/children client barrier-node)) n)
           (zk/create client (str barrier-node "/ready") :async? true)
           (do (zk/exists client (str barrier-node "/ready") :watcher watcher :async? true)
               (.wait mutex)))
         (let [results (f)]
           (if double-barrier?
             (exit-barrier client :barrier-node barrier-node :proc-name proc-name)
             (zk/delete-all client barrier-node))
           results)))))
