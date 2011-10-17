(ns treeherd.locks
  (:require [treeherd.client :as tc])
  (:import (java.util.concurrent.locks Lock)))

(defn next-lowest
  ([node sorted-nodes]
     (if (= node (first sorted-nodes)) ;; then the node is the lowest
       node
       (loop [[current & remaining] sorted-nodes
              previous nil]
         (when current ;; if there is no current node, then the node is not in this list
           (if (= node current)
             previous
             (recur remaining current)))))))

(defn make-lock-request-node
  ([client lock-node req-id]
     (let [path (str lock-node "/" req-id "-")
           request-node (tc/create-all client path :sequential? true)]
       (subs request-node (inc (count lock-node))))))

(defn get-lock-request-node
  ([client lock-node request-id]
     (filter #(re-find (re-pattern request-id) %) (tc/children client lock-node))))

(defn delete-lock
  ([client & {:keys [lock-node]
              :or {lock-node "/lock"}}]
     (tc/delete-all client lock-node)))

(defn lock*
  ([client lock-node request-node lock-watcher]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))]
       (future
         (locking mutex
           (loop [request-queue (tc/sort-sequential-nodes (tc/children client lock-node))]
             (if (seq request-queue) ;; if there are no request-queue, close the lock
               ;; when the node-to-watch is nil, then the requester is no longer in the request queue, so exit
               (when-let [node-to-watch (next-lowest request-node request-queue)]
                 (tc/exists client (str lock-node "/" node-to-watch) :watcher watcher)
                 (when (= request-node node-to-watch) ;; then request-node is the new lock holder
                   (when lock-watcher (lock-watcher)))
                 (.wait mutex) ;; wait until zookeeper invokes the watcher, which calls notify
                 (recur (tc/sort-sequential-nodes (tc/children client lock-node))))
               (delete-lock client :lock-node lock-node))))))))

(defn unlock*
  ([client lock-node request-id]
     (tc/delete client (str lock-node "/" (first (get-lock-request-node client lock-node request-id))))))

(defrecord DistributedLock [client lock-node request-id]
  Lock
  (lock [this]
    (let [request-node (make-lock-request-node client lock-node request-id)
          mutex (Object.)]
      (locking mutex
        (lock* client
               lock-node
             request-node
             #(do
                (println (str "lock has been granted to " request-node))
                (locking mutex (.notify mutex))))
      (.wait mutex))))

  (unlock [this]
    (unlock* client lock-node request-id)))

(defn distributed-lock
  "

  Examples:

    (use '(treeherd client locks))
    (def treeherd (client \"127.0.0.1\"))

    (delete-lock treeherd) ;; delete existing lock (default lock-node is \"/lock\")

    (def dlock (distributed-lock treeherd))
    (.lock dlock)

    (def dlock2 (distributed-lock treeherd))
    (.lock dlock2)
    ;; will wait until it get's the lock.

   ;; from another repl
   ;; connect to dlock lock
   (def dlock-reconnected (distributed-lock treeherd :request-id \"dlock-request-id\"))
   (.unlock dlock-reconnected)

"
  ([client & {:keys [lock-node request-id]
              :or {lock-node "/lock"
                   request-id (.toString (java.util.UUID/randomUUID))}}]
     (DistributedLock. client lock-node request-id)))

