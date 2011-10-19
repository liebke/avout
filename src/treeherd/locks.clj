(ns treeherd.locks
  (:require [treeherd.client :as tc]
            [treeherd.logger :as log])
  (:import (java.util.concurrent.locks Lock
                                       ReentrantLock)))

(defn- next-lowest
  ([node sorted-nodes]
     (if (= node (first sorted-nodes)) ;; then the node is the lowest
       node
       (loop [[current & remaining] sorted-nodes
              previous nil]
         (when current ;; if there is no current node, then the node is not in this list
           (if (= node current)
             previous
             (recur remaining current)))))))

(defn- make-lock-request-node
  ([client lock-node req-id]
     (let [path (str lock-node "/" req-id "-")
           request-node (tc/create-all client path :sequential? true)]
       (subs request-node (inc (count lock-node))))))

(defn delete-lock*
  ([client & {:keys [lock-node]
              :or {lock-node "/lock"}}]
     (tc/delete-all client lock-node)))

(defn this-request-node*
  ([client lock-node request-id]
     (when-let [requests (tc/children client lock-node)]
       (filter #(re-find (re-pattern request-id) %) requests))))

(defn all-request-nodes*
  ([client lock-node]
     (tc/children client lock-node)))

(defn lock-holder-node*
  ([client lock-node]
     (when-let [requests (tc/children client lock-node)]
       (first (tc/sort-sequential-nodes requests)))))

(defn lock*
  ([client lock-node request-node lock-watcher]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))]
       (future
         (locking mutex
           (loop [request-queue (tc/sort-sequential-nodes (tc/children client lock-node))]
             (if (seq request-queue) ;; if there are no requests in the queue, delete the lock
               ;; when the node-to-watch is nil, then the requester is no longer in the request queue, so exit
               (when-let [node-to-watch (next-lowest request-node request-queue)]
                 (tc/exists client (str lock-node "/" node-to-watch) :watcher watcher)
                 (when (= request-node node-to-watch) ;; then request-node is the new lock holder
                   (when lock-watcher (lock-watcher)))
                 (.wait mutex) ;; wait until zookeeper invokes the watcher, which calls notify
                 (recur (tc/sort-sequential-nodes (tc/children client lock-node))))
               (delete-lock* client :lock-node lock-node))))))))

(defn unlock*
  ([client lock-node request-id]
     (when-let [req-node (first (this-request-node* client lock-node request-id))]
       (tc/delete client (str lock-node "/" req-node)))))

(defprotocol DistributedLock
  (this-request-node [this] "Returns the child node representing this lock request instance")
  (all-request-nodes [this] "Returns all queued request-nodes for this distributed lock")
  (lock-holder-node [this] "Returns the child node that holds the lock.")
  (delete-lock [this] "Deletes the distributed lock."))

(defrecord DistributedReentrantLock [client lock-node request-id local-lock lock-counter]
  Lock
  (lock [this]
    (if (.get lock-counter)
      (do
        (.set lock-counter (inc (.get lock-counter))))
      (let [request-node (make-lock-request-node client lock-node request-id)
            condition (.newCondition local-lock)
            watcher (fn []
                      (try
                        (.lock local-lock)
                        (.signal condition)
                        (finally (.unlock local-lock))))]
        (try
          (lock* client lock-node request-node watcher)
          (.lock local-lock)
          (.set lock-counter 1)
          (.await condition)
          (finally (.unlock local-lock))))))

  (unlock [this]
    (if-let [counter (.get lock-counter)]
      (do
        (.set lock-counter (dec counter))
        (if (= counter 1)
          (unlock* client lock-node request-id)))
      (throw (IllegalMonitorStateException. "Attempting to unlock without first obtaining that lock on this thread"))))

  DistributedLock
  (this-request-node [this]
    (this-request-node* client lock-node request-id))

  (all-request-nodes [this]
    (all-request-nodes* client lock-node))

  (lock-holder-node [this]
    (lock-holder-node* client lock-node))

  (delete-lock [this]
    (delete-lock* client lock-node)))

(defn distributed-lock
  "

  Examples:

    (use '(treeherd client locks))
    (def treeherd (client \"127.0.0.1\"))

    (delete-lock* treeherd)

    (def dlock (distributed-lock treeherd))
    (future (.lock dlock) (println \"first lock acquired\") (flush) (Thread/sleep 5000))
    (.lock-holder-node dlock)
    (delete treeherd (str \"/lock/\" (.lock-holder-node dlock)))

    (def dlock2 (distributed-lock treeherd))
    (future
      (.lock dlock2)
      (println \"second lock acquired: \" (.get (.lock-counter dlock2)))
      (.lock dlock2)
      (println \"second lock acquired again in same thread: \" (.get (.lock-counter dlock2)))
      (flush)
      (.unlock dlock2)
      (println \"unlocked second lock: \" (.get (.lock-counter dlock2))))
      (.unlock dlock2)
      (println \"unlocked second lock again: \" (.get (.lock-counter dlock2)))
    (.lock-holder-node dlock2)
    (delete treeherd (str \"/lock/\" (.lock-holder-node dlock2)))


    (future (.lock dlock) (println \"first lock acquired acquired again\") (flush))
    (.lock-holder-node dlock)
    (.unlock dlock)

    (.all-request-nodes dlock)

   ;; from another repl
   ;; connect to dlock lock
   (def dlock-reconnected (distributed-lock treeherd :request-id (.lock-holder-node dlock)))
   (.unlock dlock-reconnected)

   (.delete dlock)

"
  ([client & {:keys [lock-node request-id]
              :or {lock-node "/lock"
                   request-id (.toString (java.util.UUID/randomUUID))}}]
     (DistributedReentrantLock. client lock-node request-id (ReentrantLock. true) (ThreadLocal.))))

