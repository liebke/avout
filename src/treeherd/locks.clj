(ns treeherd.locks
  (:require [treeherd.client :as zk]
            [treeherd.logger :as log])
  (:import (java.util.concurrent.locks Lock ReentrantLock)))

(defprotocol DistributedLock
  (requestNode [this] "Returns the child node representing this lock request instance")
  (queuedRequests [this] "Returns all queued request-nodes for this distributed lock")
  (getOwner [this] "Returns the child node that holds the lock.")
  (deleteLock [this] "Deletes the distributed lock.")
  (hasLock [this] "Indicates if the current thread has the lock"))

(defprotocol DistributedReentrantLock
  (getHoldCount [this] "Gets the number of holds for this lock in the current thread"))

(defmacro with-lock
  ([lock & body]
     `(try
        (.lock ~lock)
        ~@body
        (finally (.unlock ~lock)))))

(defmacro try-lock
  ([lock & body]
     `(try
        (.tryLock ~lock)
        ~@body
        (finally (.unlock ~lock)))))

(defmacro try-lock-with-timeout
  ([lock timeunits duration & body]
     `(try
        (.tryLock ~lock ~timeunits ~duration)
        ~@body
        (finally (.unlock ~lock)))))


;; internal functions

(defn find-next-lowest-node
  ([node sorted-nodes]
     (if (= node (first sorted-nodes))
       node
       (loop [[current & remaining] sorted-nodes
              previous nil]
         (when current
           (if (= node current)
             previous
             (recur remaining current)))))))

(defn create-lock-request-node
  ([client lock-node request-id]
     (let [path (str lock-node "/" request-id "-")
           request-node (zk/create-all client path :sequential? true)]
       (subs request-node (inc (count lock-node))))))

(defn find-lock-request-node
  ([client lock-node request-id]
     (when-let [requests (zk/children client lock-node)]
       (first (filter #(re-find (re-pattern request-id) %) requests)))))

(defn delete-lock-request
  ([client lock-node request-id]
     (zk/delete client (str lock-node "/" (find-lock-request-node client lock-node request-id)))))

(defn delete-lock
  ([client lock-node]
     (zk/delete-all client lock-node)))

(defn queued-requests
  ([client lock-node]
     (zk/children client lock-node)))

(defn get-owner
  ([client lock-node]
     (when-let [requests (zk/children client lock-node)]
       (first (zk/sort-sequential-nodes requests)))))

(defn submit-lock-request
  ([client lock-node request-id lock-watcher]
     (let [mutex (Object.)
           lock-request-node (create-lock-request-node client lock-node request-id)
           watcher (fn [event] (locking mutex (.notify mutex)))]
       (future
         (locking mutex
           (loop [lock-request-queue (zk/sort-sequential-nodes (zk/children client lock-node))]
             (if (seq lock-request-queue) ;; if no requests in the queue, delete the lock
               ;; when the node-to-watch is nil, the requester is no longer in the queue, so exit
               (if-let [node-to-watch (find-next-lowest-node lock-request-node lock-request-queue)]
                 (if (zk/exists client (str lock-node "/" node-to-watch) :watcher watcher)
                   (do (when (= lock-request-node node-to-watch)
                         (lock-watcher)) ;; then lock-request-node is the new lock holder, invoke lock-watcher
                       (.wait mutex) ;; wait until zookeeper invokes the watcher, which calls notify
                       (recur (zk/sort-sequential-nodes (zk/children client lock-node))))
                   ;; if the node-to-watch no longer exists recur to find the next node-to-watch
                   (recur (zk/sort-sequential-nodes (zk/children client lock-node))))
                 (delete-lock client lock-node))))))
       lock-request-node)))

(deftype ZKDistributedReentrantLock [client lockNode requestId localLock]
  Lock
  (lock [this]
    (if (> (.getHoldCount localLock) 0)
      (.lock localLock)
      (let [lock-granted (.newCondition localLock)
            watcher (fn []
                      (try
                        (.lock localLock)
                        (.signal lock-granted)
                        (finally (.unlock localLock))))]
        (submit-lock-request client lockNode requestId watcher)
        (.lock localLock)
        (.await lock-granted))))

  (unlock [this]
    (try
      (if (= (.getHoldCount localLock) 1)
        (delete-lock-request client lockNode requestId))
      (finally (.unlock localLock))))

  (tryLock [this]
    (if (> (.getHoldCount localLock) 0)
      (.tryLock localLock)
      (if (.tryLock localLock)
        (do (submit-lock-request client lockNode requestId nil)
            (.sync client lockNode nil nil)
            (if (.hasLock this)
              true
              (do (delete-lock-request-node client lockNode requestId)
                  false)))
        false)))

  DistributedLock
  (requestNode [this]
    (find-lock-request-node client lockNode requestId))

  (queuedRequests [this]
    (queued-requests client lockNode))

  (getOwner [this]
    (get-owner client lockNode))

  (deleteLock [this]
    (delete-lock client lockNode))

  (hasLock [this]
    (and (.isHeldByCurrentThread localLock)
         (= (.requestNode this) (.getOwner this))))

  DistributedReentrantLock
  (getHoldCount [this]
    (.getHoldCount localLock)))

(defn distributed-lock
  "

  Examples:

    (use '(treeherd client locks))
    (def treeherd (client \"127.0.0.1\"))

    (delete-lock treeherd \"/lock\")

    (def dlock (distributed-lock treeherd))
    (future (.lock dlock) (println \"first lock acquired\") (flush) (Thread/sleep 5000))
    (.getOwner dlock)
    (delete treeherd (str \"/lock/\" (.getOwner dlock)))

    (def dlock2 (distributed-lock treeherd))
    (future
      (.lock dlock2)
      (println \"second lock acquired: \" (.getHoldCount dlock2))
      (Thread/sleep 5000)
      (println \"attempting to lock dlock2 again...\")
      (.lock dlock2)
      (println \"second lock acquired again in same thread: \" (.getHoldCount dlock2))
      (flush)
      (println \"sleeping...\") (flush)
      (Thread/sleep 5000)
      (println \"awake...\") (flush)
      (.unlock dlock2)
      (println \"unlocked second lock: \" (.getHoldCount dlock2))
      (.unlock dlock2)
      (println \"unlocked second lock again: \" (.getHoldCount dlock2)))
    (.getOwner dlock2)
    (delete treeherd (str \"/lock/\" (.getOwner dlock2)))


    (future (.lock dlock) (println \"first lock acquired acquired again\") (flush))
    (.getOwner dlock)
    (.unlock dlock)

    (.queuedRequests dlock)

   ;; from another repl
   ;; connect to dlock lock
   (def dlock-reconnected (distributed-lock treeherd :request-id (.getOwner dlock)))
   (.unlock dlock-reconnected)

   (.delete dlock)

"
  ([client & {:keys [lock-node request-id]
              :or {lock-node "/lock"
                   request-id (.toString (java.util.UUID/randomUUID))}}]
     (ZKDistributedReentrantLock. client lock-node request-id (ReentrantLock. true))))

