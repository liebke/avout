(ns treeherd.locks
  (:require [treeherd.zookeeper :as zk]
            [treeherd.zookeeper.util :as zutil]
            [treeherd.logger :as log])
  (:import (java.util.concurrent.locks Lock ReentrantLock Condition)
           (java.util.concurrent TimeUnit)
           (java.util Date UUID)))

(defprotocol DistributedLock
  (requestNode [this] "Returns the child node representing this lock request instance")
  (queuedRequests [this] "Returns all queued request-nodes for this distributed lock")
  (getOwner [this] "Returns the child node that holds the lock.")
  (deleteLock [this] "Deletes the distributed lock.")
  (hasLock [this] "Indicates if the current thread has the lock"))

(defprotocol DistributedReentrantLock
  (getHoldCount [this] "Gets the number of holds for this lock in the current thread"))

(defn unlock-all
  "Calls unlock on the given lock for each hold reported by getHoldCount"
  ([lock]
     (dotimes [i (.getHoldCount lock)] (.unlock lock))))

(defmacro with-lock
  ([lock & body]
     `(try
        (.lock ~lock)
        ~@body
        (finally (.unlock ~lock)))))

(defmacro when-lock
  ([lock & body]
     `(when (.tryLock ~lock)
        (try
          ~@body
          (finally (.unlock ~lock))))))

(defmacro if-lock
  ([lock then-exp else-exp]
     `(if (.tryLock ~lock)
        (try
          ~then-exp
          (finally (.unlock ~lock)))
        ~else-exp)))

(defmacro when-lock-with-timeout
  ([lock duration time-units & body]
     `(try
        (.tryLock ~lock ~duration ~time-units)
        ~@body
        (finally (.unlock ~lock)))))

(defmacro if-lock-with-timeout
  ([lock duration time-units then-exp else-exp]
     `(if (.tryLock ~lock ~duration ~time-units)
        (try
          ~then-exp
          (finally (.unlock ~lock)))
        ~else-exp)))

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

(defn delete-lock-request-node
  ([client lock-node request-id]
     (zk/delete client (str lock-node "/" (first (zutil/filter-children-by-prefix client lock-node request-id))))))

(defn delete-lock
  ([client lock-node]
     (zk/delete-all client lock-node)))

(defn queued-requests
  ([client lock-node]
     (zk/children client lock-node)))

(defn get-owner
  ([client lock-node]
     (when-let [requests (zk/children client lock-node)]
       (first (zutil/sort-sequential-nodes requests)))))

(defn submit-lock-request
  ([client lock-node request-id local-lock lock-watcher]
     (let [monitor-lock (ReentrantLock. true)
           watched-node-event (.newCondition monitor-lock)
           lock-request-node (create-lock-request-node client lock-node request-id)
           watcher (fn [event] (with-lock monitor-lock (.signal watched-node-event)))]
       (future
         (with-lock monitor-lock
           (loop [lock-request-queue (zutil/sort-sequential-nodes (zk/children client lock-node))]
             (when (seq lock-request-queue) ;; if no requests in the queue, then exit
               ;; when the node-to-watch is nil, the requester is no longer in the queue, then exit
               (if-let [node-to-watch (find-next-lowest-node lock-request-node lock-request-queue)]
                 (if (zk/exists client (str lock-node "/" node-to-watch) :watcher watcher)
                   (do (when (= lock-request-node node-to-watch)
                         (lock-watcher)) ;; then lock-request-node is the new lock holder, invoke lock-watcher
                       (.await watched-node-event) ;; wait until zookeeper invokes the watcher, which calls notify
                       (recur (zutil/sort-sequential-nodes (zk/children client lock-node))))
                   ;; if the node-to-watch no longer exists recur to find the next node-to-watch
                   (recur (zutil/sort-sequential-nodes (zk/children client lock-node)))))))))
       lock-request-node)))

(deftype ZKDistributedCondition [client conditionNode requestId distributedLock localCondition]
  Condition
  ;; TODO
  (await [this] nil)

  ;; TODO
  (await [this time unit] nil)

  ;; TODO
  (awaitNanos [this nanosTimeout] nil)

  ;; TODO
  (awaitUninterruptibly [this]
    (let [local-lock (.localLock distributedLock)
          cond-watcher (fn [event] (with-lock local-lock (.signal localCondition)))
          path (zk/create-all client (str conditionNode "/" requestId "-") :sequential? true :watcher cond-watcher)]
      (.unlock distributedLock)
      (with-lock local-lock
        (loop []
          (if (zk/exists client path :watcher cond-watcher)
            (do
              (.awaitUninterruptibly localCondition)
              (recur))
            (.lock distributedLock))))))

  ;; TODO
  (awaitUntil [this data] nil)

  ;; TODO
  (signal [this] nil)

  ;; TODO
  (signalAll [this] nil))


(deftype ZKDistributedReentrantLock [client lockNode requestId localLock]
  Lock
  (lock [this]
    (cond
      (= (zk/state client) :CLOSED)
        (throw (RuntimeException. "ZooKeeper session is closed, invalidating this lock object"))
      (> (.getHoldCount localLock) 0) ;; checking for reentrancy
        (.lock localLock) ;; if so, lock the local reentrant-lock again, but don't create a new distributed lock request
      :else
        (let [lock-granted (.newCondition localLock)
              watcher #(with-lock localLock (.signal lock-granted))]
          (.set requestId (.toString (UUID/randomUUID)))
          (submit-lock-request client lockNode (.get requestId) localLock watcher)
          (.lock localLock)
          (.await lock-granted))))

  (unlock [this]
    (try
      (when (= (.getHoldCount localLock) 1)
        (delete-lock-request-node client lockNode (.get requestId)))
      (finally (.unlock localLock))))

  (tryLock [this]
    (cond
      (= (zk/state client) :CLOSED)
        (throw (RuntimeException. "ZooKeeper session is closed, invalidating this lock object"))
      (> (.getHoldCount localLock) 0) ;; checking for reentrancy
        (.tryLock localLock) ;; if so, lock the local reentrant-lock again, but don't create a new distributed lock request
      :else
        (if (.tryLock localLock)
          (do (.set requestId (.toString (UUID/randomUUID)))
              (submit-lock-request client lockNode (.get requestId) localLock nil)
              (.sync client lockNode nil nil)
              (if (.hasLock this)
                true
                (do (delete-lock-request-node client lockNode (.get requestId))
                    false)))
          false)))

  (tryLock [this time unit]
    (cond
      (= (zk/state client) :CLOSED)
        (throw (RuntimeException. "ZooKeeper session is closed, invalidating this lock object"))
      (Thread/interrupted)
        (throw (InterruptedException.))
      (> (.getHoldCount localLock) 0) ;; checking for reentrancy
        (.tryLock localLock time unit) ;; lock the local reentrant-lock again, but don't create a new distributed lock request
      :else
        (let [lock-event (.newCondition localLock)
              start-time (System/nanoTime)]
          (if (.tryLock localLock time unit)
            (let [time-left (- (.toNanos unit time) (- (System/nanoTime) start-time))
                  watcher #(when-lock-with-timeout localLock time-left (TimeUnit/NANOSECONDS) (.signal lock-event))]
              (.set requestId (.toString (UUID/randomUUID)))
              (submit-lock-request client lockNode (.get requestId) localLock watcher)
              (.awaitNanos lock-event time-left)
              (cond
                (Thread/interrupted)
                  (do (delete-lock-request-node client lockNode (.get requestId))
                      (throw (InterruptedException.)))
                (.hasLock this)
                  true
                :else
                  (do (delete-lock-request-node client lockNode (.get requestId))
                      false)))
            false))))

  ;; TODO
  (lockInterruptibly [this] nil)

  ;; TODO
  (newCondition [this]
    (ZKDistributedCondition. client
                             (str lockNode "-condition-" (UUID/randomUUID))
                             (UUID/randomUUID)
                             this
                             (.newCondition localLock)))


  DistributedLock
  (requestNode [this]
    (first (zutil/filter-children-by-prefix client lockNode (.get requestId))))

  (queuedRequests [this]
    (queued-requests client lockNode))

  (getOwner [this]
    (get-owner client lockNode))

  (deleteLock [this]
    (delete-lock client lockNode))

  (hasLock [this]
    (and (not= (zk/state client) :CLOSED)
         (.isHeldByCurrentThread localLock)
         (= (.requestNode this) (.getOwner this))))


  DistributedReentrantLock
  (getHoldCount [this]
    (.getHoldCount localLock)))


;; public constructor for RKDistributedReentrantLock
(defn distributed-lock
  "Initializer for ZKDistributedReentrantLock

  Examples:

    (use '(treeherd zookeeper locks))
    (require '[treeherd.logger :as log])

    (def client (connect \"127.0.0.1\"))

    (delete-lock client \"/lock\")


    (def dlock (distributed-lock client))
    (future (with-lock dlock
              (log/debug \"dlock granted: count = \" (.getHoldCount dlock) \", owner = \" (.getOwner dlock))
              (Thread/sleep 5000))
           (log/debug \"dlock released: count = \" (.getHoldCount dlock) \", owner = \" (.getOwner dlock)))

    (.getOwner dlock)
    (delete client (str \"/lock/\" (.getOwner dlock)))

    (def dlock2 (distributed-lock client))
    (future
      (with-lock dlock2
        (log/debug \"dlock2 granted: count = \" (.getHoldCount dlock2) \", owner = \" (.getOwner dlock2))
        (Thread/sleep 5000)
        (log/debug \"attempting to lock dlock2 again...\")
        (with-lock dlock2
          (log/debug \"dlock2 granted again: count = \" (.getHoldCount dlock2) \", owner = \" (.getOwner dlock2))
          (log/debug \"sleeping...\")
          (Thread/sleep 5000)
          (log/debug \"awake...\"))
        (log/debug \"dlock2 unlocked: count = \" (.getHoldCount dlock2) \", owner = \" (.getOwner dlock2)))
      (log/debug \"dlock2 unlocked again: count = \" (.getHoldCount dlock2) \", owner = \" (.getOwner dlock2)))

    (.getOwner dlock2)
    (delete client (str \"/lock/\" (.getOwner dlock2)))


    (future (.lock dlock) (log/debug \"first lock acquired acquired again\"))
    (.getOwner dlock)
    (.unlock dlock)

    (.queuedRequests dlock)

   ;; from another repl
   ;; connect to dlock lock
   (def dlock-reconnected (distributed-lock client :request-id (.getOwner dlock)))
   (.unlock dlock-reconnected)

   (.delete dlock)

"
  ([client & {:keys [lock-node request-id]
              :or {lock-node "/lock"}}]
     (ZKDistributedReentrantLock. client lock-node
                                  (doto (ThreadLocal.)
                                    (.set request-id))
                                  (ReentrantLock. true))))

