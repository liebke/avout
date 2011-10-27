(ns mazurka.locks
  (:require [mazurka.locks.internal :as mli]
            [zookeeper :as zk]
            [zookeeper.util :as zutil]
            [zookeeper.logger :as log])
  (:import (java.util.concurrent.locks Lock ReentrantLock Condition)
           (java.util.concurrent TimeUnit)
           (java.util Date UUID)))

;; Distributed lock protocols

(defprotocol DistributedLock
  (requestNode [this] "Returns the child node representing this lock request instance.")
  (queuedRequests [this] "Returns all queued request-nodes for this distributed lock.")
  (getOwner [this] "Returns the child node that holds the lock.")
  (deleteLock [this] "Deletes the distributed lock.")
  (hasLock [this] "Indicates if the current thread has the lock.")
  (getCondition [this name] "Returns the condition with the given name."))

(defprotocol DistributedReentrantLock
  (getHoldCount [this] "Gets the number of holds for this lock in the current thread"))

;; locking macros

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

;; Distributed lock and condition types

(deftype ZKDistributedCondition [client conditionNode requestId distributedLock localCondition]

  Condition ;; methods for the java.util.concurrent.locks.Condition interface

  (await [this] (.awaitUninterruptibly this))

  ;; TODO
  (await [this time unit] (throw (RuntimeException. "UnimplementedMethodException")))

  ;; TODO
  (awaitNanos [this nanosTimeout] (throw (RuntimeException. "UnimplementedMethodException")))

  (awaitUninterruptibly [this]
    (let [local-lock (.localLock distributedLock)
          cond-watcher (fn [event] (with-lock local-lock (log/debug "signaling condition") (.signal localCondition)))
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
  (awaitUntil [this date] (throw (RuntimeException. "UnimplementedMethodException")))

  (signalAll [this]
    (when-let [conditions-to-signal (zk/children client conditionNode)]
      (zk/delete-children client conditionNode :sort? true)))

  (signal [this]
    (when-let [conditions-to-signal (zk/children client conditionNode)]
      (zk/delete client (str conditionNode "/" (first (zutil/sort-sequential-nodes conditions-to-signal)))))))


(deftype ZKDistributedReentrantLock [client lockNode requestId localLock]

  Lock ;; methods for the java.util.concurrent.locks.Lock interface

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
          (mli/submit-lock-request client lockNode (.get requestId) localLock watcher)
          (.lock localLock)
          (.await lock-granted))))

  (unlock [this]
    (try
      (when (= (.getHoldCount localLock) 1)
        (mli/delete-lock-request-node client lockNode (.get requestId)))
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
              (mli/submit-lock-request client lockNode (.get requestId) localLock nil)
              (.sync client lockNode nil nil)
              (if (.hasLock this)
                true
                (do (mli/delete-lock-request-node client lockNode (.get requestId))
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
              (mli/submit-lock-request client lockNode (.get requestId) localLock watcher)
              (.awaitNanos lock-event time-left)
              (cond
                (Thread/interrupted)
                  (do (mli/delete-lock-request-node client lockNode (.get requestId))
                      (throw (InterruptedException.)))
                (.hasLock this)
                  true
                :else
                  (do (mli/delete-lock-request-node client lockNode (.get requestId))
                      false)))
            false))))

  (lockInterruptibly [this] (throw (RuntimeException. "UnimplementedMethodException")))

  (newCondition [this]
    (ZKDistributedCondition. client
                             (str lockNode "-condition-" (UUID/randomUUID))
                             (UUID/randomUUID)
                             this
                             (.newCondition localLock)))


  DistributedLock ;; methods for the DistributedLock protocol

  (getCondition [this conditionNode]
    (ZKDistributedCondition. client
                             conditionNode
                             (UUID/randomUUID)
                             this
                             (.newCondition localLock)))
  (requestNode [this]
    (first (zk/filter-children-by-prefix client lockNode (.get requestId))))

  (queuedRequests [this]
    (mli/queued-requests client lockNode))

  (getOwner [this]
    (mli/get-owner client lockNode))

  (deleteLock [this]
    (mli/delete-lock client lockNode))

  (hasLock [this]
    (and (not= (zk/state client) :CLOSED)
         (.isHeldByCurrentThread localLock)
         (= (.requestNode this) (.getOwner this))))


  DistributedReentrantLock ;; methods for the DistributedReentrantLock protocol

  (getHoldCount [this]
    (.getHoldCount localLock)))


;; public constructor for RKDistributedReentrantLock

(defn distributed-lock
  "Initializer for ZKDistributedReentrantLock

  Examples:

    (use 'zookeeper)
    (use 'mazurka.locks)
    (require '[zookeeper.logger :as log])

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
                                  (doto (ThreadLocal.) (.set request-id))
                                  (ReentrantLock. true))))

