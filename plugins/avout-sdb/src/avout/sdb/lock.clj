(ns avout.sdb.lock
  (:use avout.core
        avout.sdb.atom)
  (:import (java.util.concurrent.locks Lock)))

(defprotocol BreakableLock
  (tryLock [this waitTime waitUnits duration durationUnits])
  (renewLock [this duration durationUnits])
  (breakLock [this])
  (isBroken [this]))

(deftype SDBLock [client domain-name name locked flag]
  Lock

  (lock [this]
    (throw (UnsupportedOperationException.)))

  (tryLock [this]
    (if @locked
      true
      (reset! locked (.compareAndSet flag false true))))

  (tryLock [this time unit]
    (let [BACKOFF-BASE 1
          BACKOFF-FACTOR 2.5
          BACKOFF-MAX 1000
          start-time (System/nanoTime)]
      (loop [i 0, backoff BACKOFF-BASE]
        (if (.compareAndSet flag false true)
          (reset! locked true)
          (if (loop [spin 0]
                (cond
                  (neg? (- (.toNanos unit time) (- (System/nanoTime) start-time))) false
                  (< spin backoff) (recur (inc spin))
                  :else true))
              (recur (inc i)
                     (let [b (* backoff BACKOFF-FACTOR)]
                       (if (< b BACKOFF-MAX) b BACKOFF-MAX)))
              false)))))

  (unlock [this] (when @locked
                   (do (.reset flag false)
                       (reset! locked false))))

  (lockInterruptibly [this]
    (throw (UnsupportedOperationException.)))

  (newCondition [this]
    (throw (UnsupportedOperationException.))))

(defn sdb-lock [client domain-name name]
  (let [flag (sdb-atom client domain-name name)
        lock (SDBLock. client domain-name name (atom false) flag)]
    (.compareAndSet flag nil false)
    lock))

;; BREAKABLE LOCK

(deftype SDBBreakableLock [client domain-name name locked lockInfo]
  Lock

  (lock [this]
    (throw (UnsupportedOperationException.)))

  (tryLock [this]
    (throw (UnsupportedOperationException.)))

  (tryLock [this time unit]
    (throw (UnsupportedOperationException.)))

  (unlock [this] (if (and @locked (.isBroken this))
                   (throw (RuntimeException. "Lock has been broken."))
                   (do (.reset lockInfo nil)
                       (reset! locked false))))

  (lockInterruptibly [this]
    (throw (UnsupportedOperationException.)))

  (newCondition [this]
    (throw (UnsupportedOperationException.)))

  BreakableLock

  (tryLock [this waitTime waitUnit duration durationUnit]
    (let [BACKOFF-BASE 1
          BACKOFF-FACTOR 2.5
          BACKOFF-MAX 1000
          start-time (System/nanoTime)
          duration-nanos (.toNanos durationUnit duration)]
      (loop [i 0, backoff BACKOFF-BASE]
        (let [locked-time (System/nanoTime)]
          (if (.compareAndSet lockInfo nil [requestId locked-time duration-nanos])
           (reset! locked true)
           (if (loop [spin 0]
                 (cond
                  (neg? (- (.toNanos waitUnit waitTime) (- (System/nanoTime) start-time))) false
                  (< spin backoff) (recur (inc spin))
                  :else true))
             (recur (inc i)
                    (let [b (* backoff BACKOFF-FACTOR)]
                      (if (< b BACKOFF-MAX) b BACKOFF-MAX)))
             false))))))

  (breakLock [this])

  (isBroken [this]))

(defn sdb-breakable-lock [client domain-name name]
  (let [lock-info (sdb-atom client domain-name name)
        lock (SDBLock. client domain-name name (atom false) lock-info)]
    lock))

(comment

  (use 'avout.sdb :reload-all)
  (def ACCESS-KEY (get (System/getenv) "ACCESS_KEY"))
  (def SECRET-KEY (get (System/getenv) "SECRET_KEY"))
  (def sdb (sdb-client ACCESS-KEY SECRET-KEY))

  (import '(java.util.concurrent TimeUnit))
  (def lock (sdb-lock sdb "test-domain" "lock"))
  (.tryLock lock 5 TimeUnit/SECONDS)

)