(ns avout.sdb.lock
  (:use avout.core
        avout.sdb.atom)
  (:import (java.util.concurrent.locks Lock)))

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

(comment

  (use 'avout.sdb :reload-all)
  (def ACCESS-KEY (get (System/getenv) "ACCESS_KEY"))
  (def SECRET-KEY (get (System/getenv) "SECRET_KEY"))
  (def sdb (sdb-client ACCESS-KEY SECRET-KEY))

  (import '(java.util.concurrent TimeUnit))
  (def lock (sdb-lock sdb "test-domain" "lock"))
  (.tryLock lock 5 TimeUnit/SECONDS)

)