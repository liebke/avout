(ns avout.dynamodb.locks
  (:use avout.core
        avout.locks
        avout.dynamodb.atom)
  (:import (java.util.concurrent.locks Lock)
           (java.util UUID)))

(deftype DynamoDBLock [client table-name name locked flag]

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

(defn dynamodb-lock [client table-name name]
  (let [flag (dynamodb-atom client table-name name)
        lock (DynamoDBLock. client table-name name (atom false) flag)]
    (.compareAndSet flag nil false)
    lock))

;; LEASED LOCK

(defn expired-lock? [[_ lease-start-millis lease-duration-millis]]
  (> (- (System/currentTimeMillis) lease-start-millis) lease-duration-millis))

(deftype DynamoDBLeasedLock [client table-name name requestId locked lockInfo]

  LeasedLock

  (tryLeasedLock [this waitTime waitUnit leaseDuration leaseDurationUnit]
    (let [BACKOFF-BASE 1
          BACKOFF-FACTOR 2.5
          BACKOFF-MAX 1000
          start-time (System/nanoTime)
          lease-duration-millis (.toMillis leaseDurationUnit leaseDuration)]
      (loop [i 0, backoff BACKOFF-BASE]
        (let [lease-start-millis (System/currentTimeMillis)]
          (if (.compareAndSet lockInfo nil [requestId lease-start-millis lease-duration-millis])
           (reset! locked true)
           (let [current-lock-info @lockInfo]
             (if (and (expired-lock? current-lock-info)
                      (.compareAndSet lockInfo current-lock-info nil))
               (recur (inc i) backoff)
               (if (loop [spin 0]
                     (cond
                      (neg? (- (.toNanos waitUnit waitTime) (- (System/nanoTime) start-time))) false
                      (< spin backoff) (recur (inc spin))
                      :else true))
                 (recur (inc i)
                        (let [b (* backoff BACKOFF-FACTOR)]
                          (if (< b BACKOFF-MAX) b BACKOFF-MAX)))
                 false))))))))

  (unlock [this]
    (if @locked
      (if (.hasExpired this)
        (throw (IllegalMonitorStateException. "Lock lease has expired."))
        (do (.reset lockInfo nil)
            (reset! locked false)
            nil))
      (throw (IllegalMonitorStateException. "Cannot unlock: not current lock holder."))))

  (hasExpired [this]
    (let [current-lock-info @lockInfo]
      (if @locked
        (or (expired-lock? @lockInfo)
            (not= requestId (first current-lock-info)))
        (throw (IllegalMonitorStateException. "Wasn't lock holder"))))))

(defn dynamodb-leased-lock [client table-name name]
  (let [request-id (str (UUID/randomUUID))
        locked (atom false)
        lock-info (dynamodb-atom client table-name name)
        lock (DynamoDBLeasedLock. client table-name name request-id locked lock-info)]
    lock))

#_(defn dynamodb-initializer [name {:keys [ddb-client table-name]}]
  (dynamodb-leased-lock ddb-client table-name name))

(comment

  (use 'dynamodb.core :reload-all)
  (def aws-access-key-id (get (System/getenv) "AWS_ACCESS_KEY_ID"))
  (def aws-secret-key (get (System/getenv) "AWS_SECRET_KEY"))
  (def ddb (dynamodb-client aws-access-key-id aws-secret-key))

  (use 'avout.dynamodb.locks :reload-all)
  (import '(java.util.concurrent TimeUnit))
  (def lock (dynamodb-lock ddb "test" "lock"))
  (.tryLock lock 5 TimeUnit/SECONDS)

  (.unlock lock)
  
  )

(comment

  (use 'dynamodb.core :reload-all)
  (def aws-access-key-id (get (System/getenv) "AWS_ACCESS_KEY_ID"))
  (def aws-secret-key (get (System/getenv) "AWS_SECRET_KEY"))
  (def ddb (dynamodb-client aws-access-key-id aws-secret-key))

  (use 'avout.dynamodb.locks :reload-all)
  (import '(java.util.concurrent TimeUnit))
  (def lock (dynamodb-leased-lock ddb "test" "leased-lock"))
  (.tryLeasedLock lock 5 TimeUnit/SECONDS 10 TimeUnit/SECONDS)

  (.hasExpired lock)
  (.unlock lock)

)