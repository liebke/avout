(ns avout.locks)

;; lock protocols

(defprotocol LeasedLock
  (tryLeasedLock [this waitTime waitUnits duration durationUnits])
  (unlock [this])
  (hasExpired [this]))

;; locking macros

(defmacro with-lock
  ([lock & body]
     `(try
        (.lock ~lock)
        ~@body
        (finally (.unlock ~lock)))))

(defmacro when-lock
  ([lock & body]
     `(let [locked# (atom false)]
        (try
          (when (reset! locked# (.tryLock ~lock))
            ~@body)
          (finally (when @locked# (.unlock ~lock)))))))

(defmacro if-lock
  ([lock then-exp else-exp]
     `(let [locked# (atom false)]
        (try
          (if (reset! locked# (.tryLock ~lock))
            ~then-exp
            ~else-exp)
          (finally (when @locked# (.unlock ~lock)))))))

(defmacro when-lock-with-timeout
  ([lock duration time-units & body]
     `(let [locked# (atom false)]
        (try
          (when (reset! locked# (.tryLock ~lock ~duration ~time-units))
            ~@body)
          (finally (when @locked# (.unlock ~lock)))))))

(defmacro if-lock-with-timeout
  ([lock duration time-units then-exp else-exp]
     `(let [locked# (atom false)]
        (try
          (if (reset! locked# (.tryLock ~lock ~duration ~time-units))
            ~then-exp
            ~else-exp)
          (finally (when @locked# (.unlock ~lock)))))))


(defmacro when-leased-lock-with-timeout
  ([lock wait-time wait-units lease-duration lease-units & body]
     `(let [locked# (atom false)]
        (try
          (when (reset! locked# (.tryLeasedLock ~lock ~wait-time ~wait-units ~lease-duration ~lease-units))
            ~@body)
          (finally (when @locked# (.unlock ~lock)))))))

(defmacro if-leased-lock-with-timeout
  ([lock wait-time wait-units lease-duration lease-units then-exp else-exp]
     `(let [locked# (atom false)]
        (try
          (if (reset! locked# (.tryLeasedLock ~lock ~wait-time ~wait-units ~lease-duration ~lease-units))
            ~then-exp
            ~else-exp)
          (finally (when @locked# (.unlock ~lock)))))))
