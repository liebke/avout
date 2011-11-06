(ns avout.refs
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [zookeeper.util :as util]
            [avout.locks :as locks]
            [clojure.string :as s])
  (:import (clojure.lang IRef)
           (java.util Arrays TreeMap)
           (java.util.concurrent TimeUnit CountDownLatch)))

;; protocols

(defprotocol ReferenceState
  (getRefName [this] "Returns the ZooKeeper node name associated with this reference.")
  (setState [this value point] "Sets the transaction-value associated with the given clock point.")
  (getState [this point] "Returns the value associated with given clock point."))

(defprotocol Transaction
  (doGet [this ref])
  (doSet [this ref value])
  (doCommute [this f args])
  (doEnsure [this])
  (runInTransaction [this f]))

(defprotocol TransactionReference
  (getName [this])
  (setRef [this value])
  (commuteRef [this f args])
  (ensureRef [this]))

;; implementation

(def local-transaction (ThreadLocal.))

(def ^:dynamic *stm-node* "/stm")

(def RETRY-LIMIT 5)
(def LOCK-WAIT-MSEC (* 10 100))
(def BARGE-WAIT-NANOS (* 100 10 1000000))

;; transaction states
(def RUNNING (data/to-bytes 0))
(def COMMITTING (data/to-bytes 1))
(def RETRY (data/to-bytes 2))
(def KILLED (data/to-bytes 3))
(def COMMITTED (data/to-bytes 4))

(def retryex (Error. "RETRY"))

(defn retryex? [e] (= "RETRY" (.getMessage e)))

(defn init-stm
  ([client]
     (zk/create-all client (str *stm-node* "/history") :persistent? true)))

(defn reset-stm
  ([client]
     (zk/delete-all client *stm-node*)
     (init-stm client)))

(defn init-ref
  ([client ref-name]
     (zk/create-all client (str ref-name "/history") :persistent? true)
     (zk/create client (str ref-name "/txn") :persistent? true)))

(defn reset-ref
  ([client ref-name]
     (zk/delete-all client ref-name)
     (init-ref client ref-name)))

(defn next-point
  ([client]
     (zk/create client (str *stm-node* "/history/t-")
                :persistent? true
                :sequential? true)))

(defn extract-point [path]
  (subs path (- (count path) 12) (count path)))

(defn split-ref-commit-history [history-node]
  (when history-node
    (let [[_ txid _ commit-pt] (s/split history-node #"-")]
      [(str "t-" txid) (str "t-" commit-pt)])))

(defn point-node [point]
  (str *stm-node* "/history/" point))

(defn update-state
  ([client point new-state]
     (zk/set-data client (point-node point) new-state -1))
  ([client point old-state new-state]
     (zk/compare-and-set-data client (point-node point) old-state new-state)))

(defn get-history [client ref-name]
  (zk/children client (str ref-name "/history")))

(defn state= [s1 s2]
  (Arrays/equals s1 s2))

(defn current-state? [client txid & states]
  (let [state (:data (zk/data client (point-node txid)))]
    (reduce #(or %1 (state= state %2)) false states)))

(defn get-committed-point-before
  "Gets the committed point before the given one, if none is available, throws retryex"
  ([client ref-name point]
     (let [history (util/sort-sequential-nodes > (get-history client ref-name))]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (split-ref-commit-history h)]
           (if (current-state? client txid COMMITTED)
             h
             (recur hs)))))))

(defn tagged? [client ref-name]
  "Returns the txid of the running transaction that tagged this ref, otherwise it returns nil"
  (when-let [txid (first (zk/children client (str ref-name "/txn")))]
    (when (current-state? client txid RUNNING COMMITTING)
      txid)))

(defn tag-ref [client ref-name txid]
  (zk/delete-children client (str ref-name "/txn"))
  (zk/create client (str ref-name "/txn/" txid) :persistent? false))

(defn set-commit-point [client ref-name txid commit-point]
  (zk/create client (str ref-name "/history/" txid "-" commit-point)
             :persistent? true))

(defn trigger-watchers
  [client ref-name]
  (zk/set-data client ref-name (data/to-bytes 0) -1))

(defn try-write-lock [txn ref]
  (if (.tryLock (.writeLock (.lock ref)) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS)
    (swap! (.locked txn) conj ref)
    (do (println "try-write-lock: timeout")
        (throw retryex))))

(defn unlock-refs [txn]
  (doseq [r (deref (.locked txn))]
    (.unlock (.writeLock (.lock r))))
  (reset! (.locked txn) #{}))

(defn process-commutes [txn]
  ;;TODO
  nil)

(defn process-sets [txn]
  (doseq [r (deref (.sets txn))]
    (try-write-lock txn r)))

(defn validate [validator value]
  (when (and validator (not (validator value)))
    (throw (IllegalStateException. "Invalid reference state"))))

(defn validate-values [txn]
  (let [values (deref (.values txn))]
    (doseq [r (keys values)]
      (validate (deref (.validator r)) (get values r)))))

(defn process-values [txn]
  (let [values (deref (.values txn))]
    (doseq [r (keys values)]
      (set-commit-point (.client txn) (.getName r) (deref (.readPoint txn)) (deref (.commitPoint txn)))
      (.setState (.refState r) (get values r) (str (deref (.readPoint txn)) "-" (deref (.commitPoint txn)))))))

(defn barge-time-elapsed? [txn]
  (> (- (System/nanoTime) (deref (.startTime txn)))
     BARGE-WAIT-NANOS))

(defn barge [txn barged-txid]
  (and (barge-time-elapsed? txn)
       (< (util/extract-id (deref (.readPoint txn))) (util/extract-id barged-txid))
       (update-state (.client txn) barged-txid RUNNING KILLED)))

(defn block-and-bail [txn]
  (.await (.latch txn) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS)
  (println "block-and-bail")
  (throw retryex))

(defn lock-ref [txn ref]
  (locks/if-lock-with-timeout (.writeLock (.lock ref)) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS
    (if (or (not (get-history (.client txn) (.getName ref)))
            (get-committed-point-before (.client txn) (.getName ref) (deref (.readPoint txn))))
      (do
        (when-let [other-txid (tagged? (.client txn) (.getName ref))]
          (when (and (not= (deref (.readPoint txn)) other-txid) (not (barge txn other-txid)))
            (println "calling barge: " (deref (.readPoint txn)) ", " other-txid)
            (block-and-bail txn)))
        (tag-ref (.client txn) (.getName ref) (deref (.readPoint txn))))
      (do (println "lock-ref: no commit point before read point")
          (throw retryex)))
    (do (println "lock-ref: lock timeout")
        (throw retryex))))

(defn stop [txn]
  (when-not (current-state? (.client txn) (deref (.readPoint txn)) COMMITTED)
    (update-state (.client txn) (deref (.readPoint txn)) RETRY))
  (reset! (.values txn) {})
  (reset! (.sets txn) #{})
  (.clear (.commutes txn))
  (.countDown (.latch txn)))

(deftype LockingTransaction [returnValue client startTime readPoint commitPoint
                             values sets commutes ensures locked latch]

  Transaction

  (doGet [this ref]
    (if (current-state? client @readPoint RUNNING COMMITTING)
      (or (get @values ref)
          (locks/if-lock-with-timeout (.readLock (.lock ref)) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS
            (if-let [commit-point (get-committed-point-before client (.getName ref) @readPoint)]
              (.getState (.refState ref) commit-point)
              (do (println "doGet: no commit point before read-point")
                  (throw retryex)))
            (do (println "doGet: lock timeout")
                (throw retryex))))
      (do (println "doGet: transaction not running")
          (throw retryex))))

  (doSet [this ref value]
    (if (current-state? client @readPoint RUNNING COMMITTING)
      (do
        (when-not (contains? @sets ref)
          (swap! sets conj ref)
          (lock-ref this ref))
        (swap! values assoc ref value)
        value)
      (do (println "doSet: transaction not running")
          (throw retryex))))

  (doCommute [this f args]
    ;; TODO
    )

  (doEnsure [this]
    ;; TODO
    )

  (runInTransaction [this f]
    (loop [retry-count 0]
      (if (< retry-count RETRY-LIMIT)
        (do
          (try
            (when (zero? retry-count)
              (reset! readPoint (extract-point (next-point client)))
              (reset! startTime (System/nanoTime)))
            (update-state client @readPoint RUNNING)
            ;; f is user defined, and potentially long running
            (reset! returnValue (f))
            ;; once f has successfully run, begin the commit process
            (when (update-state client @readPoint RUNNING COMMITTING)
              (process-commutes this) ;; TODO
              (process-sets this)
              (validate-values this)
              (reset! commitPoint (extract-point (next-point client)))
              (process-values this)
              (update-state client @readPoint COMMITTED))
            (catch Error e
              (if (retryex? e)
                (do (println "runInTransaction: Retrying Transaction...")
                    (.printStackTrace e))
                (throw e)))
            (finally
              (unlock-refs this)
              (stop this)))
          (when-not (current-state? client @readPoint COMMITTED)
            (recur (inc retry-count))))
        (throw (RuntimeException. "Transaction failed after reaching retry limit"))))
    @returnValue))


(defn get-local-transaction [client]
  (or (.get local-transaction)
      (do (.set local-transaction
                (LockingTransaction. (atom nil) ;; returnValue
                                     client ;; client
                                     (atom nil) ;; startTime
                                     (atom nil) ;; readPoint
                                     (atom nil) ;; commitPoint
                                     (atom {}) ;; values
                                     (atom #{}) ;; sets
                                     (TreeMap.) ;; commutes
                                     (atom #{}) ;; ensures
                                     (atom #{}) ;; locked
                                     (CountDownLatch. 1) ;; latch
                                     ))
          (.get local-transaction))))

(defn run-in-transaction [client f]
  (.runInTransaction (get-local-transaction client) f))


;; distributed reference implementation

(deftype DistributedReference [client nodeName refState validator watches lock]
  TransactionReference
  (getName [this] nodeName)

  (setRef [this value] (throw (UnsupportedOperationException.)))

  (commuteRef [this f args] (throw (UnsupportedOperationException.)))

  (ensureRef [this] (throw (UnsupportedOperationException.)))

  IRef
  (deref [this] (throw (UnsupportedOperationException.)))

  ;; callback params: akey, aref, old-val, new-val, but old-val will always be nil
  (addWatch [this key callback]
    (let [watcher (fn watcher-fn [event]
                    (when (contains? @watches key)
                      (when (= :NodeDataChanged (:event-type event))
                       (let [new-value (.deref this)]
                         (callback key this nil new-value)))
                      (zk/exists client nodeName :watcher watcher-fn)))]
      (swap! watches assoc key watcher)
      (zk/exists client nodeName :watcher watcher)
      this))

  (getWatches [this] @watches)

  (removeWatch [this key] (swap! watches (dissoc key)) this)

  (setValidator [this f] (reset! validator f))

  (getValidator [this] @validator))

(defn distributed-ref [client name ref-state & {:keys [validator]}]
  (init-ref client name)
  (DistributedReference. client name ref-state
                         (atom validator) (atom {})
                         (locks/distributed-read-write-lock client :lock-node (str name "/lock"))))

;; ZK data implementation

(defn serialize-form
  "Serializes a Clojure form to a byte-array."
  ([form]
     (data/to-bytes (pr-str form))))

(defn deserialize-form
  "Deserializes a byte-array to a Clojure form."
  ([form]
     (read-string (data/to-string form))))

(deftype ZKRefState [client name]
  ReferenceState
  (getRefName [this] name)

  (getState [this point]
    (let [_ (println "getState: history-node:" " name: " name ", point: " point)
          {:keys [data stat]} (zk/data client (str name "/history/" point))]
      (deserialize-form data)))

  (setState [this value point]
    (println "setState: " " name: " name ", point: " point)
    (zk/set-data client (str name "/history/" point) (serialize-form value) -1)))

(defn zk-ref
  ([client name init-value & {:keys [validator]}]
     (let [dref (doto (distributed-ref client name (ZKRefState. client name))
                  (set-validator! validator))]
       dref
       ;;(.setState (.refState dref) init-value 0)
       )))