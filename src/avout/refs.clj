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
  (setRef [this value])
  (commuteRef [this f args])
  (ensureRef [this]))

;; implementation

(def local-transaction (ThreadLocal.))

(def ^:dynamic *stm-node* "/stm")

(def RETRY-LIMIT 10)
(def LOCK-WAIT-MSEC (* 10 100))
(def BARGE-WAIT-NANOS (* 10 10 1000000))

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
         (if-let [[txid commit-pt] (split-ref-commit-history h)]
           (if (current-state? client txid COMMITTED)
             txid
             (recur hs))
           (throw retryex))))))

(defn tagged? [client ref-name]
  "Returns the txid of the running transaction that tagged this ref, otherwise it returns nil"
  (when-let [txid (first (zk/children client (str ref-name "/txn")))]
    (when (current-state? client txid RUNNING COMMITTING)
      txid)))

(defn tag-ref [client ref-name txid]
  (zk/delete-children client (str ref-name "/txn"))
  (zk/create client (str ref-name "/txn/" txid) :persistent? false))

(defn set-commit-point [client ref-name txid commit-point]
  (zk/create client (str ref-name "/history/" txid "-" (extract-point commit-point))
             :persistent? true))

(defn trigger-watchers
  [client ref-name]
  (zk/set-data client ref-name (data/to-bytes 0) -1))

(defn validate [validator value]
  (when (and validator (not (validator value)))
    (throw (IllegalStateException. "Invalid reference state"))))

(defn try-write-lock [txn ref]
  (if-not (.tryLock (.writeLock (.lock ref)) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS)
    (swap! (.locked txn) conj ref)
    (throw retryex)))

(defn unlock-refs [txn]
  (doseq [r (.locked txn)]
    (.unlock (.writeLock (.lock r))))
  (reset! (.locked txn) #{}))

(defn process-commutes [txn]
  ;;TODO
  nil)

(defn process-sets [txn]
  (doseq [r (.sets txn)]
    (try-write-lock txn r)))

(defn process-values [txn ref-state]
  (let [values (deref (.values txn))]
    (doseq [r (keys values)]
      (.setState ref-state (.getRefName r) (get values r) (.getCommitPoint txn)))))

(defn running? [client txid]
  (current-state? client txid RUNNING COMMITTING))

(defn barge-time-elapsed? [txn]
  (> (- (System/nanoTime) (.startTime txn))
     BARGE-WAIT-NANOS))

(defn barge [txn barged-txid]
  ;; need equiv of (.countDown (.latch barged-txn))
  (and (barge-time-elapsed? txn)
       (< (.readPoint txn) barged-txid)
       (update-state (.client txn) barged-txid RUNNING KILLED)))

(defn block-and-bail [txn]
  (.await (.latch txn) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS)
  (throw retryex))

(defn lock-ref [client ref txn]
  (locks/if-lock-with-timeout (.writeLock (.lock ref)) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS
    (if (get-committed-point-before client (.getRefName ref) (.readPoint txn))
      (do
        (when-let [other-txid (tagged? client (.getRefName ref))]
         (when-not (barge txn other-txid)
           (block-and-bail txn)))
        (tag-ref client (.getRefName ref) (.readPoint txn)))
      (throw retryex))
    (throw retryex)))

(defn stop [txn client]
  (when-not (state= (deref (.state txn)) COMMITTED)
    (update-state! client txn RETRY))
  (reset! (.values txn) {})
  (reset! (.sets txn) #{})
  (.clear (.commutes txn))
  (.countDown (.latch txn)))

(deftype LockingTransaction [returnValue client startTime readPoint commitPoint
                             values sets commutes ensures locked latch]

  Transaction

  (doGet [this ref]
    (if (running? client readPoint)
      (or (get @values ref)
          (locks/if-lock-with-timeout (.readLock (.lock ref)) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS
            (.getState ref (get-committed-point-before client (.getRefName ref) @readPoint))
            (throw retryex)))
      (throw retryex)))

  (doSet [this ref value]
    (if (running? client readPoint)
      (do
        (when-not (contains? @sets ref)
          (swap! sets conj ref)
          (lock-ref client ref readPoint))
        (swap! values assoc ref value)
        value)
      (throw retryex)))

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
            (reset! readPoint (next-point client))
            (reset! startTime (System/nanoTime))
            (update-state client this RUNNING)
            (reset! returnValue (f))
            (when (update-state client this RUNNING COMMITTING)
              (process-commutes this)
              (process-sets this)
              (reset! commitPoint (next-point client))
              (process-values this)
              (update-state client this COMMITTED))
            (catch Error e
              (if (retryex? e)
                (do
                  (println "Retrying Transaction..."))
                (throw e)))
            (finally
              (unlock-refs this)
              (stop this client)))
          (when-not (current-state? client readPoint COMMITTED)
            (recur (inc retry-count))))
        (throw (RuntimeException. "Transaction failed after reaching retry limit"))))
    (deref (.returnValue this))))


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
  (zk/create client name :persistent? true)
  (DistributedReference. client name ref-state
                         (atom validator) (atom {})
                         (locks/distributed-read-write-lock client :lock-node (str name "/lock"))))

;; ZK data implementation

(deftype ZKRefState [client name]
  ReferenceState
  (setState [this value point] (throw (UnsupportedOperationException.)))

  (getState [this point] (throw (UnsupportedOperationException.))))

(defn zk-ref
  ([client name init-value & {:keys [validator]}]
     (doto (distributed-ref client name (ZKRefState. client name))
       (set-validator! validator)
       (.reset init-value))))