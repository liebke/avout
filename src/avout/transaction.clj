(ns avout.transaction
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
  (alterRef [this f args])
  (commuteRef [this f args])
  (ensureRef [this]))

;; implementation

(def ^:dynamic *stm-node* "/stm")

(def HISTORY "/history")
(def TXN "/txn")
(def PT-PREFIX "t-")
(def NODE-DELIM "/")

(def RETRY-LIMIT 25)
(def LOCK-WAIT-MSEC 100)
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
     (zk/create-all client (str *stm-node* HISTORY) :persistent? true)))

(defn reset-stm
  ([client]
     (zk/delete-all client *stm-node*)
     (init-stm client)))

(defn init-ref
  ([client ref-name]
     (zk/create-all client (str ref-name HISTORY) :persistent? true)
     (zk/create client (str ref-name TXN) :persistent? true)))

(defn reset-ref
  ([client ref-name]
     (zk/delete-all client ref-name)
     (init-ref client ref-name)))

(defn extract-point [path]
  (subs path (- (count path) 12) (count path)))

(defn next-point
  ([client]
     (extract-point
      (zk/create client (str *stm-node* HISTORY NODE-DELIM PT-PREFIX)
                 :persistent? true
                 :sequential? true))))

(defn split-ref-commit-history [history-node]
  (when history-node
    (let [[_ txid _ commit-pt] (s/split history-node #"-")]
      [(str PT-PREFIX txid) (str PT-PREFIX commit-pt)])))

(defn point-node [point]
  (str *stm-node* HISTORY NODE-DELIM point))

(defn update-state
  ([client point new-state]
     (zk/set-data client (point-node point) new-state -1))
  ([client point old-state new-state]
     (zk/compare-and-set-data client (point-node point) old-state new-state)))

(defn get-history [client ref-name]
  (zk/children client (str ref-name HISTORY)))

(defn state= [s1 s2]
  (Arrays/equals s1 s2))

(defn current-state? [client txid & states]
  (when txid
    (let [state (:data (zk/data client (point-node txid)))]
      (reduce #(or %1 (state= state %2)) false states))))

(defn get-last-committed-point
  "Gets the last committed point for the given Ref."
  ([client ref-name]
     (let [history (util/sort-sequential-nodes > (get-history client ref-name))]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (split-ref-commit-history h)]
           (if (current-state? client txid COMMITTED)
             h
             (recur hs)))))))

(defn get-committed-point-before
  "Gets the committed point before the given one."
  ([client ref-name point]
     (let [history (util/sort-sequential-nodes > (get-history client ref-name))
           point-int (util/extract-id point)]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (split-ref-commit-history h)]
           (if (and (<= (util/extract-id commit-pt) point-int)
                    (current-state? client txid COMMITTED))
             h
             (recur hs)))))))

(defn behind-committing-point?
  "Returns true when the given txn's read-point is behind either a committed or committing point."
  ([ref point]
     (when-let [history (util/sort-sequential-nodes > (get-history (.client ref) (.getName ref)))]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (split-ref-commit-history h)]
           (if (current-state? (.client ref) txid COMMITTING COMMITTED)
             (> (util/extract-id commit-pt) (util/extract-id point))
             (recur hs)))))))

(defn write-commit-point [txn ref]
  (zk/create (.client txn) (str (.getName ref) HISTORY NODE-DELIM (deref (.txid txn)) "-" (deref (.commitPoint txn)))
             :persistent? true))

(defn trigger-watches
  [txn]
  (doseq [r (keys (deref (.values txn)))]
    (zk/set-data (.client txn) (.getName r) (data/to-bytes 0) -1)))

(defn process-commutes [txn]
  ;;TODO
  nil)

(defn validate [validator value]
  (when (and validator (not (validator value)))
    (throw (IllegalStateException. "Invalid reference state"))))

(defn validate-values [txn]
  (let [values (deref (.values txn))]
    (doseq [r (keys values)]
      (validate (deref (.validator r)) (get values r)))))

(defn barge-time-elapsed? [txn]
  (> (- (System/nanoTime) (deref (.startTime txn)))
     BARGE-WAIT-NANOS))

(defn barge [txn tagged-txid]
  (and (barge-time-elapsed? txn)
       (< (util/extract-id (deref (.txid txn))) (util/extract-id tagged-txid))
       (update-state (.client txn) tagged-txid RUNNING KILLED)))

(defn block-and-bail [txn]
  (.await (deref (.latch txn)) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS)
  (throw retryex))

(defn tagged? [client ref-name]
  "Returns the txid of the running transaction that tagged this ref, otherwise it returns nil"
  (when-let [txid (first (zk/children client (str ref-name TXN)))]
    (when (current-state? client txid RUNNING COMMITTING)
      txid)))

(defn write-tag [txn ref]
  (if (behind-committing-point? ref (deref (.readPoint txn)))
    (block-and-bail txn)
    (do (zk/delete-children (.client txn) (str (.getName ref) TXN))
        (zk/create (.client txn) (str (.getName ref) TXN NODE-DELIM (deref (.txid txn))) :persistent? false))))

(defn sync-ref [ref]
  (.sync (.client ref) (str *stm-node* HISTORY) nil nil)
  (.sync (.client ref) (str (.getName ref) HISTORY) nil nil))

(defn try-tag [txn ref]
  (sync-ref ref)
  (locks/with-lock (.writeLock (.lock ref))
    (when-let [tagged-txid (tagged? (.client txn) (.getName ref))]
      (when (and (not= (deref (.txid txn)) tagged-txid)
                 (not (barge txn tagged-txid)))
        (block-and-bail txn)))
    (write-tag txn ref)))

(defn update-values [txn]
  (let [values (deref (.values txn))]
   (doseq [r (keys values)]
     (write-commit-point txn r)
     (let [ref-state (.refState r)
           point (str (deref (.txid txn)) "-" (deref (.commitPoint txn)))]
       (.setState (.refState r) (.setCache r point (get values r)) point)))))

(defn stop [txn]
  (when-not (current-state? (.client txn) (deref (.txid txn)) COMMITTED)
    (update-state (.client txn) (deref (.txid txn)) RETRY))
  (reset! (.values txn) {})
  (reset! (.sets txn) #{})
  (.clear (.commutes txn))
  (.countDown (deref (.latch txn))))

(defn running? [txn]
  (current-state? (.client txn) (deref (.txid txn)) RUNNING COMMITTING))

;; Should committment notifications be sent ReferenceState instances?
(defn send-commit-notifications [txn]
  (let [values (deref (.values txn))]
   (doseq [r (keys values)]
     (.committed (.refState r) (str (deref (.txid txn)) "-" (deref (.commitPoint txn)))))))

(deftype LockingTransaction [returnValue client txid startPoint
                             startTime readPoint commitPoint
                             values sets commutes ensures latch]
  Transaction

  (doGet [this ref]
    (if (running? this)
      (or (get @values ref)
          (do (sync-ref ref)
              (let [commit-point (get-committed-point-before client (.getName ref) @readPoint)]
                (if (behind-committing-point? ref commit-point)
                  (block-and-bail this)
                  (or (.getCache ref commit-point)
                      (.setCache ref commit-point (.getState (.refState ref) commit-point)))))))
      (throw retryex)))

  (doSet [this ref value]
    (if (running? this)
      (do
        (when-not (contains? @sets ref)
          (try-tag this ref)
          (swap! sets conj ref))
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
            (reset! latch (CountDownLatch. 1))
            (when (zero? retry-count)
              (reset! txid @readPoint)
              (reset! startPoint @readPoint)
              (reset! startTime (System/nanoTime)))
            (update-state client @txid RUNNING)
            (reset! returnValue (f))
            (when (update-state client @txid RUNNING COMMITTING)
              ;(process-commutes this) ;; TODO
              (validate-values this)
              (reset! commitPoint (next-point client))
              (update-values this)
              (trigger-watches this)
              (update-state client @txid COMMITTED)
              ;; should commit notifications to the ReferenceState be included?
              (send-commit-notifications this))
            (catch Error e (when-not (retryex? e) (throw e)))
            (finally (stop this)))
          (when-not (current-state? client @txid COMMITTED)
            (recur (inc retry-count))))
        (throw (RuntimeException. "Transaction failed after reaching retry limit"))))
    @returnValue))

(def local-transaction (ThreadLocal.))

(defn create-local-transaction [client]
  (.set local-transaction
        (LockingTransaction. (atom nil)         ;; returnValue
                             client             ;; client
                             (atom nil)         ;; txid
                             (atom nil)         ;; startPoint
                             (atom nil)         ;; startTime
                             (atom nil)         ;; readPoint
                             (atom nil)         ;; commitPoint
                             (atom {})          ;; values
                             (atom #{})         ;; sets
                             (TreeMap.)         ;; commutes
                             (atom #{})         ;; ensures
                             (atom (CountDownLatch. 1)) ;; latch
                             )))

(defn get-local-transaction [client]
  (or (.get local-transaction)
      (do (create-local-transaction client)
          (.get local-transaction))))

(defn run-in-transaction [client f]
  (.runInTransaction (get-local-transaction client) f))

