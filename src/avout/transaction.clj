(ns avout.transaction
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [zookeeper.util :as util]
            [avout.locks :as locks]
            [avout.config :as cfg]
            [clojure.string :as s])
  (:import (clojure.lang IRef)
           (java.util Arrays TreeMap)
           (java.util.concurrent TimeUnit CountDownLatch)
           (org.apache.zookeeper KeeperException$NoNodeException)))

;; protocols

(defprotocol Transaction
  (doGet [this ref])
  (doSet [this ref value])
  (doCommute [this f args])
  (doEnsure [this])
  (runInTransaction [this f]))

;; implementation

;; transaction states
(def RUNNING (data/to-bytes 0))
(def COMMITTING (data/to-bytes 1))
(def RETRY (data/to-bytes 2))
(def KILLED (data/to-bytes 3))
(def COMMITTED (data/to-bytes 4))

(def retryex (Error. "RETRY"))

(defn retryex? [e] (= "RETRY" (.getMessage e)))

(defn init-ref
  ([client-handle ref-node]
   (let [client (.getClient client-handle)]
     (zk/create-all client (str ref-node cfg/HISTORY) :persistent? true)
     (zk/create client (str ref-node cfg/TXN) :persistent? true))))

(defn reset-ref
  ([client-handle ref-node]
   (let [client (.getClient client-handle)]
     (zk/delete-all client ref-node)
     (init-ref client ref-node))))

(defn extract-point [path]
  (subs path (- (count path) 12) (count path)))

(defn next-point
  ([client-handle]
     (extract-point
      (zk/create (.getClient client-handle)
                 (str cfg/*stm-node* cfg/HISTORY cfg/NODE-DELIM cfg/PT-PREFIX)
                 :persistent? true
                 :sequential? true))))

(defn parse-version [version]
  (when version
    (let [[_ txid _ commit-pt] (s/split version #"-")]
      [(str cfg/PT-PREFIX txid) (str cfg/PT-PREFIX commit-pt)])))

(defn point-node [point]
  (str cfg/*stm-node* cfg/HISTORY cfg/NODE-DELIM point))

(defn update-state
  ([client-handle point new-state]
     (zk/set-data (.getClient client-handle) (point-node point) new-state -1))
  ([client-handle point old-state new-state]
     (zk/compare-and-set-data (.getClient client-handle) (point-node point) old-state new-state)))

(defn update-txn-state
  ([txn new-state]
     (zk/set-data (.getClient (.client-handle txn)) (point-node (deref (.txid txn))) new-state -1)
     (reset! (.state txn) new-state))
  ([txn old-state new-state]
     (when (zk/compare-and-set-data (.getClient (.client-handle txn)) (point-node (deref (.txid txn))) old-state new-state)
       (reset! (.state txn) new-state))))

(defn get-history [client-handle ref-name]
  (zk/children (.getClient client-handle) (str ref-name cfg/HISTORY)))

(defn state= [s1 s2]
  (Arrays/equals s1 s2))

(defn current-state? [client-handle txid & states]
  (when txid
    (try
      (let [state (:data (zk/data (.getClient client-handle) (point-node txid)))]
        (reduce #(or %1 (state= state %2)) false states))
      (catch KeeperException$NoNodeException e nil))))

(defn get-last-committed-point
  "Gets the last committed point for the given Ref."
  ([client-handle ref-name]
     (let [history (util/sort-sequential-nodes > (get-history client-handle ref-name))]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (parse-version h)]
           (if (current-state? client-handle txid COMMITTED)
             h
             (recur hs)))))))

(defn trim-ref-history [ref current-point history-to-remove]
  (let [point (util/extract-id current-point)]
    (when (and (zero? (mod point cfg/REF-GC-INTERVAL)) (pos? point))
      (doseq [h history-to-remove]
        (zk/delete (.getClient (.client-handle ref)) (str (.getName ref) cfg/HISTORY cfg/NODE-DELIM h) :async? true)
        (.deleteStateAt (.refState ref) h)))))

(defn trim-stm-history [client-handle current-point]
  (let [point (util/extract-id current-point)]
    (when (and (zero? (mod point cfg/STM-GC-INTERVAL)) (pos? point))
      (future
        (try
          (let [client (.getClient client-handle)
                points-to-keep (into #{} (map #(first (parse-version %))
                                              (mapcat #(zk/children client (str cfg/*stm-node* cfg/REFS cfg/NODE-DELIM % cfg/HISTORY))
                                                      (zk/children client (str cfg/*stm-node* cfg/REFS)))))
                history (drop-last cfg/MAX-STM-HISTORY
                                   (util/sort-sequential-nodes
                                    (zk/children client (str cfg/*stm-node* cfg/HISTORY))))]
            (loop [[h & hs] history]
              (when h
                (when-not (or (contains? points-to-keep h)
                              (current-state? client h RUNNING COMMITTING RETRY))
                  (zk/delete client (str cfg/*stm-node* cfg/HISTORY cfg/NODE-DELIM h)))
                (recur hs))))
          (catch Throwable e (println "trim-stm-history failed with exception: " e (.printlnStackTrace e))))))))

(defn get-committed-point-before
  "Gets the committed point before the given one."
  ([ref point]
     (let [history (util/sort-sequential-nodes > (get-history (.client-handle ref) (.getName ref)))
           point-int (util/extract-id point)]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (parse-version h)]
           (if (and (<= (util/extract-id commit-pt) point-int)
                    (current-state? (.client-handle ref) txid COMMITTED))
             (do (trim-ref-history ref point hs)
                 h)
             (recur hs)))))))

(defn behind-committing-point?
  "Returns true when the given txn's read-point is behind either a committed or committing point."
  ([ref point]
     (when-let [history (util/sort-sequential-nodes > (get-history (.client-handle ref) (.getName ref)))]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (parse-version h)]
           (if (current-state? (.client-handle ref) txid COMMITTING COMMITTED)
             (> (util/extract-id commit-pt) (util/extract-id point))
             (recur hs)))))))

(defn write-commit-point [txn ref]
  (zk/create (.getClient (.client-handle txn)) (str (.getName ref) cfg/HISTORY cfg/NODE-DELIM (deref (.txid txn)) "-" (deref (.commitPoint txn)))
             :persistent? true))

(defn trigger-watches
  [txn]
  (doseq [r (keys (deref (.values txn)))]
    (zk/set-data (.getClient (.client-handle txn)) (.getName r) (data/to-bytes 0) -1)))

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
     cfg/BARGE-WAIT-NANOS))

(defn barge [txn tagged-txid]
  (and (barge-time-elapsed? txn)
       (or (< (util/extract-id (deref (.txid txn))) (util/extract-id tagged-txid))
           (> (deref (.retryCount txn)) cfg/LAST-CHANCE-BARGE-RETRY))
       (update-state (.client-handle txn) tagged-txid RUNNING KILLED)))

(defn block-and-bail [txn]
  (.await (deref (.latch txn)) cfg/BLOCK-WAIT-MSEC TimeUnit/MILLISECONDS)
  (throw retryex))

(defn tagged? [client-handle ref-node]
  "Returns the txid of the running transaction that tagged this ref, otherwise it returns nil"
  (when-let [txid (first (zk/children (.getClient client-handle) (str ref-node cfg/TXN)))]
    (when (current-state? client-handle txid RUNNING COMMITTING)
      txid)))

(defn write-tag [txn ref]
  (if (behind-committing-point? ref (deref (.readPoint txn)))
    (block-and-bail txn)
    (do (zk/delete-children (.getClient (.client-handle txn)) (str (.getName ref) cfg/TXN))
        (zk/create (.getClient (.client-handle txn)) (str (.getName ref) cfg/TXN cfg/NODE-DELIM (deref (.txid txn)))
                   :persistent? false))))

(defn sync-ref [ref]
  (.sync (.getClient (.client-handle ref)) (str cfg/*stm-node* cfg/HISTORY) nil nil)
  (.sync (.getClient (.client-handle ref)) (str (.getName ref) cfg/HISTORY) nil nil))

(defn try-tag [txn ref]
  (sync-ref ref)
  (locks/if-lock-with-timeout (.writeLock (.lock ref)) cfg/LOCK-WAIT-MSEC TimeUnit/MILLISECONDS
    (do
      (when-let [tagged-txid (tagged? (.client-handle txn) (.getName ref))]
        (when (and (not= (deref (.txid txn)) tagged-txid)
                   (not (barge txn tagged-txid)))
          (block-and-bail txn)))
      (write-tag txn ref))
    (block-and-bail txn)))

(defn update-values [txn]
  (let [values (deref (.values txn))]
   (doseq [r (keys values)]
     (write-commit-point txn r)
     (let [ref-state (.refState r)
           point (str (deref (.txid txn)) "-" (deref (.commitPoint txn)))]
       (.setStateAt (.refState r) (get values r) point)))))

(defn reincarnate-txn [txn]
  (reset! (.txid txn) (next-point (.client-handle txn)))
  (update-txn-state txn RETRY))

(defn stop [txn]
  (when-not (current-state? (.client-handle txn) (deref (.txid txn)) COMMITTED)
    (try
      (if (deref (.txid txn))
        (update-txn-state txn RETRY)
        (reincarnate-txn txn))
      (catch KeeperException$NoNodeException e
        (reincarnate-txn txn)))
    (reset! (.values txn) {})
    (reset! (.sets txn) #{})
    (.clear (.commutes txn))
    (.countDown (deref (.latch txn)))))

(defn running? [txn]
  (when (deref (.txid txn))
    (current-state? (.client-handle txn) (deref (.txid txn)) RUNNING COMMITTING)))

(defn invalidate-cache-and-retry [txn ref]
  (.invalidateCache ref)
  (block-and-bail txn))

(defn update-caches [txn]
  (let [values (deref (.values txn))]
   (doseq [r (keys values)]
     (.setCacheAt r (get values r) (deref (.commitPoint txn))))))

(deftype LockingTransaction [returnValue client-handle txid startPoint
                             startTime readPoint commitPoint
                             values sets commutes ensures latch
                             retryCount state]
  Transaction

  (doGet [this ref]
    (if (running? this)
      (or (get @values ref)
          (do (sync-ref ref)
              (let [commit-point (get-committed-point-before ref @readPoint)]
                (if (behind-committing-point? ref commit-point)
                  (block-and-bail this)
                  (when commit-point
                    (if-let [v (and cfg/*use-cache* (= commit-point (.cachedVersion ref)) (.getCache ref))]
                      v
                      (.setCacheAt ref (.getStateAt (.refState ref) commit-point) commit-point)))))))
      (block-and-bail this)))

  (doSet [this ref value]
    (if (running? this)
      (do
        (when-not (contains? @sets ref)
          (try-tag this ref)
          (swap! sets conj ref))
        (swap! values assoc ref value)
        value)
      (block-and-bail this)))

  (doCommute [this f args]
    ;; TODO
    )

  (doEnsure [this]
    ;; TODO
    )

  (runInTransaction [this f]
    (loop [retry-count 0]
      (if (< retry-count cfg/RETRY-LIMIT)
        (do
          (try
            (reset! retryCount retry-count)
            (reset! readPoint (next-point client-handle))
            (reset! latch (CountDownLatch. 1))
            (when (zero? retry-count)
              (reset! txid @readPoint)
              (reset! startPoint @readPoint)
              (reset! startTime (System/nanoTime)))
            (update-txn-state this RUNNING)
            (reset! returnValue (f))
            (when (update-txn-state this RUNNING COMMITTING)
              ;(process-commutes this) ;; TODO
              (validate-values this)
              (reset! commitPoint (next-point client-handle))
              (update-values this)
              (trigger-watches this)
              (update-txn-state this COMMITTING COMMITTED)
              (trim-stm-history client-handle @commitPoint)
              (update-caches this))
            (catch Error e (when-not (retryex? e) (throw e)))
            (finally (stop this)))
          (when-not (current-state? client-handle @txid COMMITTED)
            (recur (inc retry-count))))
        (throw (RuntimeException. "Transaction failed after reaching retry limit"))))
    @returnValue))

(def local-transaction (ThreadLocal.))

(defn create-local-transaction [client-handle]
  (.set local-transaction
        (LockingTransaction. (atom nil)         ;; returnValue
                             client-handle      ;; client-handle
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
                             (atom 0)           ;; retry-count
                             (atom nil)         ;; state
                             )))

(defn get-local-transaction [client-handle]
  (or (.get local-transaction)
      (do (create-local-transaction client-handle)
          (.get local-transaction))))

(defn run-in-transaction [client-handle f]
  (try
    (.runInTransaction (get-local-transaction client-handle) f)
    (catch Throwable e (println "run-in-transaction exception: " e (.printStackTrace e)))))

