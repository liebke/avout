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
  (alterRef [this f args])
  (commuteRef [this f args])
  (ensureRef [this]))

;; implementation

(def local-transaction (ThreadLocal.))

(def ^:dynamic *stm-node* "/stm")

(def RETRY-LIMIT 25)
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
     (println "update-state: " point (data/to-long new-state))
     (zk/set-data client (point-node point) new-state -1))
  ([client point old-state new-state]
     (zk/compare-and-set-data client (point-node point) old-state new-state)))

(defn get-history [client ref-name]
  (zk/children client (str ref-name "/history")))

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

(defn behind-committing-point?
  "Returns true when the given txn's read-point is behind either a committed or committing point."
  ([ref point]
     (when-let [history (util/sort-sequential-nodes > (get-history (.client ref) (.getName ref)))]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (split-ref-commit-history h)]
           (if (current-state? (.client ref) txid COMMITTING COMMITTED)
             (do (println "behind-committing-point? point=" point ", commit-pt=" commit-pt ": " (> (util/extract-id commit-pt) (util/extract-id point)))
                 (> (util/extract-id commit-pt) (util/extract-id point)))
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

(defn tagged? [client ref-name]
  "Returns the txid of the running transaction that tagged this ref, otherwise it returns nil"
  (println "tagged? " ref-name)
  (when-let [txid (first (zk/children client (str ref-name "/txn")))]
    (when (current-state? client txid RUNNING COMMITTING)
      (println "tagged?: " ref-name " is tagged by " txid)
      txid)))

(defn tag-ref [txn ref]
  (if (behind-committing-point? ref (deref (.readPoint txn)))
    (throw retryex)
    (do
      (zk/delete-children (.client txn) (str (.getName ref) "/txn"))
      (zk/create (.client txn) (str (.getName ref) "/txn/" (deref (.txid txn))) :persistent? false))))

(defn write-commit-point [txn ref]
  (zk/create (.client txn) (str (.getName ref) "/history/" (deref (.txid txn)) "-" (deref (.commitPoint txn)))
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

(defn process-values [txn]
  (let [values (deref (.values txn))]
   (doseq [r (keys values)]
     (write-commit-point txn r)
     (.setState (.refState r) (get values r) (str (deref (.readPoint txn)) "-" (deref (.commitPoint txn)))))))

(defn barge-time-elapsed? [txn]
  (> (- (System/nanoTime) (deref (.startTime txn)))
     BARGE-WAIT-NANOS))

(defn barge [txn barged-txid]
  ;(println "barge: barge-time-elapsed? " (barge-time-elapsed? txn))
  ;(println "barge: readpoint " (util/extract-id (deref (.txid txn))))
  ;(println "barge: other readpoint "  (util/extract-id barged-txid))
  ;(println "barge: other txn running?" (current-state? (.client txn) barged-txid RUNNING))
  (and (barge-time-elapsed? txn)
       (< (util/extract-id (deref (.txid txn))) (util/extract-id barged-txid))
       ;(update-state (.client txn) barged-txid RUNNING KILLED)
       )
  ;; always fail
  false
  )

(defn block-and-bail [txn]
  (.await (.latch txn) LOCK-WAIT-MSEC TimeUnit/MILLISECONDS)
  (println "block-and-bail")
  (throw retryex))

(defn lock-ref [txn ref]
  (.sync (.client txn) (str "/stm/history") nil nil)
  (.sync (.client txn) (str (.getName ref) "/history") nil nil)
  (locks/with-lock (.writeLock (.lock ref))
    (println "lock-ref: locking ref: " (deref (.txid txn)) (.getName ref) "lock requestNode: " (.requestNode (.writeLock (.lock ref))))
    (println "lock-ref: checking for tag on ref..." (.getName ref))
    (when-let [other-txid (tagged? (.client txn) (.getName ref))]
      (println "lock-ref: tag found, checking its status")
      (when (not= (deref (.txid txn)) other-txid)
        (println "lock-ref: retryex " (deref (.txid txn)) " could not tag ref " (.getName ref) " already tagged by " other-txid)
        (block-and-bail txn)))
    (println "tag-ref: about to tag ref")
    (tag-ref txn ref)))

(defn stop [txn]
  (when-not (current-state? (.client txn) (deref (.txid txn)) COMMITTED)
    (update-state (.client txn) (deref (.txid txn)) RETRY))
  (reset! (.values txn) {})
  (reset! (.sets txn) #{})
  (.clear (.commutes txn))
  (.countDown (.latch txn)))


(deftype LockingTransaction [returnValue client txid startPoint startTime readPoint commitPoint
                             values sets commutes ensures locked latch]

  Transaction

  (doGet [this ref]
    (if (current-state? client @txid RUNNING COMMITTING)
      (or (get @values ref)
          (do (.sync (.client this) (str (.getName ref) "/history") nil nil)
              (let [commit-point (get-committed-point-before client (.getName ref) @readPoint)]
                (if (behind-committing-point? ref commit-point)
                  (do (println "doGet: retryex the current commit point is behind a committing point")
                      (throw retryex))
                  (do (println "doGet: " @txid " prev-commit-point: " (.getName ref) commit-point ", value: " (.getState (.refState ref) commit-point))
                      (.getState (.refState ref) commit-point))))))
      (do (println "doGet: transaction not running")
          (throw retryex))))

  (doSet [this ref value]
    (if (current-state? client @txid RUNNING COMMITTING)
      (do
        (when-not (contains? @sets ref)
          (lock-ref this ref)
          (println "doSet: adding ref to sets: " @txid (.getName ref) value)
          (swap! sets conj ref))
        (println "doSet: adding value to values: " @txid (.getName ref) value)
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
            (reset! readPoint (extract-point (next-point client)))
            (println "loop(" retry-count "): readPoint: " @readPoint)
            (reset! txid @readPoint)
            (when (zero? retry-count)
              (reset! startPoint @readPoint)
              (reset! startTime (System/nanoTime)))
            (update-state client @txid RUNNING)
            ;; f is user defined, and potentially long running
            (reset! returnValue (f))
            ;; once f has successfully run, begin the commit process
            (when (update-state client @txid RUNNING COMMITTING)
              (println "COMMITTING: " @txid)
              ;(process-commutes this) ;; TODO
              (validate-values this)
              (reset! commitPoint (extract-point (next-point client)))
              (process-values this)
              (trigger-watches this)
              (update-state client @txid COMMITTED)
              (println "COMMITTED: " @txid))
            (catch Error e
              (if (retryex? e)
                (println "runInTransaction: Retrying Transaction...")
                (do
                  (println "Exception thrown in runInTransaction: " e)
                  (throw e))))
            (finally
;;             (unlock-refs this)
             (stop this)))
          (when-not (current-state? client @txid COMMITTED)
            (recur (inc retry-count))))
        (throw (RuntimeException. "Transaction failed after reaching retry limit"))))
    @returnValue))

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
                             (atom #{})         ;; locked
                             (CountDownLatch. 1) ;; latch
                             )))

(defn get-local-transaction [client]
  (or
   (.get local-transaction)
   (do (create-local-transaction client)
       (.get local-transaction))))

(defn run-in-transaction [client f]
  (.runInTransaction (get-local-transaction client) f))


;; distributed reference implementation

(defn running? [txn]
  (current-state? (.client txn) (deref (.txid txn)) RUNNING COMMITTING))

(deftype DistributedReference [client nodeName refState validator watches lock]
  TransactionReference
  (getName [this] nodeName)

  (setRef [this value]
    (let [t (get-local-transaction client)]
      (if (running? t)
        (.doSet t this value)
        (throw (RuntimeException. "Must run set-ref from a transaction")))))

  (alterRef [this f args]
    (let [t (get-local-transaction client)]
      (if (running? t)
        (.doSet t this (apply f (.doGet t this) args))
        (throw (RuntimeException. "Must run set-ref from a transaction")))))

  (commuteRef [this f args] (throw (UnsupportedOperationException.)))

  (ensureRef [this] (throw (UnsupportedOperationException.)))

  IRef
  (deref [this]
    (let [t (get-local-transaction client)]
      (if (running? t)
        (.doGet t this)
        (.getState refState (get-last-committed-point client (.getName this))))))

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


;; ref functions

(defmacro txn
  ([client & body]
     `(do (create-local-transaction ~client)
          (run-in-transaction ~client (fn [] ~@body)))))

(defn ref-set!! [ref value]
  (.setRef ref value))

(defn alter!! [ref f & args]
  (.alterRef ref f args))


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
    (let [{:keys [data stat]} (zk/data client (str name "/history/" point))]
      (deserialize-form data)))

  (setState [this value point]
    (println "ZKRefState: setState: " (.getRefName this) value point)
    (zk/set-data client (str name "/history/" point) (serialize-form value) -1)))

(defn zk-ref
  ([client name init-value & {:keys [validator]}]
     (let [r (doto (distributed-ref client name (ZKRefState. client name))
               (set-validator! validator))]
       (txn client (ref-set!! r init-value))
       r))
  ([client name]
     ;; for connecting to an existing ref only
     (distributed-ref client name (ZKRefState. client name))))


(comment

  (use 'avout.refs :reload-all)
  (require '[zookeeper :as zk])

  (def client (zk/connect "127.0.0.1"))
  (def a (zk-ref client "/a" 0))
  (def b (zk-ref client "/b" 0))
  @a
  @b
  (txn client
    (alter!! a inc)
    (alter!! b #(+ @a %)))


  ;; from another repl
  (use 'avout.refs :reload-all)
  (require '[zookeeper :as zk])

  ;; connect to the stm
  (def stm (zk/connect "127.0.0.1"))

  ;; no initial value, connect to an existing distributed ref
  (def a (zk-ref stm "/a"))
  (def b (zk-ref stm "/b"))

  (txn stm
    (alter!! a inc)
    (alter!! b #(+ @a %)))


  ;; concurrency test
  (use 'avout.refs :reload-all)
  (require '[zookeeper :as zk])

  ;; connect to the stm
  (def client (zk/connect "127.0.0.1"))

  (def a (zk-ref client "/aaal" 0))
  (def b (zk-ref client "/baal" 0))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try (txn client
                             (alter!! a inc)
                             (alter!! b inc))
                             (catch Throwable e (.printStackTrace e)))))))


  (def a (zk-ref client "/aaal"))
  (def b (zk-ref client "/baal"))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try (txn client
                             (alter!! a inc)
                             (alter!! b inc))
                             (catch Throwable e (.printStackTrace e)))))))

)