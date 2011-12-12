(ns avout.transaction
  (:use avout.core)
  (:require [avout.config :as cfg])
  (:import (java.util UUID TreeMap)
           (java.util.concurrent CountDownLatch)))

(defprotocol Transaction
  (doGet [this ref])
  (doSet [this ref value])
  (doCommute [this f args])
  (doEnsure [this])
  (runInTransaction [this f]))

(def retryex (Error. "RETRY"))

(defn retryex? [e] (= "RETRY" (.getMessage e)))

(defn init-stm [initializer config]
  (when-let [txids (datom "/stm/txns" initializer config)]
    (doseq [t txids] (.destroy (datom (str "/stm/txns/" t) initializer config))))
  (when-let [refs (datom "/stm/refs" initializer config)]
    (doseq [r refs] (.destroy (datom (str "/stm/refs/" r) initializer config))))
  {:clock  (datom "/stm/clock" 0 initializer config)
   :txns (datom "/stm/txns" [] initializer config)
   :refs (datom "/stm/refs" [] initializer config)
   :initializer initializer
   :config config})

(defn get-stm [initializer config]
  {:clock  (datom "/stm/clock" initializer config)
   :txns (datom "/stm/txns" initializer config)
   :refs (datom "/stm/refs" initializer config)
   :initializer initializer
   :config config})

(defn next-point [stm]
  (swap!! (:clock stm) inc))

(defn init-txn-info [stm]
  (let [txid (str (UUID/randomUUID))]
    (datom (str "/stm/txns/" txid)
           {:txid txid
            :state :RUNNING
            :start-point nil
            :commit-point nil}
           (:initializer stm) (:config stm))))

(defn get-txn-info [stm txid]
  (datom (str "/stm/txns/" txid) (:initializer stm) (:config stm)))

(defn init-ref-info
  ([stm name]
     (datom (str "/stm/refs/" name) {:history [], :txid nil} (:initializer stm) (:config stm))))

(defn get-ref-info [stm name]
  (datom (str "/stm/refs/" name) (:initializer stm) (:config stm)))

(defn current-state? [tinfo & states]
  (reduce #(or %1 (= (:state tinfo) %2)) false states))

(defn tagged?
  "Returns the running txn-info datom that has tagged the given Ref, if there is one."
  ([rinfo initializer config]
     (when-let [txid (:txid rinfo)]
       (let [txn-info (get-txn-info txid initializer config)]
         (when (current-state? @txn-info :RUNNING :COMMITTING)
           txn-info)))))

(defn try-tag [ref-info txid initializer config]
  (let [new-info (swap!! ref-info
                         (fn [current-info]
                           (if (tagged? current-info initializer config)
                             current-info
                             (update-in current-info [:txid] identity txid))))]
    (= (:txid new-info) txid)))

(defn update-txn-state
  ([txn-info new-state]
     (swap!! txn-info assoc new-state))
  ([txn-info old-state new-state]
     (let [tinfo (swap!! txn-info update-in [:state] #(if (= old-state %) new-state old-state))]
       (= (:state tinfo) new-state))))

(defn get-last-committed-txn [stm rinfo]
  (let [history (sort-by :commit-point > (:history rinfo))]
    (loop [[h & hs] history]
      (when h
        (if (current-state? (deref (get-txn-info stm (:txid h))) :COMMITTED)
         h
         (recur hs))))))

(defn get-committed-txn-before [stm rinfo read-point]
  (let [history (sort-by :commit-point > (:history rinfo))]
    (loop [[h & hs] history]
      (when h
        (if (and (> read-point (:commit-point h))
                 (current-state? (deref (get-txn-info stm (:txid h))) :COMMITTED))
         h
         (recur hs))))))

(defn behind-committing-txn? [stm rinfo read-point]
  (let [history (sort-by :commit-point > (:history rinfo))]
    (loop [[h & hs] history]
      (when h
        (if (and (< read-point (:commit-point h))
                 (current-state? (deref (get-txn-info stm (:txid h))) :COMMITTING :COMMITTED))
         h
         (recur hs))))))

(defn barge-time-elapsed? [txn]
  (> (- (System/nanoTime) (deref (.startTime txn)))
     cfg/BARGE-WAIT-NANOS))

(defn barge [txn tagged-txn-info]
  (and (barge-time-elapsed? txn)
       (or (< (:start-point (deref (.txnInfo txn))) (:start-point tagged-txn-info))
           (> (deref (.retryCount txn)) cfg/LAST-CHANCE-BARGE-RETRY))
       (update-txn-state tagged-txn-info :RUNNING :KILLED)))

(defn running? [txn]
  (current-state? (.stm txn) (deref (.txnInfo txn)) :RUNNING :COMMITTING))

(defn block-and-bail [txn] nil)

(defn validate-values [txn])

(defn update-values [txn])

(defn trim-stm-history [stm])

(defn stop [txn])

(deftype LockingTransaction [returnValue stm txnInfo startTime readPoint
                             values sets commutes ensures latch retryCount]
  Transaction

  (doGet [this ref]
    (if (running? this)
      (or (get @values ref)
          (let [commit-point (:commit-point (get-committed-txn-before ref @readPoint))]
            (if (behind-committing-txn? ref commit-point)
              (block-and-bail this)
              (when commit-point
                (if-let [v (and cfg/*use-cache* (= commit-point (.cachedVersion ref)) (.getCache ref))]
                  v
                  (.setCacheAt ref (.getStateAt (.refState ref) commit-point) commit-point))))))
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
            (reset! readPoint (next-point stm))
            (reset! latch (CountDownLatch. 1))
            (when (zero? retry-count)
              (swap!! txnInfo assoc :start-point readPoint)
              (reset! startTime (System/nanoTime)))
            (update-txn-state txnInfo :RUNNING)
            (reset! returnValue (f))
            (when (update-txn-state txnInfo :RUNNING :COMMITTING)
              ;(process-commutes this) ;; TODO
              (validate-values this)
              (swap!! txnInfo assoc :commit-point (next-point stm))
              (update-values this)
              ;(trigger-watches this) ;; No watches without notifications
              (update-txn-state this :COMMITTING :COMMITTED)
              (trim-stm-history stm)
              ;(update-caches this)
              )
            (catch Error e (when-not (retryex? e) (throw e)))
            (finally (stop this)))
          (when-not (current-state? stm @txnInfo :COMMITTED)
            (recur (inc retry-count))))
        (throw (RuntimeException. "Transaction failed after reaching retry limit"))))
    @returnValue))

(def local-transaction (ThreadLocal.))

(defn create-local-transaction [stm]
  (.set local-transaction
        (LockingTransaction. (atom nil)                 ;; returnValue
                             stm                        ;; stm ;; need to stop using readpoint as id
                             (init-txn-info stm)        ;; txnInfo
                             (atom nil)                 ;; startTime
                             (atom nil)                 ;; readPoint
                             (atom {})                  ;; values
                             (atom #{})                 ;; sets
                             (TreeMap.)                 ;; commutes
                             (atom #{})                 ;; ensures
                             (atom (CountDownLatch. 1)) ;; latch
                             (atom 0)                   ;; retry-count
                             )))

(defn get-local-transaction [stm]
  (or (.get local-transaction)
      (do (create-local-transaction stm)
          (.get local-transaction))))

(defn run-in-transaction [stm f]
  (try
    (.runInTransaction (get-local-transaction stm) f)
    (catch Throwable e (println "run-in-transaction exception: " e (.printStackTrace e)))))

