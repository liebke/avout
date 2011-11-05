(ns avout.refs
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [zookeeper.util :as util]
            [avout.locks :as locks]
            [clojure.string :as s])
  (:import (clojure.lang IRef)
           (java.util Arrays)))

;; protocols

(defprotocol ReferenceState
  (setValue [this value point] "Sets the transaction-value associated with the given clock point.")
  (getValue [this point] "Returns the value associated with given clock point."))

(defprotocol TransactionReference
  (doSet [this value])
  (doCommute [this f args])
  (doEnsure [this]))

;; implementation

(def ^:dynamic *stm-node* "/stm")

(def RUNNING (data/to-bytes 0))
(def COMMITTING (data/to-bytes 1))
(def RETRY (data/to-bytes 2))
(def KILLED (data/to-bytes 3))
(def COMMITTED (data/to-bytes 4))

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
  ([client ref-name point]
     (let [history (util/sort-sequential-nodes > (get-history client ref-name))]
       (loop [[h & hs] history]
         (when-let [[txid commit-pt] (split-ref-commit-history h)]
           (if (current-state? client txid COMMITTED)
             txid
             (recur hs)))))))

(defn tagged? [client ref-name]
  (when-let [txid (first (zk/children client (str ref-name "/txn")))]
    (and txid (current-state? client txid RUNNING COMMITTING))))

(defn tag [client ref-name txid]
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

(deftype DistributedReference [client nodeName refState validator watches lock]
  TransactionReference
  (doSet [this value] (throw (UnsupportedOperationException.)))

  (doCommute [this f args] (throw (UnsupportedOperationException.)))

  (doEnsure [this] (throw (UnsupportedOperationException.)))

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
  (setValue [this value point] (throw (UnsupportedOperationException.)))

  (getValue [this point] (throw (UnsupportedOperationException.))))

(defn zk-ref
  ([client name init-value & {:keys [validator]}]
     (doto (distributed-ref client name (ZKRefState. client name))
       (set-validator! validator)
       (.reset init-value))))