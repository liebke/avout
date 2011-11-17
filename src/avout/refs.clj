(ns avout.refs
  (:use avout.state)
  (:require [zookeeper :as zk]
            [avout.locks :as locks]
            [avout.transaction :as tx]
            [avout.config :as cfg])
  (:import (clojure.lang IRef)))


;; reference protocols

(defprotocol TransactionReference
  (version [this])
  (setRef [this value])
  (alterRef [this f args])
  (commuteRef [this f args])
  (ensureRef [this])
  (clearLocks [this]))


(defn txn-running? [txn]
  (tx/state= (deref (.state txn)) tx/RUNNING))

;; distributed reference implementation

(deftype DistributedReference [client nodeName refState cache validator watches lock]
  Identity
  (init [this]
    (tx/init-ref client nodeName)
    (.invalidateCache this) ;; sets initial watch
    (.initVersionedStateContainer refState))

  (destroy [this]
    (zk/delete-all client nodeName)
    (.destroyVersionedStateContainer refState))

  (getName [this] nodeName)

  TransactionReference

  (setRef [this value]
    (let [t (tx/get-local-transaction client)]
      (if (txn-running? t) ; (tx/state= (deref (.state t)) tx/RUNNING)
        (.doSet t this value)
        (throw (RuntimeException. "Must run set-ref!! within the dosync!! macro")))))

  (alterRef [this f args]
    (let [t (tx/get-local-transaction client)]
      (if (txn-running? t) ; (tx/state= (deref (.state t)) tx/RUNNING)
        (.doSet t this (apply f (.doGet t this) args))
        (throw (RuntimeException. "Must run alter!! within the dosync!! macro")))))

  (commuteRef [this f args] (throw (UnsupportedOperationException.)))

  (ensureRef [this] (throw (UnsupportedOperationException.)))

  (version [this]
    (let [t (tx/get-local-transaction client)]
      (if (tx/get-local-transaction client)
        (tx/get-committed-point-before client (.getName this) (.readPoint ref))
        (tx/get-last-committed-point client (.getName this)))))

  (clearLocks [this]
    (zk/delete-children client (str nodeName cfg/TXN))
    (zk/delete-children client (str nodeName cfg/LOCK)))

  StateCache
  (invalidateCache [this]
    (zk/children client (str nodeName cfg/TXN) :watcher (fn [event] (.invalidateCache this)))
    (swap! cache assoc :valid false))

  (getCache [this]
    (when (:valid @cache)
      (:value @cache)))

  (setCacheAt [this value version]
    (reset! cache {:valid true :value value :version version})
    value)

  (cachedVersion [this]
    (:version @cache))

  IRef
  (deref [this]
    (let [t (tx/get-local-transaction client)]
      (if (txn-running? t) ; (tx/get-local-transaction client)
        (.doGet t this)
        (or (when cfg/*use-cache* (.getCache this))
            (when-let [commit-point (tx/get-last-committed-point client (.getName this))]
              (.setCacheAt this (.getStateAt refState commit-point) commit-point))))))

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
  (let [node-name (str cfg/*stm-node* cfg/REFS name)]
    (doto (avout.refs.DistributedReference. client
                                           node-name
                                           ref-state
                                           (atom {}) ;; cache
                                           (atom validator)
                                           (atom {}) ;; watchers
                                           (locks/distributed-read-write-lock client :lock-node (str node-name cfg/LOCK)))
     .init)))

