(ns avout.refs
  (:use avout.state)
  (:require [avout.locks :as locks]
            [avout.transaction :as tx]
            [avout.config :as cfg])
  (:import (clojure.lang IRef)))


;; reference protocols

(defprotocol TransactionReference
  (version [this])
  (setRef [this value])
  (alterRef [this f args])
  (commuteRef [this f args])
  (ensureRef [this]))


(defn txn-running? [txn]
  (= (:state (deref (.txnInfo txn))) :RUNNING))

;; distributed reference implementation

(deftype DistributedReference [stm nodeName refInfo refState cache validator watches lock]
  Identity
  (init [this]
    (tx/init-ref-info stm nodeName)
    (.initVersionedStateContainer refState))

  (destroy [this]
    (.destroyVersionedStateContainer refState))

  (getName [this] nodeName)

  TransactionReference

  (setRef [this value]
    (let [t (tx/get-local-transaction stm)]
      (if (txn-running? t) ; (tx/state= (deref (.state t)) tx/RUNNING)
        (.doSet t this value)
        (throw (RuntimeException. "Must run set-ref!! within the dosync!! macro")))))

  (alterRef [this f args]
    (let [t (tx/get-local-transaction stm)]
      (if (txn-running? t) ; (tx/state= (deref (.state t)) tx/RUNNING)
        (.doSet t this (apply f (.doGet t this) args))
        (throw (RuntimeException. "Must run alter!! within the dosync!! macro")))))

  (commuteRef [this f args] (throw (UnsupportedOperationException.)))

  (ensureRef [this] (throw (UnsupportedOperationException.)))

  (version [this]
    (let [t (tx/get-local-transaction stm)]
      (if (tx/get-local-transaction stm)
        (tx/get-committed-txn-before stm refInfo (:read-point (deref (.txnInfo t))))
        (tx/get-last-committed-txn stm refInfo))))

  StateCache
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
    (let [t (tx/get-local-transaction stm)]
      (if (txn-running? t) ; (tx/get-local-transaction stm)
        (.doGet t this)
        (or (when cfg/*use-cache* (.getCache this))
            (when-let [commit-point (tx/get-last-committed-txn stm refInfo this)]
              (.setCacheAt this (.getStateAt refState commit-point) commit-point))))))

  ;; callback params: akey, aref, old-val, new-val, but old-val will always be nil
  (addWatch [this key callback] (throw (UnsupportedOperationException.)))

  (getWatches [this]  (throw (UnsupportedOperationException.)))

  (removeWatch [this key] (throw (UnsupportedOperationException.)))

  (setValidator [this f] (reset! validator f))

  (getValidator [this] @validator))



(defn distributed-ref [stm name ref-state initializer config & {:keys [validator]}]
  (let [node-name (str cfg/*stm-node* cfg/REFS name)]
    (doto (avout.refs.DistributedReference. stm
                                            node-name
                                            (tx/init-ref-info stm node-name)
                                            ref-state
                                            (atom {}) ;; cache
                                            (atom validator)
                                            (atom {}) ;; watchers
                                            (locks/distributed-leased-lock (str node-name cfg/LOCK) initializer config))
     .init)))

