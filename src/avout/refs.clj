(ns avout.refs
  (:require [zookeeper :as zk]
            [avout.locks :as locks]
            [avout.transaction :as tx])
  (:import (clojure.lang IRef)))

;; Distributed versions of Clojure's standard Ref functions

(defmacro dosync!!
  "Distributed version of Clojure's dosync macro."
  ([client & body]
     `(do (tx/create-local-transaction ~client)
          (tx/run-in-transaction ~client (fn [] ~@body)))))

(defn ref-set!!
  "Distributed version of Clojure's ref-set function."
  ([ref value] (.setRef ref value)))

(defn alter!!
  "Distributed version of Clojure's alter function."
  ([ref f & args] (.alterRef ref f args)))


;; protocols

(defprotocol ReferenceState
  (getRefName [this] "Returns the ZooKeeper node name associated with this reference.")
  (setState [this value point] "Sets the transaction-value associated with the given clock point.")
  (getState [this point] "Returns the value associated with given clock point."))

(defprotocol TransactionReference
  (getName [this])
  (setRef [this value])
  (alterRef [this f args])
  (commuteRef [this f args])
  (ensureRef [this]))


;; distributed reference implementation

(deftype DistributedReference [client nodeName refState validator watches lock]
  TransactionReference
  (getName [this] nodeName)

  (setRef [this value]
    (let [t (tx/get-local-transaction client)]
      (if (tx/running? t)
        (.doSet t this value)
        (throw (RuntimeException. "Must run set-ref!! within the txn macro")))))

  (alterRef [this f args]
    (let [t (tx/get-local-transaction client)]
      (if (tx/running? t)
        (.doSet t this (apply f (.doGet t this) args))
        (throw (RuntimeException. "Must run set-ref!! within the txn macro")))))

  (commuteRef [this f args] (throw (UnsupportedOperationException.)))

  (ensureRef [this] (throw (UnsupportedOperationException.)))

  IRef
  (deref [this]
    (let [t (tx/get-local-transaction client)]
      (if (tx/running? t)
        (.doGet t this)
        (.getState refState (tx/get-last-committed-point client (.getName this))))))

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
  (tx/init-ref client name)
  (DistributedReference. client name ref-state
                         (atom validator) (atom {})
                         (locks/distributed-read-write-lock client :lock-node (str name "/lock"))))

