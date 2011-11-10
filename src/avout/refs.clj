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
  (initState [this] "Used to initialize the reference state container if necessary")
  (getRefName [this] "Returns the ZooKeeper node name associated with this reference.")
  (setState [this value point] "Sets the transaction-value associated with the given clock point.")
  (getState [this point] "Returns the value associated with given clock point.")
  (committed [this point] "A callback notification, to let the ReferenceState know that the point has been committed")
  (destroyState [this] "Used to destroy all the reference state associated with the instance."))

(defprotocol TransactionReference
  (initRef [this])
  (getName [this])
  (setRef [this value])
  (alterRef [this f args])
  (commuteRef [this f args])
  (ensureRef [this])
  (getCache [this point])
  (setCache [this point value])
  (destroyRef [this]))


;; distributed reference implementation

(deftype DistributedReference [client nodeName refState cache validator watches lock]
  TransactionReference
  (initRef [this]
    (tx/init-ref client nodeName)
    (.initState refState))

  (destroyRef [this]
    (zk/delete-all client nodeName)
    (.destroyState refState))

  (getName [this] nodeName)

  (setRef [this value]
    (let [t (tx/get-local-transaction client)]
      (if (tx/running? t)
        (.doSet t this value)
        (throw (RuntimeException. "Must run set-ref!! within the dosync!! macro")))))

  (alterRef [this f args]
    (let [t (tx/get-local-transaction client)]
      (if (tx/running? t)
        (.doSet t this (apply f (.doGet t this) args))
        (throw (RuntimeException. "Must run set-ref!! within the dosync!! macro")))))

  (commuteRef [this f args] (throw (UnsupportedOperationException.)))

  (ensureRef [this] (throw (UnsupportedOperationException.)))

  (getCache [this point]
    (get point @cache))

  (setCache [this point value]
    (reset! cache {point value})
    value)


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
  (doto (DistributedReference. client
                               name
                               ref-state
                               (atom {}) ;; cache
                               (atom validator)
                               (atom {}) ;; watchers
                               (locks/distributed-read-write-lock client :lock-node (str name "/lock")))
    .initRef))

