(ns avout.atom
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.locks :as locks])
  (:import (org.apache.zookeeper KeeperException$BadVersionException)
           (clojure.lang IDeref)))

;; PROTOCOLS

(defprotocol AtomData
  (nodeName [this] "Returns the ZooKeeper node name associated with this AtomData.")
  (getVersionedValue [this] "Returns a map containing the :value and a :version
    which may just be the current value or a version number and is used in
    compareAndSet to determine if an update should occur.")
  (compareAndSetValue [this new-value expected-version])
  (resetValue [this new-value]))

(defprotocol AtomReference
  (swap [this f] [this f args])
  (reset [this new-value]))

(deftype ZKAtomReference [client atomData lock]
  IDeref
  (deref [this]
    (locks/with-lock (.readLock lock)
      (:value (.getVersionedValue atomData))))
  AtomReference
  (swap [this f] (.swap this f nil))
  (swap [this f args]
    (locks/with-lock (.writeLock lock)
      (let [{:keys [value version]} (.getVersionedValue atomData)
            new-value (apply f value args)]
        (when (.compareAndSetValue atomData new-value version)
          new-value))))
  (reset [this new-value]
    (locks/with-lock (.writeLock lock)
      (when (.resetValue atomData new-value)
        new-value))))

;; Versions of Clojure's Atom functions for use with AtomReferences

(defn !swap
  "Cannot use standard swap! because Clojure expects a clojure.lang.Atom."
  ([atom f & args] (.swap atom f args)))

(defn !reset
  "Cannot use standard reset! because Clojure expects a clojure.lang.Atom."
  ([atom new-value] (.reset atom new-value)))

;; Default implementation of AtomReference that uses ZooKeeper has the data value container.

(defn serialize-form
  "Serializes a Clojure form to a byte-array."
  ([form]
     (data/to-bytes (pr-str form))))

(defn deserialize-form
  "Deserializes a byte-array to a Clojure form."
  ([form]
     (read-string (data/to-string form))))


(deftype ZKAtomData [client name]
  AtomData
  (nodeName [this] name)
  (getVersionedValue [this]
    (let [{:keys [data stat]} (zk/data client name)]
      {:value (deserialize-form data), :version (:version stat)}))
  (compareAndSetValue [this new-value current-version]
    (try
      (zk/set-data client name (serialize-form new-value) current-version)
      (catch KeeperException$BadVersionException e
        (compareAndSetValue this new-value current-version)
        )))
  (resetValue [this new-value] (zk/set-data client name (serialize-form new-value) -1)))

(defn zk-atom
  ([client name]
     (zk-atom client name nil))
  ([client name init-value]
     (zk/create client name :persistent? true)
     (doto (ZKAtomReference. client (ZKAtomData. client name) (locks/distributed-read-write-lock client :lock-node (str name "/lock")))
       (.reset init-value))))

