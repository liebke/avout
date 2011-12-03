(ns avout.atoms.sdb
  (:use avout.state)
  (:require [avout.sdb :as sdb]
            [avout.atoms :as atoms])
  (:import clojure.lang.IRef))

(deftype SDBStateContainer [client domain-name name]

  StateContainer

  (initStateContainer [this])

  (destroyStateContainer [this]
    (sdb/delete-attributes client domain-name name [{:name "value"}]))

  (getState [this]
    (let [data (sdb/get-attributes client domain-name name)]
      (if (get data "value")
          (read-string (get data "value"))
          (throw (RuntimeException. "Atom value unbound")))))

  (setState [this new-value]
    (sdb/put-attributes client domain-name name [{:name "value" :value (pr-str new-value)}])))

(defn zk-sdb-atom
  ([zk-client sdb-client domain-name name init-value & {:keys [validator]}]
     (doto (atoms/distributed-atom zk-client name (SDBStateContainer. sdb-client domain-name name))
       (set-validator! validator)
       (.reset init-value)))
  ([zk-client sdb-client domain-name name]
     (atoms/distributed-atom zk-client name (SDBStateContainer. sdb-client domain-name name))))

;;;;;;;;;

(deftype SDBDistributedAtom [client domainName nodeName atomState validator]
  Identity
  (init [this]
    (.initStateContainer atomState))

  (getName [this] nodeName)

  (destroy [this]
    (.destroyStateContainer atomState))

  avout.atoms.AtomReference
  (compareAndSet [this old-value new-value]
    (atoms/validate @validator new-value)
    (sdb/put-attributes client domainName nodeName
                        [{:name "value" :value (pr-str new-value)}]
                        {:name "value" :value (pr-str old-value)}))

  (swap [this f] (.swap this f nil))

  (swap [this f args]
    (let [MAX-RETRY-COUNT 100]
      (loop [i 0]
        (let [old-value (.getState atomState)
              new-value (apply f old-value args)]
          (atoms/validate @validator new-value)
          (if (.compareAndSet this old-value new-value)
            new-value
            (if (< i MAX-RETRY-COUNT)
              (recur (inc i))
              (throw (RuntimeException. "Reached maximum number of retries: " MAX-RETRY-COUNT))))))))

  (reset [this new-value]
    (atoms/validate @validator new-value)
    (.setState atomState new-value)
    new-value)

  IRef
  (deref [this]
    (.getState atomState))

  (addWatch [this key callback]
    (throw (UnsupportedOperationException.)))

  (getWatches [this]
    (throw (UnsupportedOperationException.)))

  (removeWatch [this key] (throw (UnsupportedOperationException.)))

  (setValidator [this f] (reset! validator f))

  (getValidator [this] @validator))

(defn sdb-atom
  ([sdb-client domain-name name init-value & {:keys [validator]}]
     (doto (SDBDistributedAtom. sdb-client domain-name name
                                (SDBStateContainer. sdb-client domain-name name)
                                (atom validator))
       (.reset init-value)))
  ([sdb-client domain-name name]
     (SDBDistributedAtom. sdb-client domain-name name
                          (SDBStateContainer. sdb-client domain-name name)
                          (atom nil))))