(ns avout.sdb.atom
  (:use avout.state)
  (:require [avout.sdb :as sdb]
            [avout.atoms :as atoms])
  (:import clojure.lang.IRef))

(deftype SDBStateContainer [client domainName name]

  StateContainer

  (initStateContainer [this]
    (when-not (seq (sdb/get-attributes client domainName name))
      (sdb/put-attributes client domainName name [{:name "value" :value (pr-str nil)}])))

  (destroyStateContainer [this]
    (sdb/delete-attributes client domainName name [{:name "value"}]))

  (getState [this]
    (let [data (sdb/get-attributes client domainName name)]
      (if (contains? data "value")
          (read-string (get data "value"))
          (throw (RuntimeException. "sdb-atom unbound")))))

  (setState [this new-value]
    (sdb/put-attributes client domainName name [{:name "value" :value (pr-str new-value)}])))

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
    (let [MAX-RETRY-COUNT 100
          BACKOFF-BASE 1
          BACKOFF-FACTOR 2.5
          BACKOFF-MAX 1000]
      (loop [i 0, backoff BACKOFF-BASE]
        (let [old-value (.getState atomState)
              new-value (apply f old-value args)]
          (atoms/validate @validator new-value)
          (if (.compareAndSet this old-value new-value)
            new-value
            (if (< i MAX-RETRY-COUNT)
              (do (loop [spin 0]
                    (when (< spin backoff)
                      (recur (inc spin))))
                  (recur (inc i)
                         (let [b (* backoff BACKOFF-FACTOR)]
                           (if (< b BACKOFF-MAX) b BACKOFF-MAX))))
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
       .init
       (.reset init-value)))
  ([sdb-client domain-name name]
     (doto (SDBDistributedAtom. sdb-client domain-name name
                                (SDBStateContainer. sdb-client domain-name name)
                                (atom nil))
       .init)))