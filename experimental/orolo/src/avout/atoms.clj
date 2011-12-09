(ns avout.atoms
  (:use avout.state)
  (:import clojure.lang.IRef))

(defn validate [validator value]
  (when (and validator (not (validator value)))
    (throw (IllegalStateException. "Invalid reference state"))))

(defprotocol AtomReference
  "The mutation methods used by the clojure.lang.Atom class."
  (swap [this f] [this f args])
  (reset [this new-value])
  (compareAndSet [this old-value new-value]))

(deftype DistributedAtom [client domainName nodeName atomState validator]
  Identity
  (init [this]
    (.initStateContainer atomState))

  (getName [this] nodeName)

  (destroy [this]
    (.destroyStateContainer atomState))

  avout.atoms.AtomReference

  (compareAndSet [this oldValue newValue]
    (validate @validator newValue)
    (.compareAndSwap atomState oldValue newValue))

  (swap [this f] (.swap this f nil))

  (swap [this f args]
    (let [MAX-RETRY-COUNT 100
          BACKOFF-BASE 1
          BACKOFF-FACTOR 2.5
          BACKOFF-MAX 1000]
      (loop [i 0, backoff BACKOFF-BASE]
        (let [old-value (.getState atomState)
              new-value (apply f old-value args)]
          (validate @validator new-value)
          (if (.compareAndSwap atomState old-value new-value)
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
    (validate @validator new-value)
    (.setState atomState new-value)
    new-value)

  IRef
  (deref [this]
    (.getState atomState))

  (addWatch [this key callback]
    (throw (UnsupportedOperationException.)))

  (getWatches [this]
    (throw (UnsupportedOperationException.)))

  (removeWatch [this key]
    (throw (UnsupportedOperationException.)))

  (setValidator [this f] (reset! validator f))

  (getValidator [this] @validator))

(defn datom
  ([name initializer config]
     (initializer name config))
  ([name init-value initializer config]
     (initializer name init-value config)))