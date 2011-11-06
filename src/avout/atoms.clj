(ns avout.atoms
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.locks :as locks])
  (:import (clojure.lang IRef)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PROTOCOLS

(defprotocol AtomState
  "Protocol to implement when creating new types of distributed atoms."
  (getState [this])
  (setState [this value]))

(defprotocol AtomReference
  "The mutation methods used by the clojure.lang.Atom class."
  (swap [this f] [this f args])
  (reset [this new-value])
  (compareAndSet [this old-value new-value]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DistributedAtom implementation

(defn trigger-watchers
  [client node-name]
  (zk/set-data client node-name (data/to-bytes 0) -1))

(defn validate [validator value]
  (when (and validator (not (validator value)))
    (throw (IllegalStateException. "Invalid reference state"))))

(deftype DistributedAtom [client nodeName atomState validator watches lock]
  AtomReference
  (compareAndSet [this old-value new-value]
    (validate @validator new-value)
    (locks/with-lock (.writeLock lock)
      (if (= old-value (.getState atomState))
        (do (.setState atomState new-value)
            (trigger-watchers client nodeName)
            true)
        false)))

  (swap [this f] (.swap this f nil))

  (swap [this f args]
    (locks/with-lock (.writeLock lock)
      (let [new-value (apply f (.getState atomState) args)]
        (validate @validator new-value)
        (.setState atomState new-value)
        (trigger-watchers client nodeName)
        new-value)))

  (reset [this new-value]
    (locks/with-lock (.writeLock lock)
      (validate @validator new-value)
      (.setState atomState new-value)
      (trigger-watchers client nodeName)
      new-value))

  IRef
  (deref [this] (.getState atomState))

  ;; callback params: akey, aref, old-val, new-val, but old-val will always be nil
  (addWatch [this key callback]
    (let [watcher (fn watcher-fn [event]
                    (when (contains? @watches key)
                      (when (= :NodeStateChanged (:event-type event))
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

(defn distributed-atom [client name atom-data & {:keys [validator]}]
  (zk/create client name :persistent? true)
  (DistributedAtom. client name atom-data
                    (atom validator) (atom {})
                    (locks/distributed-read-write-lock client :lock-node (str name "/lock"))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Versions of Clojure's Atom functions swap!, reset!, compare-and-set! for use with AtomReferences
;; Built-in Clojure functions that work against IRef work with AtomReferences, including
;; deref, the @ deref reader-macro, set-validator!, get-validator!, add-watch, and remove-watch

(defn swap!!
  "Cannot use standard swap! because Clojure expects a clojure.lang.Atom."
  ([^avout.atoms.AtomReference atom f & args] (.swap atom f args)))

(defn reset!!
  "Cannot use standard reset! because Clojure expects a clojure.lang.Atom."
  ([^avout.atoms.AtomReference atom new-value] (.reset atom new-value)))

(defn compare-and-set!!
  "Cannot use standard reset! because Clojure expects a clojure.lang.Atom."
  ([^avout.atoms.AtomReference atom old-value new-value] (.compareAndSet atom old-value new-value)))

