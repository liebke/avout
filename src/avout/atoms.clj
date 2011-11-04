(ns avout.atoms
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.locks :as locks])
  (:import (clojure.lang IRef)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PROTOCOLS

(defprotocol AtomState
  (getValue [this] "Returns a map containing the :value and a :version
    which may just be the current value or a version number and is used in
    compareAndSet to determine if an update should occur.")
  (setValue [this value]))

(defprotocol AtomReference
  (swap [this f] [this f args])
  (reset [this new-value])
  (compareAndSet [this old-value new-value]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DistributedAtom implementation

(defn trigger-watchers
  [client node-name]
  (zk/set-data client node-name (data/to-bytes 0) -1))

(deftype DistributedAtom [client nodeName atomData validator watches lock]
  AtomReference
  (compareAndSet [this old-value new-value]
    (if (and @validator (not (@validator new-value)))
      (throw (IllegalStateException. "Invalid reference state"))
      (locks/with-lock (.writeLock lock)
        (let [value (.getValue atomData)]
          (and (= old-value value)
               (.setValue atomData new-value))))))

  (swap [this f] (.swap this f nil))

  (swap [this f args]
    (locks/with-lock (.writeLock lock)
      (let [new-value (apply f (.getValue atomData) args)]
        (if (and @validator (not (@validator new-value)))
          (throw (IllegalStateException. "Invalid reference state"))
          (do (.setValue atomData new-value)
              (trigger-watchers client nodeName)
              new-value)))))

  (reset [this new-value]
    (locks/with-lock (.writeLock lock)
      (if (and @validator (not (@validator new-value)))
        (throw (IllegalStateException. "Invalid reference state"))
        (do (.setValue atomData new-value)
            (trigger-watchers client nodeName)
            new-value))))

  IRef
  (deref [this] (.getValue atomData))

  (addWatch [this key callback] ;; callback params: akey, aref, old-val, new-val, but old-val will be nil
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

