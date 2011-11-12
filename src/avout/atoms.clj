(ns avout.atoms
  (:use avout.state)
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.locks :as locks])
  (:import (clojure.lang IRef)))

;; atom protocols

(defprotocol AtomReference
  "The mutation methods used by the clojure.lang.Atom class."
  (swap [this f] [this f args])
  (reset [this new-value])
  (compareAndSet [this old-value new-value]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DistributedAtom implementation

(def DELIM "/")
(def LOCK "/lock")

(defn trigger-watchers
  [client node-name]
  (zk/set-data client node-name (data/to-bytes 0) -1))

(defn validate [validator value]
  (when (and validator (not (validator value)))
    (throw (IllegalStateException. "Invalid reference state"))))

(defn set-state [atom value]
  ;; trigger cache invalidation watchers
  (.setState (.atomState atom) (.setCache atom value)))

(deftype DistributedAtom [client nodeName atomState cache validator watches lock]
  Identity
  (init [this]
    (zk/create-all client nodeName :persistent? true)
    (.invalidateCache this)
    (.init atomState))

  (getName [this] nodeName)

  (destroy [this]
    (.destroy atomState))

  AtomReference
  (compareAndSet [this old-value new-value]
    (validate @validator new-value)
    (locks/with-lock (.writeLock lock)
      (if (= old-value (or (.getCache this) (.setCache this (.getState atomState))))
        (do (set-state this new-value)
            (trigger-watchers client nodeName)
            true)
        false)))

  (swap [this f] (.swap this f nil))

  (swap [this f args]
    (locks/with-lock (.writeLock lock)
      (let [new-value (apply f (or (.getCache this) (.setCache this (.getState atomState))) args)]
        (validate @validator new-value)
        (set-state this new-value)
        (trigger-watchers client nodeName)
        new-value)))

  (reset [this new-value]
    (locks/with-lock (.writeLock lock)
      (validate @validator new-value)
      (set-state this new-value)
      (trigger-watchers client nodeName)
      new-value))

  IRef
  (deref [this]
    (or (.getCache this)
        (.setCache this (.getState atomState))))

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

  (getValidator [this] @validator)


  StateCache
  (getCache [this]
    (when (:valid @cache) (:value @cache)))

  (setCache [this value]
    (reset! cache {:valid true, :value value})
    value)

  (invalidateCache [this]
    (zk/exists (.client this) (.getName this)
               :watcher (fn [event] (.invalidateCache this)))
    (swap! cache assoc :valid false)))

(defn distributed-atom [client name atom-data & {:keys [validator]}]
  (doto (DistributedAtom. client
                          name
                          atom-data
                          (atom {}) ;; cache
                          (atom validator)
                          (atom {})
                          (locks/distributed-read-write-lock client :lock-node (str name "/lock")))
    .init))
