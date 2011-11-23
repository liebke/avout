(ns avout.refs.local
  (:use avout.state)
  (:require [zookeeper :as zk]
            [avout.transaction :as tx]
            [avout.util :as util]))


(deftype LocalVersionedStateContainer [client name state]

  VersionedStateContainer

  (initVersionedStateContainer [this] nil)

  (destroyVersionedStateContainer [this] (reset! state {}))

  (getStateAt [this version]
    (if (contains? @state version)
      (get @state version)
      ;; in the rare event that the requested value has been GCed, throw retryex
      (throw tx/retryex)))

  (setStateAt [this value version]
    (swap! state assoc version value))

  (deleteStateAt [this version]
    (swap! state dissoc version)))

