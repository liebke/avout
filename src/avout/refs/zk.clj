(ns avout.refs.zk
  (:use avout.state)
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.config :as cfg]
            [avout.util :as util]
            [avout.transaction :as tx]))

;; ZK data implementation

(deftype ZKVersionedStateContainer [client name]

  VersionedStateContainer

  (initVersionedStateContainer [this] nil)

  (destroyVersionedStateContainer [this] nil)

  (getStateAt [this version]
    (try
      (let [{:keys [data stat]} (zk/data client (str name cfg/HISTORY cfg/NODE-DELIM version))]
        (util/deserialize-form data))
      ;; in the rare event that the requested value has already been GCed, throw retryex
      (catch org.apache.zookeeper.KeeperException$NoNodeException e (throw tx/retryex))))

  (setStateAt [this value version]
    (zk/set-data client (str name cfg/HISTORY cfg/NODE-DELIM version) (util/serialize-form value) -1))

  (deleteStateAt [this version]
    ;; This method doesn't need to clean up, because Avout's GC cleans
    ;; up the data as a side effect of trimming the Ref's history.
    nil))

