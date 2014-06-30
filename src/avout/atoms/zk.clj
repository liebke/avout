(ns avout.atoms.zk
  (:use avout.state)
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.util :as util]))

(deftype ZKStateContainer [client-handle dataNode]

  StateContainer

  (initStateContainer [this]
    (zk/create-all (.getClient client-handle) dataNode :persistent? true))

  (destroyStateContainer [this]
    (zk/delete-all (.getClient client-handle) dataNode))

  (getState [this]
    (let [{:keys [data stat]} (zk/data (.getClient client-handle) dataNode)]
      (util/deserialize-form data)))

  (setState [this new-value]
    (zk/set-data (.getClient client-handle)
                 dataNode
                 (util/serialize-form new-value) -1)))

