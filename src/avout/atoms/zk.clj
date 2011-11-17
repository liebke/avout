(ns avout.atoms.zk
  (:use avout.state)
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.util :as util]))

(deftype ZKStateContainer [client dataNode]

  StateContainer

  (initStateContainer [this]
    (zk/create-all client dataNode))

  (destroyStateContainer [this]
    (zk/delete-all client dataNode))

  (getState [this]
    (println "ZKAtomState getState called " dataNode)
    (let [{:keys [data stat]} (zk/data client dataNode)]
      (util/deserialize-form data)))

  (setState [this new-value] (zk/set-data client dataNode (util/serialize-form new-value) -1)))

