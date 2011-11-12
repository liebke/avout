(ns avout.atoms.zk
  (:use avout.atoms)
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.util :as util]))

(deftype ZKAtomState [client dataNode]
  AtomState
  (initState [this]
    (zk/create-all client dataNode))

  (destroyState [this]
    (zk/delete-all client dataNode))

  (getState [this]
    (println "ZKAtomState getState called " dataNode)
    (let [{:keys [data stat]} (zk/data client dataNode)]
      (util/deserialize-form data)))

  (setState [this new-value] (zk/set-data client dataNode (util/serialize-form new-value) -1)))

