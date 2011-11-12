(ns avout.refs.local
  (:use avout.state)
  (:require [zookeeper :as zk]
            [avout.transaction :as tx]
            [avout.util :as util]))


(deftype LocalRefState [client name state]
  Identity
  (init [this] nil)

  (getName [this] name)

  (destroy [this] (reset! state {}))

  VersionedStateContainer
  (getStateAt [this version]
    (println "RefState getStateAt called " name version)
    (get @state version))

  (setStateAt [this value version]
    (swap! state assoc version value)))

