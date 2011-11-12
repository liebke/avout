(ns avout.refs.local
  (:use avout.refs)
  (:require [zookeeper :as zk]
            [avout.transaction :as tx]
            [avout.util :as util]))


(deftype LocalRefState [client name state]
  ReferenceState
  (initState [this] nil)

  (getRefName [this] name)

  (getState [this point]
    (println "RefState getState called " name point)
    (get @state point))

  (setState [this value point]
    (swap! state assoc point value))

    (destroyState [this] (reset! state {})))

