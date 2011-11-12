(ns avout.refs.zk
  (:use avout.refs)
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.transaction :as tx]
            [avout.util :as util]))

;; ZK data implementation

(deftype ZKRefState [client name]
  ReferenceState
  (initState [this] nil)

  (getRefName [this] name)

  (getState [this point]
    (println "RefState getState called " name point)
    (let [{:keys [data stat]} (zk/data client (str name tx/HISTORY tx/NODE-DELIM point))]
      (util/deserialize-form data)))

  (setState [this value point]
    (zk/set-data client (str name tx/HISTORY tx/NODE-DELIM point) (util/serialize-form value) -1))

    (destroyState [this] nil))

