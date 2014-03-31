(ns avout.client-handle
  (:require [zookeeper :as zk]))

(defprotocol ClientHandle
  (getClient [this])
  (close [this]))

(defn make-zookeeper-client-handle
  "Creates a new ZooKeeper client handle that simply wraps a single ZooKeeper
  connection. Calls to getClient will always return the same ZooKeeper
  instance, even if it has been disconnected."
  [& args]
  (let [client (apply zk/connect args)]
    (reify ClientHandle
      (getClient [this]
        client)
      (close [this]
        (zk/close client)))))