(ns maneuver.queues
  (:import (java.util Queue
                      TreeMap)
           (org.apache.zookeeper KeeperException$NoNodeException))
  (require [zookeeper :as zk]
           [zookeeper.util :as util]))

(defn populate-tree-map
  ([client tree-map dir]
     (doseq [[k v] (zk/index-sequential-nodes (zk/children client dir))]
       (.put tree-map k v))
     tree-map))

(defn first-entry
  ([client tree-map dir]
     (loop []
       (if-let [value (util/try*
                      (.getValue (.firstEntry tree-map))
                      (catch java.util.NoSuchElementException e))]
         (loop []
           (if-let [node (util/try*
                      (.getValue (.firstEntry tree-map))
                      (catch java.util.NoSuchElementException e))]
             (try
               (zk/data client node)
              (catch KeeperException$NoNodeException e
                (recur)))))
         (do (populate-tree-map client tree-map dir)
             (.getValue (.firstEntry tree-map)))))))

;; queue of maps {:name name, :path path, :data data, :stat stat}

(defn next-lowest
  ([tree-map node]
     (let [id (zk/extract-id node)
           next-lowest-key (.lowerKey tree-map (Integer/valueOf id))]
       (.getValue (.floorEntry tree-map next-lowest-key)))))

(defn element* [client tree-map dir]
  (first-entry client tree-map dir))

(defn offer*
  ([client tree-map dir node-prefix data]
     (let [node (zk/create client (str dir "/" node-prefix "-"))
           id (zk/extract-id node)]
       (.put tree-map id node)
       (when data
         (zk/set-data client node data)))))

(defn peek* [client tree-map dir]
  (util/try*
   (first-entry client tree-map dir)
   (catch java.util.NoSuchElementException e)))

(defn poll* [client dir tree-map]
  (let [node (util/try*
              (first-entry client tree-map dir)
              (catch java.util.NoSuchElementException e))]
    (zk/delete client (str dir "/" node))
    node))

(defn remove* [client tree-map dir]
  (let [node (first-entry client tree-map dir)]
    (zk/delete client (str dir "/" node))
    node))

(defprotocol DistributedTreeMap
  (offer [this data & options] (apply offer* this data options)))

(deftype DistributedQueue [client tree-map dir node-prefix]
  Queue
  (element [this]
    (element* client tree-map dir))

  (offer [this data]
    (offer* client tree-map dir node-prefix data))

  (peek [this]
    (peek* client tree-map dir))

  (poll [this]
    (poll* client dir tree-map))

  (remove [this]
    (remove* client tree-map dir))

  DistributedTreeMap
  (offer [this data & options]
    (apply offer* client tree-map dir node-prefix data)))
