(ns treeheard.server
  (:import (org.apache.zookeeper.server ZooKeeperServerMain
                                        ServerConfig)))

(defn server-config
  ([filename]
     (-> (ServerConfig.)
         (.parse filename))))

(defn start-server
  ([config-filename]
     (-> (ZooKeeperServerMain.)
         (.runFromConfig (server-config config-filename)))))