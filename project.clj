(defproject treeherd "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.apache.zookeeper/zookeeper "3.3.2"]
                 [log4j/log4j "1.2.16"] ;; somehow needed to get dependencies required by zookeeper
                ])
