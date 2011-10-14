(defproject treeherd "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.apache.zookeeper/zookeeper "3.3.2"]
                 [commons-codec "1.5"] ;; needed for SHA-1 and BASE64 encoding
                 [log4j/log4j "1.2.16"] ;; somehow needed to get dependencies required by zookeeper
                ])
