(ns avout.config
  (:require [zookeeper.data :as data]))


(def ^:dynamic *use-cache* true)

(def ^:dynamic *stm-node* "/stm")

(def HISTORY "/history")
(def TXN "/txn")
(def LOCK "/lock")
(def REFS "/refs")
(def ATOMS "/atoms")
(def PT-PREFIX "t-")
(def NODE-DELIM "/")

(def RETRY-LIMIT 50)
(def LOCK-WAIT-MSEC 25)
(def BARGE-WAIT-NANOS (* 10 10 1000000))
(def MAX-STM-HISTORY 100)
(def STM-GC-INTERVAL 100)
