(ns avout.config)

(def ^:dynamic *use-cache* true)

(def ^:dynamic *stm-node* "/stm")

(def HISTORY "/history")
(def TXN "/txn")
(def LOCK "/lock")
(def REFS "/refs")
(def ATOMS "/atoms")
(def PT-PREFIX "t-")
(def NODE-DELIM "/")

(def RETRY-LIMIT 100)
(def LAST-CHANCE-BARGE-RETRY (* 2/3 RETRY-LIMIT))
(def BARGE-WAIT-NANOS (* 10 10 1000000))
(def LOCK-WAIT-MSEC 5000)
(def BLOCK-WAIT-MSEC 25)

(def MAX-STM-HISTORY 1000)
(def STM-GC-INTERVAL 100)
(def REF-GC-INTERVAL 10)
