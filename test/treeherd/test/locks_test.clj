(ns treeherd.test.locks_test
  (:use [treeherd.locks])
  (:use [clojure.test])
  (:require [treeherd.zookeeper :as zk]))


(defn test-locks
  ([client]
     (let [lock (distributed-lock client)
           state (ref [])
           prom1 (promise) ;; use to wait for both worker 1 to complete
           prom2 (promise) ;; use to wait for both worker 2 to complete
           prom3 (promise)
           worker3 (fn [prom]
                     (future (with-lock lock
                               (dosync (alter state #(conj % [3 1 (.getHoldCount lock)]))))
                             (deliver prom @state)))
           worker2 (fn [prom]
                     (future (with-lock lock
                               (dosync (alter state #(conj % [2 1 (.getHoldCount lock)])))
                               (worker3 prom3)
                               (with-lock lock
                                 (dosync (alter state #(conj % [2 2 (.getHoldCount lock)]))))
                               (dosync (alter state #(conj % [2 3 (.getHoldCount lock)]))))
                             (deliver prom @state)))
           worker1 (fn [prom]
                     (future (with-lock lock
                               (dosync (alter state #(conj % [1 1 (.getHoldCount lock)])))
                               (worker2 prom2)
                               (with-lock lock
                                 (dosync (alter state #(conj % [1 2 (.getHoldCount lock)]))))
                               (dosync (alter state #(conj % [1 3 (.getHoldCount lock)]))))
                             (deliver prom @state)))]
       (worker1 prom1)
       @prom1
       @prom2
       @prom3
       @state)))


(deftest with-lock-test
  (is (= (test-locks (zk/connect "127.0.0.1"))
         [[1 1 1] [1 2 2] [1 3 1] [2 1 1] [2 2 2] [2 3 1] [3 1 1]])))