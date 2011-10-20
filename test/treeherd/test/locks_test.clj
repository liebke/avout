(ns treeherd.test.locks_test
  (:use [treeherd.locks])
  (:use [clojure.test])
  (:require [treeherd.zookeeper :as zk]))


(defn test-lock
  ([lock]
     (let [state (ref [])
           prom1 (promise) ;; use promises to wait for workers to complete before returning result
           prom2 (promise)
           prom3 (promise)
           worker3 (fn [prom]
                     (future (when-lock lock (dosync (alter state #(conj % [:FAILED]))))
                             (with-lock lock
                               (dosync (alter state #(conj % [3 1 (.getHoldCount lock)]))))
                             (deliver prom @state)))
           worker2 (fn [prom]
                     (future (with-lock lock
                               (dosync (alter state #(conj % [2 1 (.getHoldCount lock)])))
                               (with-lock lock
                                 (dosync (alter state #(conj % [2 2 (.getHoldCount lock)]))))
                               (dosync (alter state #(conj % [2 3 (.getHoldCount lock)]))))
                             (deliver prom @state)))
           worker1 (fn [prom]
                     (future (if-lock lock
                                      (do (worker2 prom2)
                                          (worker3 prom3)
                                          (dosync (alter state #(conj % [1 1 (.getHoldCount lock)])))
                                          (with-lock lock
                                            (dosync (alter state #(conj % [1 2 (.getHoldCount lock)]))))
                                          (dosync (alter state #(conj % [1 3 (.getHoldCount lock)]))))
                                      (dosync (alter state #(conj % [:FAILED]))))
                             (deliver prom @state)))]
       (worker1 prom1)
       @prom1
       @prom2
       @prom3
       @state)))


(deftest locking-test
  (let [client (zk/connect "127.0.0.1")
        locking-results (test-lock (distributed-lock client :lock-node "/testing-lock"))]
    ;; distributed-lock should behave just like ReentrantLock
    (is (= locking-results (test-lock (java.util.concurrent.locks.ReentrantLock. true))))
    ;; this is the absolute results
    (is (= locking-results [[1 1 1] [1 2 2] [1 3 1] [2 1 1] [2 2 2] [2 3 1] [3 1 1]]))

    (.close client)))