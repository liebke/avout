(ns treeherd.test.locks_test
  (:use [treeherd.locks])
  (:use [clojure.test])
  (:import [java.util.concurrent TimeUnit]
           [java.util.concurrent.locks ReentrantLock])
  (:require [treeherd.zookeeper :as zk]))


(defn test-lock
  ([lock]
     (let [state (ref [])
           prom1 (promise) ;; use promises to wait for workers to complete before returning result
           prom2 (promise)
           prom3 (promise)
           worker3 (fn [prom]
                     (future (try
                               (when-lock lock (dosync (alter state #(conj % [3 1 :FAILED]))))
                               (with-lock lock
                                 (dosync (alter state #(conj % [3 1 (.getHoldCount lock)]))))
                               (deliver prom @state)
                               (catch Throwable e (.printStackTrace e)))))
           worker2 (fn [prom]
                     (future (try
                               (with-lock lock
                                 (worker3 prom3)
                                 (Thread/sleep 2000)
                                 (dosync (alter state #(conj % [2 1 (.getHoldCount lock)])))
                                 (with-lock lock
                                   (dosync (alter state #(conj % [2 2 (.getHoldCount lock)]))))
                                 (dosync (alter state #(conj % [2 3 (.getHoldCount lock)]))))
                               (deliver prom @state)
                               (catch Throwable e (.printStackTrace e)))))
           worker1 (fn [prom]
                     (future (try
                               (if-lock lock
                                       (do (worker2 prom2)
                                           (dosync (alter state #(conj % [1 1 (.getHoldCount lock)])))
                                           (with-lock lock
                                             (dosync (alter state #(conj % [1 2 (.getHoldCount lock)]))))
                                           (dosync (alter state #(conj % [1 3 (.getHoldCount lock)]))))
                                       (dosync (alter state #(conj % [1 1 :FAILED]))))
                               (deliver prom @state)
                               (catch Throwable e (.printStackTrace e)))))]
       (worker1 prom1)
       @prom1
       @prom2
       @prom3
       @state)))

(defn test-lock-with-timeout
  ([lock]
     (let [state (ref [])
           prom1 (promise) ;; use promises to wait for workers to complete before returning result
           prom2 (promise)
           prom3 (promise)
           worker3 (fn [prom]
                     (future (if-lock-with-timeout lock 1 TimeUnit/SECONDS
                               (dosync (alter state #(conj % [3 1 :FAILED])))
                               (dosync (alter state #(conj % [3 1 (.getHoldCount lock)]))))
                             (deliver prom @state)))
           worker2 (fn [prom]
                     (future (if-lock-with-timeout lock 10 TimeUnit/SECONDS
                               (dosync (alter state #(conj % [2 1 (.getHoldCount lock)])))
                               (dosync (alter state #(conj % [2 1 :FAILED]))))
                             (deliver prom @state)))
           worker1 (fn [prom]
                     (future (with-lock lock
                               (worker2 prom2)
                               (worker3 prom3)
                               (Thread/sleep 5000)
                               (dosync (alter state #(conj % [1 1 (.getHoldCount lock)]))))
                             (deliver prom @state)))]
       (worker1 prom1)
       @prom1
       @prom2
       @prom3
       @state)))

(deftest locking-test
  (let [client (zk/connect "127.0.0.1")
        _ (zk/delete-all client "/testing-lock")
        _ (zk/delete-all client "/testing-lock-timeout")
        locking-results (test-lock (distributed-lock client :lock-node "/testing-lock"))
        locking-timeout-results (test-lock-with-timeout (distributed-lock client :lock-node "/testing-lock-timeout"))]

    ;; distributed-lock should behave just like ReentrantLock
    (is (= locking-results (test-lock (ReentrantLock. true))))

    ;; this is the absolute results
    (is (= locking-results [[1 1 1] [1 2 2] [1 3 1] [2 1 1] [2 2 2] [2 3 1] [3 1 1]]))

    (is (= locking-timeout-results (test-lock-with-timeout (ReentrantLock. true))))

    (is (= locking-timeout-results [[3 1 0] [1 1 1] [2 1 1]]))
    (.close client)))