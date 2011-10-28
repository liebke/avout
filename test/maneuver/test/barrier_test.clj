(ns maneuver.test.barrier-test
  (:use [maneuver barrier]
        [zookeeper]
        [clojure test])
  (:import (java.util UUID)))

(deftest barrier-test
  (let [client (connect "127.0.0.1:2181")
        result (ref [])
        make-processor (fn [sleep]
                         (fn []
                           (Thread/sleep (* 100 sleep))
                           (dosync (alter result #(conj % :done)))))
        barrier-node (str "/test-barrier-" (UUID/randomUUID))]
    (doseq [i (range 5)]
      (future
        (try
          (enter-barrier client 4 (make-processor i)
                         :barrier-node barrier-node
                         :proc-name (str "proc-" i))
          (catch Throwable e (.printStackTrace e)))))
    (is (= (count @result) 0))
    (enter-barrier client 4 (make-processor 0) :barrier-node barrier-node)
    (is (= (count @result) 6))))
