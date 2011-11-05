(ns avout.test.refs-test
  (:use avout.refs
        clojure.test)
  (:require [zookeeper :as zk])
  (:import [java.util UUID]))

(def client (zk/connect "127.0.0.1"))
(def test-ref "/test-ref")
(def txid0 "t-0000000000")
(def txid1 "t-0000000001")
(def txid2 "t-0000000002")
(def txid3 "t-0000000003")
(def pt0 (str *stm-node* "/history/" txid0))
(def pt1 (str *stm-node* "/history/" txid1))

(deftest refs-test
  (reset-stm client)
  (reset-ref client test-ref)
  (is (= txid0 (extract-point pt0)))
  (is (= pt0 (next-point client)))
  (is (= pt1 (next-point client)))
  (is (current-state? client txid0 nil))
  (update-state client txid0 RUNNING)
  (is (current-state? client txid0 RUNNING))
  (update-state client txid0 KILLED COMMITTING)
  (is (current-state? client txid0 RUNNING))
  (update-state client txid0 RUNNING COMMITTING)
  (is (current-state? client txid0 COMMITTING))

  (is (not (tagged? client test-ref)))
  (tag client test-ref txid0)
  (is (tagged? client test-ref))

  (is (= (str test-ref "/history/" txid0 "-" txid2)
         (add-history client test-ref txid0 (next-point client))))
  (is (= (str test-ref "/history/" txid1 "-" txid3)
         (add-history client test-ref txid1 (next-point client))))
  (is (= (into #{} (zk/children client (str test-ref "/history")))
         #{(str txid0 "-" txid2)
           (str txid1 "-" txid3)}))
  (is (= [txid0 txid2] (split-read-commit-points (str txid0 "-" txid2))))
  (update-state client txid0 COMMITTED)


  (is (= txid0 (get-committed-point-before client test-ref txid2)))

  )