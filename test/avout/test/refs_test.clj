(ns avout.test.refs-test
  (:use avout.transaction
        clojure.test)
  (:require [zookeeper :as zk]
            [avout.config :as cfg])
  (:import [java.util UUID]))

(binding [cfg/*stm-node* "/stm-test"]
  (println "binding cfg/*stm-node*: " cfg/*stm-node*)

  (def client (zk/connect "127.0.0.1"))
  (println "connected to zookeeper")

  (def test-ref "/test-ref")
  (def txid0 "t-0000000000")
  (def txid1 "t-0000000001")
  (def commit-pt0 "t-0000000002")
  (def commit-pt1 "t-0000000003")
  (def pt0 (str cfg/*stm-node* "/history/" txid0))
  (def pt1 (str cfg/*stm-node* "/history/" txid1))

  (println "about to define test")
  (deftest transaction-test
    (println "starting transaction-test")
    (reset-stm client)
    (println "reset-stm called")
    (is (= txid0 (extract-point pt0)))
    (is (= pt0 (next-point client)))
    (is (= pt1 (next-point client)))
    (is (current-state? client txid0 nil))
    (update-state client txid0 RUNNING)
    (is (current-state? client txid0 COMMITTING RUNNING))
    (update-state client txid0 KILLED COMMITTING)
    (is (current-state? client txid0 RUNNING))
    (update-state client txid0 RUNNING COMMITTING)
    (is (current-state? client txid0 COMMITTING))

    (is (not (tagged? client test-ref)))
    (write-tag (get-local-transaction client) test-ref)
    (is (tagged? client test-ref))

    (is (= (str test-ref "/history/" txid0 "-" commit-pt0)
           (write-commit-point client test-ref txid0 (next-point client))))
    (is (= (str test-ref "/history/" txid1 "-" commit-pt1)
           (write-commit-point client test-ref txid1 (next-point client))))
    (is (= (into #{} (zk/children client (str test-ref "/history")))
           #{(str txid0 "-" commit-pt0)
             (str txid1 "-" commit-pt1)}))
    (is (= [txid0 commit-pt0] (parse-version (str txid0 "-" commit-pt0))))
    (update-state client txid0 COMMITTED)
    (is (= txid0 (get-committed-point-before client test-ref commit-pt0)))
    (is (= txid0 (get-committed-point-before client test-ref (next-point client))))
    (update-state client txid1 COMMITTED)
    (is (= txid1 (get-committed-point-before client test-ref (next-point client))))
    ))