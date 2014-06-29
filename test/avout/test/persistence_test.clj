(ns avout.test.persistence-test
  (:require [avout.core :as zk])
  (:use [clojure.test]))

(deftest atoms-are-persistent
  (let [client (zk/connect "127.0.0.1")
        zk-test-atom (zk/zk-atom client "/zk-test-atom" 0)]
    ;; Set the atom to 42 and verify that it worked
    (zk/reset!! zk-test-atom 42)
    (is (= @zk-test-atom 42))
    ;; Disconnect the client
    (.close client))
  ;; Create a brand-new client for the same server
  (let [client (zk/connect "127.0.0.1")
        ;; Connect to the same atom without providing an initial value.
        ;; This should prevent it from being clobbered.
        zk-test-atom (zk/zk-atom client "/zk-test-atom")]
    (is (= @zk-test-atom 42))
    (.close client)))

(deftest refs-are-persistent
  (let [client (zk/connect "127.0.0.1")
        zk-test-ref (zk/zk-ref client "/zk-test-ref" 0)]
    ;; Set the ref to 42 and verify that it worked
    (zk/dosync!! client
                 (zk/ref-set!! zk-test-ref 42))
    (is (= @zk-test-ref 42))
    ;; Disconnect the client
    (.close client))
  ;; Create a brand-new client for the same server
  (let [client (zk/connect "127.0.0.1")
        ;; Connect to the same ref without providing an initial value.
        ;; This should prevent it from being clobbered.
        zk-test-ref (zk/zk-ref client "/zk-test-ref")]
    (is (= @zk-test-ref 42))
    (.close client)))
