(ns avout.test.watch-test
  (:require [avout.core :as zk])
  (:use [clojure.test]))

(defn count-changes
  "A watch function that accepts a counter (atom) and increments it whenever
  the watch is triggered."
  [counter _ _ _ _]
  (swap! counter inc))

(deftest atom-add-watch-test
  (let [client (zk/connect "127.0.0.1")
        changes (atom 0)
        zk-test-atom (zk/zk-atom client "/zk-test-atom" 0)]
    ;; Add a simple watch and trigger it twice:
    (add-watch zk-test-atom :count-changes (partial count-changes changes))
    (zk/swap!! zk-test-atom inc)
    (zk/swap!! zk-test-atom inc)
    ;; Verify that the swap!! ops worked and the watches triggered:
    (is (= 2 @zk-test-atom))
    (is (= 2 @changes))
    ;; Trigger the watch again, but decrement the zk-atom this time:
    (zk/swap!! zk-test-atom dec)
    ;; Verify that both atoms still have the correct values:
    (is (= 1 @zk-test-atom))
    (is (= 3 @changes))
    (.close client)))

(deftest atom-remove-watch-test
  (let [client (zk/connect "127.0.0.1")
        counter-1 (atom 0)
        counter-2 (atom 0)
        zk-test-atom (zk/zk-atom client "/zk-test-atom" 0)]
    ;; Add two different watches:
    (add-watch zk-test-atom :watch-1 (partial count-changes counter-1))
    (add-watch zk-test-atom :watch-2 (partial count-changes counter-2))
    ;; Test that both are active:
    (is (= #{:watch-1 :watch-2} (-> zk-test-atom .getWatches keys set)))
    ;; Remove watch-1 and verify that watch-2 is still active:
    (remove-watch zk-test-atom :watch-1)
    (is (= #{:watch-2} (-> zk-test-atom .getWatches keys set)))
    (.close client)))

(deftest ref-add-watch-test
  (let [client (zk/connect "127.0.0.1")
        changes (atom 0)
        zk-test-ref (zk/zk-ref client "/zk-test-atom" 0)]
    ;; Add a simple watch and trigger it twice:
    (add-watch zk-test-ref :count-changes (partial count-changes changes))
    (zk/dosync!! client
                 (zk/alter!! zk-test-ref inc))
    (zk/dosync!! client
                 (zk/alter!! zk-test-ref inc))
    ;; Verify that the ref and counter have the correct values:
    (is (= 2 @zk-test-ref))
    (is (= 2 @changes))
    ;; Alter the ref by decrementing it:
    (zk/dosync!! client
                 (zk/alter!! zk-test-ref dec))
    ;; Verify that the ref and counter are both still correct:
    (is (= 1 @zk-test-ref))
    (is (= 3 @changes))
    (.close client)))

(deftest ref-remove-watch-test
  (let [client (zk/connect "127.0.0.1")
        counter-1 (atom 0)
        counter-2 (atom 0)
        zk-test-ref (zk/zk-ref client "/zk-test-atom" 0)]
    ;; Add two different watches:
    (add-watch zk-test-ref :watch-1 (partial count-changes counter-1))
    (add-watch zk-test-ref :watch-2 (partial count-changes counter-2))
    ;; Test that both are active:
    (is (= #{:watch-1 :watch-2} (-> zk-test-ref .getWatches keys set)))
    ;; Remove watch-1 and verify that watch-2 is still active:
    (remove-watch zk-test-ref :watch-1)
    (is (= #{:watch-2} (-> zk-test-ref .getWatches keys set)))
    (.close client)))
