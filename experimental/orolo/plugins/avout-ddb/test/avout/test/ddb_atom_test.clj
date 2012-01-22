(ns avout.test.ddb-atom-test
  (:use avout.core
        clojure.test)
  (:import (java.util UUID)))

;; Need to set Log property before requiring anything that
;; uses the AWS's DynamoDB SDK in order to prevent console logging
;; during testing.
(System/setProperty "org.apache.commons.logging.Log"
                    "org.apache.commons.logging.impl.NoOpLog")

(use 'dynamodb.core
     'avout.dynamodb.atom)

(defn timer [f]
  (let [start (System/nanoTime)]
    (f)
    (/ (double (- (System/nanoTime) start)) 1000000.0)))

(defn write-skew-test [a n]
  (let [times (for [i (range n)] (promise))]
    (reset!! a [])
    (dotimes [i n]
      (future
        (try
          (deliver (nth times i) (timer (fn [] (swap!! a #(conj % (inc (count %)))))))
          (catch Throwable e (println "dynamodb-atom-write-skew-test deliver ex: ") (.printStackTrace e)))))
    (let [res  (loop [j 0]
                 (let [v @a]
                   (if (= (count v) n)
                     v
                     (do (Thread/sleep 500)
                         (recur (inc j))))))]
      [(doall (sort (map #(deref % 10000 -1) times))) res])))

(defn proces-results [[times v] n]
  (let [total-time (last times)
        time-intervals (map (fn [t0 t1] (- t1 t0)) (conj times 0) times)
        time-interval-per-thread (map / time-intervals (range n 0 -1))
        avg-time-interval (/ (apply + time-intervals) n)
        avg-time-interval-per-thread (/ (apply + time-interval-per-thread) n)]
    {:times times
     :total-time total-time
     :time-intervals time-intervals
     :time-interval-per-thread time-interval-per-thread
     :a v
     :pass? (or (zero? (count v))
                (= (last v) (count v)))
     :skew-count (if (zero? (count v))
                   0
                   (- (count v) (last v)))
     :n n
     :avg-time-interval avg-time-interval
     :avg-time-interval-per-thread avg-time-interval-per-thread}))

(defn analyze-write-skew-test [a n]
  (try
    (let [res (proces-results (write-skew-test a n) n)]
      (if (and (or (zero? (count (:a res)))
                   (= (last (:a res)) (count (:a res))))
               (:pass? res))
        res
        (assoc res :pass? false)))
    (catch Throwable e (println "analyze-write-skew-test: " e (.printStackTrace e)))))


(deftest write-skew
  (let [run-count 100
        max-threads 10
        ACCESS-KEY (get (System/getenv) "AWS_ACCESS_KEY_ID")
        SECRET-KEY (get (System/getenv) "AWS_SECRET_KEY")
        _ (println "'" ACCESS-KEY "' : '" SECRET-KEY "'")
        ddb (dynamodb-client ACCESS-KEY SECRET-KEY)
;        ddb (doto (dynamodb-client ACCESS-KEY SECRET-KEY :endpoint "dynamodb.us-west-1.amazonaws.com")
;              (create-table "test-table" "name"))
        uuid (str (UUID/randomUUID))
        a (dynamodb-atom ddb "test" (str "a-" uuid) [])
        test-results (atom [])
        _ (dotimes [i run-count]
            (let [threads (inc (rand-int max-threads))
                  res (analyze-write-skew-test a threads)]
              (print (str i ": threads " threads ", items " (count (:a res)) ", write-skews " (:skew-count res) ", pass? " (:pass? res)))
              (println ", values " (:a res))
              (swap! test-results conj res)))
        avg-time-interval-per-thread (/ (apply + (map :avg-time-interval-per-thread @test-results)) (count @test-results))]
    (println (str "total writes: " (reduce + (map :n @test-results))))
    (println (str "total write skews: " (reduce + (map :skew-count @test-results))))
    (println (str "probability of write skew: " (* 1.0 (/ (reduce + (map :skew-count @test-results))
                                                          (reduce + (map :n @test-results))))))
    (println (str "avg-time-interval-per-thread: " avg-time-interval-per-thread " msec"))
    (is (reduce #(and %1 %2) (map :pass? @test-results)))))
