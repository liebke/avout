(ns avout.test.ref-write-skew-test
  (:use avout.core
        clojure.test))

  (defn timer [f]
    (let [start (System/nanoTime)]
      (f)
      (/ (double (- (System/nanoTime) start)) 1000000.0)))

  (defn write-skew-test [client c d n]
    (let [times (for [i (range n)] (promise))]
      (dosync!! client (ref-set!! c 0))
      (dosync!! client (ref-set!! d []))
      (dotimes [i n]
        (future
          (try
            (deliver (nth times i) (timer #(dosync!! client (alter!! d conj (alter!! c inc)))))
            (print ".")
            (catch Throwable e (println "write-skew-test deliver ex: ") (.printStackTrace e)))))
      [times c d]))

  (defn proces-results [[time-proms c d] n]
    (let [times (doall (sort (map #(deref % 500000 -1) time-proms)))
          total-time (last times)
          time-intervals (map (fn [t0 t1] (- t1 t0)) (conj times 0) times)
          time-interval-per-thread (map / time-intervals (range n 0 -1))
          avg-time-interval (/ (apply + time-intervals) n)
          avg-time-interval-per-thread (/ (apply + time-interval-per-thread) n)]
      {:times times
       :total-time total-time
       :time-intervals time-intervals
       :time-interval-per-thread time-interval-per-thread
       :c @c
       :d @d
       :pass? (= @c (count @d))
       :skew-count (- (count @d) @c)
       :n n
       :avg-time-interval avg-time-interval
       :avg-time-interval-per-thread avg-time-interval-per-thread}))

  (defn analyze-write-skew-test [client c d n]
    (try
      (let [res (proces-results (write-skew-test client c d n) n)]
        (if (and (= (:c res) n) (:pass? res))
          res
          (assoc res :pass? false)))
      (catch Throwable e (println "analyze-write-skew-test: " e (.printStackTrace e)))))

;; results from 1000 run test
;; prob of write-skew: 0.001
;; avg-time-interval-per-thread: 63.66

(deftest write-skew
  (let [run-count 1000
        max-threads 10
        client (connect "127.0.0.1")
        c (zk-ref client "/c-test" 0)
        d (zk-ref client "/d-test" [])
        test-results (atom [])
        _ (dotimes [i run-count]
            (print i)
            (let [threads (inc (rand-int max-threads))
                  res (analyze-write-skew-test client c d threads)]
              (println (if (:pass? res) "" "FAIL"))
              ;(println (str i ": threads " threads ", write-skews " (:skew-count res) ", pass? " (:pass? res)))
              (swap! test-results conj res)))
        avg-time-interval-per-thread (/ (apply + (map :avg-time-interval-per-thread @test-results)) (count @test-results))
        passed (reduce #(and %1 %2) (map :pass? @test-results))]
    (println "Result Summary")
    (println (str "total writes: " (reduce + (map :n @test-results))))
    (println (str "total write skews: " (reduce + (map :skew-count @test-results))))
    (println (str "probability of write skew: " (* 1.0 (/ (reduce + (map :skew-count @test-results))
                                                          (reduce + (map :n @test-results))))))
    (println (str "avg-time-interval-per-thread: " avg-time-interval-per-thread " msec"))
    (is passed)))
