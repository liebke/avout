(ns avout.test.write-skew-test
  (:use avout.core
        clojure.test))

  (defn timer [f]
    (let [start (System/nanoTime)]
      (f)
      (/ (double (- (System/nanoTime) start)) 1000000.0)))

  (defn write-skew-test [client n]
    (let [times (for [i (range n)] (promise))
          c (zk-ref client "/c" 0)
          d (zk-ref client "/d" [])]
      (dotimes [i n]
        (print ".")
        (future
          (try
            (deliver (nth times i) (timer #(dosync!! client (alter!! d conj (alter!! c inc)))))
            (catch Throwable e (println "write-skew-test deliver ex: ") (.printStackTrace e)))))
      (println "submitted")
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
       :avg-time-interval avg-time-interval
       :avg-time-interval-per-thread avg-time-interval-per-thread}))

  (defn analyze-write-skew-test [client n]
    (try
      (let [res (proces-results (write-skew-test client n) n)]
       (assoc (if (and (= (:c res) n) (:pass? res))
                res
                (assoc res :pass? false))
         :n n))
      (catch Throwable e (println "analyze-write-skew-test: " e (.printStackTrace e)))))

(deftest write-skew
  (let [client (connect "127.0.0.1")
        test-results (atom [])
        _ (dotimes [i 50]
            (print i ": ")
            (swap! test-results conj (analyze-write-skew-test client 25)))
        avg-time-interval-per-thread (/ (apply + (map :avg-time-interval-per-thread @test-results)) (count @test-results))]
    (println "run results: " (map :pass? @test-results))
    (println "avg-time-interval-per-thread: " avg-time-interval-per-thread)
    (is (reduce #(and %1 %2) (map :pass? @test-results)))))
