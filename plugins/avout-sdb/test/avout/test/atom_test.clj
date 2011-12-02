(ns avout.test.atom-test
  (:use avout.core
        avout.sdb
        avout.atoms.sdb
        clojure.test))

  (defn timer [f]
    (let [start (System/nanoTime)]
      (f)
      (/ (double (- (System/nanoTime) start)) 1000000.0)))

  (defn write-skew-test [client a n]
    (let [times (for [i (range n)] (promise))]
      (reset!! a [])
      (dotimes [i n]
        (future
          (try
            (deliver (nth times i) (timer (fn [] (swap!! a #(conj % (inc (count %)))))))
            (catch Throwable e (println "atom-write-skew-test deliver ex: ") (.printStackTrace e)))))
      [times a]))

  (defn proces-results [[time-proms a] n]
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
       :a @a
       :pass? (when-let [v @a] (or (zero? (count v))
                                   (= (last v) (count v))))
       :skew-count (when-let [v @a] (if (zero? (count v))
                                      0
                                      (- (count v) (last v))))
       :n n
       :avg-time-interval avg-time-interval
       :avg-time-interval-per-thread avg-time-interval-per-thread}))

  (defn analyze-write-skew-test [client a n]
    (try
      (let [res (proces-results (write-skew-test client a n) n)]
        (if (and (or (zero? (count (:a res)))
                     (= (last (:a res)) (count (:a res))))
                 (:pass? res))
          res
          (assoc res :pass? false)))
      (catch Throwable e (println "analyze-write-skew-test: " e (.printStackTrace e)))))


(deftest write-skew
  (let [run-count 100
        max-threads 10
        client (connect "127.0.0.1")
        ACCESS-KEY (get (System/getenv) "ACCESS_KEY")
        SECRET-KEY (get (System/getenv) "SECRET_KEY")
        _ (println "'" ACCESS-KEY "' : '" SECRET-KEY "'")
        sdb (sdb-client ACCESS-KEY SECRET-KEY)
        a (sdb-atom client sdb "test-domain" "/a-test" [])
        test-results (atom [])
        _ (dotimes [i run-count]
            (let [threads (inc (rand-int max-threads))
                  res (analyze-write-skew-test client a threads)]
              (print (str i ": threads " threads ", write-skews " (:skew-count res) ", pass? " (:pass? res)))
              (println ", values " (:a res))
              (swap! test-results conj res)))
        avg-time-interval-per-thread (/ (apply + (map :avg-time-interval-per-thread @test-results)) (count @test-results))]
    (println (str "total writes: " (reduce + (map :n @test-results))))
    (println (str "total write skews: " (reduce + (map :skew-count @test-results))))
    (println (str "probability of write skew: " (* 1.0 (/ (reduce + (map :skew-count @test-results))
                                                          (reduce + (map :n @test-results))))))
    (println (str "avg-time-interval-per-thread: " avg-time-interval-per-thread " msec"))
    (is (reduce #(and %1 %2) (map :pass? @test-results)))))
