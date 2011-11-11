(ns avout.refs.local
  (:use avout.refs)
  (:require [zookeeper :as zk]
            [avout.transaction :as tx]
            [avout.util :as util]))


(deftype LocalRefState [client name data]
  ReferenceState
  (initState [this] nil)

  (getRefName [this] name)

  (getState [this point]
    (println "RefState getState called " name point)
    (get @data point))

  (setState [this value point]
    (swap! data assoc point value))

    (destroyState [this] (reset! data {})))

(defn local-ref
  ([client name init-value & {:keys [validator]}]
     (let [r (doto (distributed-ref client name (LocalRefState. client name (atom {})))
               (set-validator! validator))]
       (dosync!! client (ref-set!! r init-value))
       r))
  ([client name]
     ;; for connecting to an existing ref only
     (distributed-ref client name (LocalRefState. client name (atom {})))))


(comment

  (use 'avout.refs :reload-all)
  (use 'avout.refs.local :reload-all)
  (require '[zookeeper :as zk])

  (def client (zk/connect "127.0.0.1"))
  (def a (local-ref client "/a" 0))
  (def b (local-ref client "/b" 0))
  @a
  @b
  (dosync!! client
    (alter!! a inc)
    (alter!! b #(+ @a %)))


  ;; from another repl
  (use 'avout.refs :reload-all)
  (use 'avout.refs.local :reload-all)
  (require '[zookeeper :as zk])

  ;; connect to the stm
  (def stm (zk/connect "127.0.0.1"))

  ;; no initial value, connect to an existing distributed ref
  (def a (local-ref stm "/a"))
  (def b (local-ref stm "/b"))

  (dosync!! stm
    (alter!! a inc)
    (alter!! b #(+ @a %)))


  ;; concurrency test
  (use 'avout.refs :reload-all)
  (use 'avout.refs.local :reload-all)
  (require '[zookeeper :as zk])

  ;; connect to the stm
  (def client (zk/connect "127.0.0.1"))

  (zk/delete-all client "/a")
  (zk/delete-all client "/b")
  (def a (local-ref client "/a" 0))
  (def b (local-ref client "/b" 0))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try
                          (dosync!! client
                            (alter!! a inc)
                            (alter!! b inc))
                          (catch Throwable e (.printStackTrace e)))))))
  [@a @b]


  (def c (local-ref client "/c" 0))
  (def d (local-ref client "/d" []))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try
                          (dosync!! client
                            (alter!! d conj (alter!! c inc)))
                          (catch Throwable e (.printStackTrace e)))))))
  [@c @d]


  (def a (local-ref client "/a"))
  (def b (local-ref client "/b"))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try
                          (dosync!! client
                            (alter!! a inc)
                            (alter!! b inc))
                          (catch Throwable e (.printStackTrace e)))))))

)