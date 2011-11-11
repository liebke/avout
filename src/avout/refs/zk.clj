(ns avout.refs.zk
  (:use avout.refs)
  (:require [zookeeper :as zk]
            [zookeeper.data :as data]
            [avout.transaction :as tx]
            [avout.util :as util]))

;; ZK data implementation

(deftype ZKRefState [client name]
  ReferenceState
  (initState [this] nil)

  (getRefName [this] name)

  (getState [this point]
    (println "RefState getState called " name point)
    (let [{:keys [data stat]} (zk/data client (str name tx/HISTORY tx/NODE-DELIM point))]
      (util/deserialize-form data)))

  (setState [this value point]
    (zk/set-data client (str name tx/HISTORY tx/NODE-DELIM point) (util/serialize-form value) -1))

    (destroyState [this] nil))

(defn zk-ref
  ([client name init-value & {:keys [validator]}]
     (let [r (doto (distributed-ref client name (ZKRefState. client name))
               (set-validator! validator))]
       (dosync!! client (ref-set!! r init-value))
       r))
  ([client name]
     ;; for connecting to an existing ref only
     (distributed-ref client name (ZKRefState. client name))))


(comment

  (use 'avout.refs :reload-all)
  (use 'avout.refs.zk :reload-all)
  (require '[zookeeper :as zk])

  (def client (zk/connect "127.0.0.1"))
  (def a (zk-ref client "/a" 0))
  (def b (zk-ref client "/b" 0))
  @a
  @b
  (dosync!! client
    (alter!! a inc)
    (alter!! b #(+ @a %)))


  ;; from another repl
  (use 'avout.refs :reload-all)
  (use 'avout.refs.zk :reload-all)
  (require '[zookeeper :as zk])

  ;; connect to the stm
  (def stm (zk/connect "127.0.0.1"))

  ;; no initial value, connect to an existing distributed ref
  (def a (zk-ref stm "/a"))
  (def b (zk-ref stm "/b"))

  (dosync!! stm
    (alter!! a inc)
    (alter!! b #(+ @a %)))


  ;; concurrency test
  (use 'avout.refs :reload-all)
  (use 'avout.refs.zk :reload-all)
  (require '[zookeeper :as zk])

  ;; connect to the stm
  (def client (zk/connect "127.0.0.1"))

  (zk/delete-all client "/a")
  (zk/delete-all client "/b")
  (def a (zk-ref client "/a" 0))
  (def b (zk-ref client "/b" 0))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try
                          (dosync!! client
                            (alter!! a inc)
                            (alter!! b inc))
                          (catch Throwable e (.printStackTrace e)))))))
  [@a @b]


  (def c (zk-ref client "/c" 0))
  (def d (zk-ref client "/d" []))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try
                          (dosync!! client
                            (alter!! d conj (alter!! c inc)))
                          (catch Throwable e (.printStackTrace e)))))))
  [@c @d]


  (def a (zk-ref client "/a"))
  (def b (zk-ref client "/b"))
  (doall
   (repeatedly 6
               (fn [] (future
                        (try
                          (dosync!! client
                            (alter!! a inc)
                            (alter!! b inc))
                          (catch Throwable e (.printStackTrace e)))))))

)