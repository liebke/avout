(ns avout.core
  (:require [avout.refs :as refs]
            [avout.atoms :as atoms]
            [avout.transaction :as tx]
            [avout.locks :as locks]
            [zookeeper :as zk]
            avout.refs.zk
            avout.refs.local
            avout.atoms.zk))

(def connect zk/connect)

;; Distributed versions of Clojure's standard Ref functions

(defmacro dosync!!
  "Distributed version of Clojure's dosync macro."
  ([client & body]
     `(if (or (coll? '~client)
              (not (instance? org.apache.zookeeper.ZooKeeper ~client)))
        (throw (RuntimeException. "First argument to dosync!! must be a ZooKeeper client instance."))
        (do (tx/create-local-transaction ~client)
            (tx/run-in-transaction ~client (fn [] ~@body))))))

(defn ref-set!!
  "Distributed version of Clojure's ref-set function."
  ([ref value] (.setRef ref value)))

(defn alter!!
  "Distributed version of Clojure's alter function."
  ([ref f & args] (.alterRef ref f args)))

;; ZK and local Reference implementations

(defn zk-ref
  ([client name init-value & {:keys [validator]}]
     (let [r (doto (refs/distributed-ref client name
                                         (avout.refs.zk.ZKVersionedStateContainer.
                                          client
                                          (str tx/*stm-node* tx/REFS name)))
               (set-validator! validator))]
       (dosync!! client (ref-set!! r init-value))
       r))
  ([client name]
     ;; for connecting to an existing ref only
     (refs/distributed-ref client name
                           (avout.refs.zk.ZKVersionedStateContainer.
                             client
                             (str tx/*stm-node* tx/REFS name)))))

(defn local-ref
  ([client name init-value & {:keys [validator]}]
     (let [r (doto (refs/distributed-ref client name
                                         (avout.refs.local.LocalVersionedStateContainer.
                                           client
                                           (str tx/*stm-node* tx/REFS name) (atom {})))
               (set-validator! validator))]
       (dosync!! client (ref-set!! r init-value))
       r))
  ([client name]
     ;; for connecting to an existing ref only
     (refs/distributed-ref client name
                           (avout.refs.local.LocalVersionedStateContainer.
                             client
                             (str tx/*stm-node* tx/REFS name) (atom {})))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Versions of Clojure's Atom functions swap!, reset!, compare-and-set! for use with AtomReferences
;; Built-in Clojure functions that work against IRef work with AtomReferences, including
;; deref, the @ deref reader-macro, set-validator!, get-validator!, add-watch, and remove-watch

(defn swap!!
  "Cannot use standard swap! because Clojure expects a clojure.lang.Atom."
  ([atom f & args] (.swap atom f args)))

(defn reset!!
  "Cannot use standard reset! because Clojure expects a clojure.lang.Atom."
  ([atom new-value] (.reset atom new-value)))

(defn compare-and-set!!
  "Cannot use standard reset! because Clojure expects a clojure.lang.Atom."
  ([atom old-value new-value] (.compareAndSet atom old-value new-value)))

;; ZK-based atom implementation

(defn zk-atom
  ([client name init-value & {:keys [validator]}]
     (doto (atoms/distributed-atom client name (avout.atoms.zk.ZKStateContainer. client (str name "/data")))
       (set-validator! validator)
       (.reset init-value)))
  ([client name] ;; for connecting to an existing atom only
     (atoms/distributed-atom client name (avout.atoms.zk.ZKStateContainer. client (zk/create-all client (str name "/data"))))))





(comment

  (use 'avout.core :reload-all)
  (require '[avout.transaction :as tx])

  (def client (connect "127.0.0.1"))
  (tx/reset-stm client)

  (defn thread-test [client n]
    (let [c (zk-ref client "/c" 0)
          d (zk-ref client "/d" [])]
      (doall
       (repeatedly n
                   (fn [] (future
                            (try
                              (time (dosync!! client (alter!! d conj (alter!! c inc))))
                              (catch Throwable e (.printStackTrace e)))))))
      [c d]))

    (defn single-thread-test [client n]
      (let [c (zk-ref client "/c" 0)
            d (zk-ref client "/d" [])]
        (doall
         (repeatedly n
                     (fn [] (try
                              (time (dosync!! client (alter!! d conj (alter!! c inc))))
                              (catch Throwable e (.printStackTrace e))))))
        [c d]))

  (def refs (thread-test 1))
  (map deref refs)
  )
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; zk-atom examples
(comment

  (use 'avout.core :reload-all)
  (require '[zookeeper :as zk])

  (def client (zk/connect "127.0.0.1"))
  (def a0 (zk-atom client "/a1" 0))
  @a0
  (swap!! a0 inc)
  @a0

  (def a1 (zk-atom client "/a1" {}))
  @a1
  (swap!! a1 assoc :a 1)
  (swap!! a1 update-in [:a] inc)

  ;; check that reads are not blocked by writes
  (future (swap!! a1 (fn [v] (Thread/sleep 5000) (update-in v [:a] inc))))
  @a1

  ;; test watches
  (add-watch a1 :a1 (fn [akey aref old-val new-val] (println akey aref old-val new-val)))
  (swap!! a1 update-in [:a] inc)
  (swap!! a1 update-in [:a] inc)
  (remove-watch a1 :a1)
  (swap!! a1 update-in [:a] inc)

  )

(comment

  (use 'avout.core :reload-all)
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
  (use 'avout.core :reload-all)
  (require '[zookeeper :as zk])

  ;; connect to the stm
  (def client (zk/connect "127.0.0.1"))

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

(comment

  (use 'avout.core :reload-all)
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
  (use 'avout.core :reload-all)
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
  (use 'avout.core :reload-all)
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