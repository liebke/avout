(ns avout.mongo
  (:use avout.core)
  (:require [avout.refs :as refs]
            [avout.atoms :as atoms]
            avout.refs.mongo
            avout.atoms.mongo
            [somnium.congomongo :as mongo]))

(defn mongo-ref
  ([zk-client mongo-conn name init-value & {:keys [validator]}]
     (let [r (doto (mongo-ref zk-client mongo-conn name)
               (set-validator! validator))]
       (dosync!! zk-client (ref-set!! r init-value))
       r))
  ([zk-client mongo-conn name]
     (mongo/with-mongo mongo-conn
       (let [r (mongo/with-mongo mongo-conn
                 (or (mongo/fetch-one :refs :where {:name name})
                     (mongo/insert! :refs {:name name}))
                 (refs/distributed-ref zk-client name (avout.refs.mongo.MongoVersionedStateContainer. mongo-conn name)))]
         (dosync!! zk-client (ref-set!! r nil))
         r))))


(defn mongo-atom
  ([zk-client mongo-conn name init-value & {:keys [validator]}]
     (doto (atoms/distributed-atom zk-client name (avout.atoms.mongo.MongoStateContainer. mongo-conn name))
       (set-validator! validator)
       (.reset init-value)))
  ([zk-client mongo-conn name]
     (mongo/with-mongo mongo-conn
       (if (mongo/fetch-one :atoms :where {:name name})
         (atoms/distributed-atom zk-client name (avout.atoms.mongo.MongoStateContainer. mongo-conn name))
         (throw (RuntimeException. "Either provide a name of an existing distributed atom, or provide an intial value"))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; mongo-atom examples
(comment

  (use 'avout.refs)
  (use 'avout.refs.mongo :reload-all)
  (require '[somnium.congomongo :as mongo])
  (require '[zookeeper :as zk])

  (def zk-client (zk/connect "127.0.0.1"))
  (def mongo-conn (mongo/make-connection "statedb" :host "127.0.0.1" :port 27017))

  (def r0 (mongo-ref zk-client mongo-conn "/r0" {:a 1}))
  @r0
  (dosync!! zk-client  (alter!! r0 assoc :c 3))
  @r0
  (dosync!! zk-client (alter!! r0 update-in [:a] inc))
  @r0

  (def r1 (mongo-ref zk-client mongo-conn "/r1" 1 :validator pos?))
  (add-watch r1 :r1 (fn [key ref old-val new-val]
                      (println key ref old-val new-val)))
  @r1
  (dosync!! zk-client (alter!! r1 inc))
  @r1
  (dosync!! zk-client (alter!! r1 - 2))
  (remove-watch r1 :r1)
  @r1

  (use 'avout.refs.zk)
  (def r2 (zk-ref zk-client "/r2" []))
  (def r3 (mongo-ref zk-client mongo-conn "/r3" 1 :validator pos?))

  (dosync!! zk-client
    (alter!! r3 inc)
    (alter!! r2 conj @r3))

  (dosync!! zk-client
    (alter!! r2 conj (alter!! r3 inc)))
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; mongo-atom examples
(comment

  (use 'avout.atoms)
  (use 'avout.atoms.mongo :reload-all)
  (require '[somnium.congomongo :as mongo])
  (require '[zookeeper :as zk])

  (def zk-client (zk/connect "127.0.0.1"))
  (def mongo-conn (mongo/make-connection "statedb" :host "127.0.0.1" :port 27017))

  (def a0 (mongo-atom zk-client mongo-conn "/a0" {:a 1}))
  @a0
  (swap!! a0 assoc :c 3)
  @a0
  (swap!! a0 update-in [:a] inc)
  @a0

  (def a1 (mongo-atom zk-client mongo-conn "/a1" 1 :validator pos?))
  (add-watch a1 :a1 (fn [key ref old-val new-val]
                      (println key ref old-val new-val)))
  @a1
  (swap!! a1 inc)
  @a1
  (swap!! a1 - 2)
  (remove-watch a1 :a1)
  @a1

)