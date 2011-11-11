(ns avout.atoms.mongo
  (:require [avout.atoms :as atoms]
            [somnium.congomongo :as mongo])
  (:import (avout.atoms AtomState)))

(deftype MongoAtomState [conn name] ;; add reference _id field to type, so it doesn't have to be retrieved
  AtomState
  (initState [this]
    (mongo/with-mongo conn
      (or (mongo/fetch-one :atoms :where {:name name})
          (mongo/insert! :atoms {:name name}))))

  (getState [this]
    (:value (mongo/with-mongo conn
              (mongo/fetch-one :atoms :where {:name name}))))

  (setState [this new-value]
    (mongo/with-mongo conn
      (let [data (mongo/fetch-one :atoms :where {:name name})]
        (mongo/update! :atoms data (assoc data :value new-value)))))

  (destroyState [this]
    (mongo/with-mongo conn
      (mongo/destroy! :atoms (mongo/fetch-one :atoms :where {:name name})))))

(defn mongo-atom
  ([zk-client mongo-conn name init-value & {:keys [validator]}]
     (doto (atoms/distributed-atom zk-client name (MongoAtomState. mongo-conn name))
       (set-validator! validator)
       (.reset init-value)))
  ([zk-client mongo-conn name]
     (mongo/with-mongo mongo-conn
       (if (mongo/fetch-one :atoms :where {:name name})
         (atoms/distributed-atom zk-client name (MongoAtomState. mongo-conn name))
         (throw (RuntimeException. "Either provide a name of an existing distributed atom, or provide an intial value"))))))

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
