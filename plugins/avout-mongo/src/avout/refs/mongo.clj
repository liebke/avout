(ns avout.refs.mongo
  (:require [avout.refs :as refs]
            [somnium.congomongo :as mongo])
  (:import (avout.refs ReferenceState)))

(deftype MongoRefState [conn name]
  ReferenceState
  (initState [this])

  (getRefName [this] name)

  (getState [this point]
    (:value (mongo/with-mongo conn
              (mongo/fetch-one :refs :where {:name name :point point}))))

  (setState [this value point]
    (let [data (if value
                 {:name name :value value :point point}
                 {:name name :point point})]
      (mongo/with-mongo conn (mongo/insert! :refs data))))

  (destroyState [this]
    (mongo/with-mongo conn
      (mongo/destroy! :refs :where {:name name}))))

(defn mongo-ref
  ([zk-client mongo-conn name init-value & {:keys [validator]}]
     (let [r (doto (mongo-ref zk-client mongo-conn name)
               (set-validator! validator))]
       (refs/dosync!! zk-client (refs/ref-set!! r init-value))
       r))
  ([zk-client mongo-conn name]
     (mongo/with-mongo mongo-conn
       (let [r (mongo/with-mongo mongo-conn
                 (or (mongo/fetch-one :refs :where {:name name})
                     (mongo/insert! :refs {:name name}))
                 (refs/distributed-ref zk-client name (MongoRefState. mongo-conn name)))]
         (refs/dosync!! zk-client (refs/ref-set!! r nil))
         r))))

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
