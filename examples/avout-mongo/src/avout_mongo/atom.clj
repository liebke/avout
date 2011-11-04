(ns avout-mongo.atom
  (:require [avout.atoms :as atoms]
            [somnium.congomongo :as mongo]
            [avout.locks :as locks])
  (:import (avout.atoms AtomData)))

(deftype MongoAtomData [conn name]
  AtomData
  (getValue [this]
    (:value (mongo/with-mongo conn
              (mongo/fetch-one :atoms :where {:name name}))))

  (setValue [this new-value]
    (let [data (mongo/with-mongo conn (mongo/fetch-one :atoms :where {:name name}))]
      (mongo/with-mongo conn
        (mongo/update! :atoms data (assoc data :value new-value))))))

(defn mongo-atom
  ([zk-client mongo-conn name init-value]
     (doto (mongo-atom zk-client mongo-conn name)
       (.reset init-value)))
  ([zk-client mongo-conn name]
     (mongo/with-mongo mongo-conn
       (or (mongo/fetch-one :atoms :where {:name name})
           (mongo/insert! :atoms {:name name}))
       (atoms/distributed-atom zk-client name (MongoAtomData. mongo-conn name)))))

;; example usage
(comment
(use 'avout-demo.demo :reload-all)
(require '[somnium.congomongo :as mongo])
(require '[zookeeper :as zk])
(use 'avout.atoms)
(def zk-client (zk/connect "127.0.0.1"))
(def mongo-conn (mongo/make-connection "mydb"
                                 :host "127.0.0.1"
                                 :port 27017))

(def matom (mongo-atom zk-client mongo-conn "/matom"))
@matom
(swap!! matom assoc :c 3)
@matom
(swap!! matom update-in [:a] inc)
@matom

)
