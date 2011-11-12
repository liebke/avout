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


