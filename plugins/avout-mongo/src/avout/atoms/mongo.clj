(ns avout.atoms.mongo
  (:use avout.state)
  (:require [avout.atoms :as atoms]
            [somnium.congomongo :as mongo]))

(deftype MongoAtomState [conn name] ;; add reference _id field to type, so it doesn't have to be retrieved
  Identity
  (init [this]
    (mongo/with-mongo conn
      (or (mongo/fetch-one :atoms :where {:name name})
          (mongo/insert! :atoms {:name name}))))

  (destroyState [this]
    (mongo/with-mongo conn
      (mongo/destroy! :atoms (mongo/fetch-one :atoms :where {:name name}))))

  StateContainer
  (getState [this]
    (:value (mongo/with-mongo conn
              (mongo/fetch-one :atoms :where {:name name}))))

  (setState [this new-value]
    (mongo/with-mongo conn
      (let [data (mongo/fetch-one :atoms :where {:name name})]
        (mongo/update! :atoms data (assoc data :value new-value))))))


