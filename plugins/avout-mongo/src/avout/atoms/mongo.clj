(ns avout.atoms.mongo
  (:use avout.state)
  (:require [avout.atoms :as atoms]
            [somnium.congomongo :as mongo]))

(deftype MongoStateContainer [conn name]

  StateContainer

  (initStateContainer [this]
    (mongo/with-mongo conn
      (or (mongo/fetch-one :atoms :where {:name name})
          (mongo/insert! :atoms {:name name}))))

  (destroyStateContainer [this]
    (mongo/with-mongo conn
      (mongo/destroy! :atoms (mongo/fetch-one :atoms :where {:name name}))))

  (getState [this]
    (:value (mongo/with-mongo conn
              (mongo/fetch-one :atoms :where {:name name}))))

  (setState [this new-value]
    (mongo/with-mongo conn
      (let [data (mongo/fetch-one :atoms :where {:name name})]
        (mongo/update! :atoms data (assoc data :value new-value))))))


