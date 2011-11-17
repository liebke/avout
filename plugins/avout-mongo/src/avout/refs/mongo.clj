(ns avout.refs.mongo
  (:use avout.state)
  (:require [somnium.congomongo :as mongo]))

(deftype MongoRefState [conn name]

  VersionedStateContainer

  (initVersionedStateContainer [this])

  (destroyVersionedStateContainer [this]
    (mongo/with-mongo conn
      (mongo/destroy! :refs :where {:name name})))

  (getStateAt [this point]
    (:value (mongo/with-mongo conn
              (mongo/fetch-one :refs :where {:name name :point point}))))

  (setStateAt [this value point]
    (let [data (if value
                 {:name name :value value :point point}
                 {:name name :point point})]
      (mongo/with-mongo conn (mongo/insert! :refs data)))))


