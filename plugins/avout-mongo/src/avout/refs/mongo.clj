(ns avout.refs.mongo
  (:use avout.state)
  (:require [somnium.congomongo :as mongo]
            [avout.transaction :as tx]))

(deftype MongoVersionedStateContainer [conn name]

  VersionedStateContainer

  (initVersionedStateContainer [this])

  (destroyVersionedStateContainer [this]
    (mongo/with-mongo conn
      (mongo/destroy! :refs {:name name})))

  (getStateAt [this point]
    (if-let [res (mongo/with-mongo conn
                   (mongo/fetch-one :refs :where {:name name :point point}))]
      (:value res)
      (throw tx/retryex)))

  (setStateAt [this value point]
    (let [data (if value
                 {:name name :value value :point point}
                 {:name name :point point})]
      (mongo/with-mongo conn (mongo/insert! :refs data))))

  (deleteStateAt [this version]
    (mongo/with-mongo conn
      (mongo/destroy! :refs {:name name, :point version}))))


