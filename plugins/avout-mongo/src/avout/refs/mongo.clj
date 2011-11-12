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


