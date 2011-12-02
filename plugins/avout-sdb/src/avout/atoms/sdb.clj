(ns avout.atoms.sdb
  (:use avout.state)
  (:require [avout.sdb :as sdb]
            [avout.atoms :as atoms]))


(deftype SDBStateContainer [client domain-name name]

  StateContainer

  (initStateContainer [this])

  (destroyStateContainer [this]
    (sdb/delete-attributes client domain-name name [{:name "value"}]))

  (getState [this]
    (let [data (sdb/get-attributes client domain-name name)]
      (if (get data "value")
          (read-string (get data "value"))
          (do (println "Atom value unbound")
              (throw (RuntimeException. "Atom value unbound"))))))

  (setState [this new-value]
    (sdb/put-attributes client domain-name name [{:name "value" :value (pr-str new-value)}])))


(defn sdb-atom
  ([zk-client sdb-client domain-name name init-value & {:keys [validator]}]
     (doto (atoms/distributed-atom zk-client name (SDBStateContainer. sdb-client domain-name name))
       (set-validator! validator)
       (.reset init-value)))
  ([zk-client sdb-client domain-name name]
     (atoms/distributed-atom zk-client name (SDBStateContainer. sdb-client domain-name name))))