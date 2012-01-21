(ns avout.dynamodb.atom
  (:use avout.state)
  (:require [dynamodb.core :as ddb]
            [avout.atoms :as atoms])
  (:import clojure.lang.IRef))

(deftype DynamoDBStateContainer [client tableName name]

  StateContainer

  (initStateContainer [this]
    (when-not (ddb/get-item client tableName name)
      (ddb/put-item client tableName {"name" name, "value" (pr-str nil)})))

  (destroyStateContainer [this]
    (ddb/delete-item client tableName name))

  (getState [this]
    (let [data (ddb/get-item client tableName name)]
      (if (contains? data "value")
          (read-string (get data "value"))
          (throw (RuntimeException. "dynamodb-atom unbound")))))

  (setState [this newValue]
    (ddb/put-item client tableName {"name" name, "value" (pr-str newValue)}))

  (compareAndSwap [this oldValue newValue]
    (ddb/put-item client tableName
                  {"name" name, "value" (pr-str newValue)}
                  {"name" name, "value" (pr-str oldValue)})))


(defn dynamodb-atom
  ([ddb-client table-name name init-value & {:keys [validator]}]
     (doto (avout.atoms.DistributedAtom. ddb-client table-name name
                                         (DynamoDBStateContainer. ddb-client table-name name)
                                         (atom validator))
       .init
       (.reset init-value)))
  ([ddb-client table-name name]
     (doto (avout.atoms.DistributedAtom. ddb-client table-name name
                                         (DynamoDBStateContainer. ddb-client table-name name)
                                         (atom nil))
       .init)))

(defn dynamodb-initializer
  ([name {:keys [ddb-client table-name]}]
     (dynamodb-atom ddb-client table-name name))
  ([name init-value {:keys [ddb-client table-name]}]
     (dynamodb-atom ddb-client table-name name init-value)))

(comment

  (use 'dynamodb.core)
  (use 'avout.core)
  (use 'avout.dynamodb.atom)
  (def aws-access-key-id (get (System/getenv) "AWS_ACCESS_KEY_ID"))
  (def aws-secret-key (get (System/getenv) "AWS_SECRET_KEY"))
  (def ddb (dynamodb-client aws-access-key-id aws-secret-key))

  (def a0 (dynamodb-atom ddb "test" "a" 0))
  @a
  (swap!! a0 inc)

  (def a1 (dynamodb-atom ddb "test" "a1" []))
  @a1
  (swap!! a1 conj 0)
  (swap!! a1 conj 1)

)
