(ns avout.dynamodb.atom
  (:use avout.state)
  (:require [dynamodb.core :as ddb]
            [avout.atoms :as atoms])
  (:import clojure.lang.IRef))

(set! *warn-on-reflection* true)

(deftype DynamoDBStateContainer [client tableName name]

  StateContainer

  (initStateContainer [this]
    (when-not (contains? (ddb/list-tables client) tableName)
      (throw (RuntimeException. (str "Table " tableName " does not exist.")))))

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

(defn init-dynamodb-table
  "Creates DynamoDB table, blocking until it has been created."
  ([ddb-client table-name & {:keys [kb-read-per-sec kb-write-per-sec]
                             :or {kb-read-per-sec 5 kb-write-per-sec 5}}]
     (if (contains? (ddb/list-tables ddb-client) table-name)
       :table-already-exist
       (do
         (ddb/create-table ddb-client table-name "name"
                           :kb-read-per-sec kb-read-per-sec
                           :kb-write-per-sec kb-write-per-sec)
         (let [max-retries 10]
           (loop [retry 0]
             (if (= retry max-retries)
               :timed-out
               (if (contains? (ddb/list-tables ddb-client) table-name)
                 :table-created
                 (do (Thread/sleep 1000)
                     (recur (inc retry)))))))))))

(defn dynamodb-atom
  "The DynamoDB table must already exist, with a primary-key set to the name attribute."
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

  ;; shut logging off
  (System/setProperty "org.apache.commons.logging.Log"
                      "org.apache.commons.logging.impl.NoOpLog")
  (use 'dynamodb.core)
  (use 'avout.core)
  (use 'avout.dynamodb.atom)
  (def aws-access-key-id (get (System/getenv) "AWS_ACCESS_KEY_ID"))
  (def aws-secret-key (get (System/getenv) "AWS_SECRET_KEY"))
  (def ddb (dynamodb-client aws-access-key-id aws-secret-key))

  (def table-name "example")
  (init-dynamodb-table ddb table-name)
  
  (def a0 (dynamodb-atom ddb table-name "a" 0))
  @a
  (swap!! a0 inc)

  (def a1 (dynamodb-atom ddb table-name "a1" []))
  @a1
  (swap!! a1 conj 0)
  (swap!! a1 conj 1)

)
