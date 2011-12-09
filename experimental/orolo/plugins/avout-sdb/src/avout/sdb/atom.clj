(ns avout.sdb.atom
  (:use avout.state)
  (:require [simpledb.core :as sdb]
            [avout.atoms :as atoms])
  (:import clojure.lang.IRef))

(deftype SDBStateContainer [client domainName name]

  StateContainer

  (initStateContainer [this]
    (when-not (seq (sdb/get-attributes client domainName name))
      (sdb/put-attributes client domainName name [{:name "value" :value (pr-str nil)}])))

  (destroyStateContainer [this]
    (sdb/delete-attributes client domainName name [{:name "value"}]))

  (getState [this]
    (let [data (sdb/get-attributes client domainName name)]
      (if (contains? data "value")
          (read-string (get data "value"))
          (throw (RuntimeException. "sdb-atom unbound")))))

  (setState [this newValue]
    (sdb/put-attributes client domainName name [{:name "value" :value (pr-str newValue)}]))

  (compareAndSwap [this oldValue newValue]
    (sdb/put-attributes client domainName name
                        [{:name "value" :value (pr-str newValue)}]
                        {:name "value" :value (pr-str oldValue)})))


(defn sdb-atom
  ([sdb-client domain-name name init-value & {:keys [validator]}]
     (doto (avout.atoms.DistributedAtom. sdb-client domain-name name
                                         (SDBStateContainer. sdb-client domain-name name)
                                         (atom validator))
       .init
       (.reset init-value)))
  ([sdb-client domain-name name]
     (doto (avout.atoms.DistributedAtom. sdb-client domain-name name
                                         (SDBStateContainer. sdb-client domain-name name)
                                         (atom nil))
       .init)))

(defn sdb-initializer
  ([name {:keys [sdb-client domain-name]}]
     (sdb-atom sdb-client domain-name name))
  ([name init-value {:keys [sdb-client domain-name]}]
     (sdb-atom sdb-client domain-name name init-value)))

(comment
  (use 'simpledb.core)
  (use 'avout.core)
  (use 'avout.sdb.atom)
  (def ACCESS-KEY (get (System/getenv) "AWS_ACCESS_KEY"))
  (def SECRET-KEY (get (System/getenv) "AWS_SECRET_KEY"))
  (def sdb (sdb-client ACCESS-KEY SECRET-KEY))

  (def a0 (sdb-atom sdb "test-domain" "atest" 0))
  @a
  (swap!! a0 inc)

  (def a1 (sdb-atom sdb "test-domain" "atest" []))
  @a1
  (swap!! a1 conj 0)
  (swap!! a1 conj 1)

)