(ns avout.util
  (:require [zookeeper.data :as data]))

(defn serialize-form
  "Serializes a Clojure form to a byte-array."
  ([form]
     (data/to-bytes (pr-str form))))

(defn deserialize-form
  "Deserializes a byte-array to a Clojure form."
  ([form]
     (when form (read-string (data/to-string form)))))
