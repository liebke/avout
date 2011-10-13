(ns treeherd.data
  (:import (java.nio ByteBuffer)))

(defmacro get-bytes [value size f]
  `(let [bytes# (make-array Byte/TYPE ~size)
          buf# (-> (ByteBuffer/allocateDirect ~size)
                  (~f ~value)
                  .clear
                  (.get bytes# 0 ~size))]
    bytes#))

(defprotocol ByteConverter
  (to-bytes [this] "Converts value to a byte array"))

(extend-type String
  ByteConverter
  (to-bytes [this] (.getBytes this)))

(extend-type Integer
  ByteConverter
  (to-bytes [i]
    (get-bytes i 4 .putInt)))

(extend-type Double
  ByteConverter
  (to-bytes [d]
    (get-bytes d 8 .putDouble)))

(extend-type Long
  ByteConverter
  (to-bytes [l]
    (get-bytes l 8 .putLong)))

(extend-type Float
  ByteConverter
  (to-bytes [f]
    (get-bytes f 4 .putFloat)))

(extend-type Short
  ByteConverter
  (to-bytes [s]
    (get-bytes s 2 .putShort)))

(extend-type Character
  ByteConverter
  (to-bytes [c]
    (get-bytes c 2 .putChar)))

(defn to-string
  ([bytes](String. bytes)))

(defn to-int
  ([bytes]
     (.getInt (ByteBuffer/wrap bytes))))

(defn to-long
  ([bytes]
     (.getLong (ByteBuffer/wrap bytes))))

(defn to-double
  ([bytes]
     (.getDouble (ByteBuffer/wrap bytes))))

(defn to-float
  ([bytes]
     (.getFloat (ByteBuffer/wrap bytes))))

(defn to-short
  ([bytes]
     (.getShort (ByteBuffer/wrap bytes))))

(defn to-char
  ([bytes]
     (.getChar (ByteBuffer/wrap bytes))))

