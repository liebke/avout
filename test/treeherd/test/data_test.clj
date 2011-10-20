(ns treeherd.test.data_test
  (:use [treeherd.data])
  (:use [clojure.test]))

(deftest to-from-bytes
  (is (= "hello" (to-string (to-bytes "hello"))))
  (is (= 1234 (to-int (to-bytes (Integer/valueOf 1234)))))
  (is (= 1234 (to-long (to-bytes 1234))))
  (is (= 123.456 (to-double (to-bytes 123.456))))
  (is (= (float 123.456) (to-float (to-bytes (float 123.456)))))
  (is (= 12 (to-short (to-bytes (short 12)))))
  (is (= \a (to-char (to-bytes \a)))))