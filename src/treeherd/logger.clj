(ns treeherd.logger
  (:import [java.util.logging Level LogManager]))

(defn root-logger []
  (.getLogger (java.util.logging.LogManager/getLogManager) ""))

(def levels {:off java.util.logging.Level/OFF
             :severe java.util.logging.Level/SEVERE
             :info java.util.logging.Level/INFO
             :all java.util.logging.Level/ALL
             :warning java.util.logging.Level/WARNING
             :finest java.util.logging.Level/FINEST
             :fine java.util.logging.Level/FINE})

(defn log-level
  ([level]
     (.setLevel (root-logger) (levels level))))

(defn log
  ([level & msg]
     (.log (root-logger) (levels level) (apply str msg))))

(defn info
  ([& msg]
     (.info (root-logger) (apply str msg))))

(defn debug
  ([& msg]
     (.info (root-logger) (apply str msg))))

(defn warning
  ([& msg]
     (.warning (root-logger) (apply str msg))))

(defn error
  ([& msg]
     (.severe (root-logger) (apply str msg))))
