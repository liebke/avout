(ns treeherd.barrier
  (:require [treeherd.client :as tc])
  (:import (java.net InetAddress)))

(defn host-name
  ([] (-> (InetAddress/getLocalHost) .getCanonicalHostName)))

(defn enter-barrier
  "
  Examples:

    (use '(treeherd client barrier))
    (def treeherd (client \"127.0.0.1:2181\"))
    (defn make-processor [i] (fn [] (println (str \"process \" i \" is running \")) i))
    (delete-all treeherd \"/barrier\")

    (def f0 (enter-barrier treeherd \"/barrier\" (make-processor 0) 3))

    (def f1 (enter-barrier treeherd \"/barrier\" (make-processor 1) 3 :proc-name \"proc2\"))

    (def f2 (enter-barrier treeherd \"/barrier\" (make-processor 2) 3 :proc-name \"proc3\"))

    [@f0 @f1 @f2]
"
  ([client barrier-node f N & {:keys [proc-name] :or {proc-name (host-name)}}]
     (future
       (let [mutex (Object.)
             watcher (fn [event] (locking mutex (.notify mutex)))]
         (locking mutex
           (if-not (tc/exists client barrier-node)
             (tc/create client barrier-node :persistent? true))
           (tc/create client (str barrier-node "/" proc-name))
           (loop []
             (if (>= (count (tc/children client barrier-node :watcher watcher)) N)
               (f)
               (do
                 (.wait mutex)
                 (recur)))))))))