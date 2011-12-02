(ns avout.core
  (:require [avout.refs :as refs]
            [avout.atoms :as atoms]
            [avout.transaction :as tx]
            [avout.config :as cfg]
            [avout.locks :as locks]
            [zookeeper :as zk]
            avout.refs.zk
            avout.refs.local
            avout.atoms.zk))

(defn init-stm
  "Called the first time the STM is used, creates necessary ZooKeeper nodes."
  ([client]
     (zk/create-all client (str cfg/*stm-node* cfg/HISTORY) :persistent? true)
     (zk/create client (str cfg/*stm-node* cfg/REFS) :persistent? true)
     (zk/create client (str cfg/*stm-node* cfg/ATOMS) :persistent? true)))

(defn reset-stm
  "Used to clear and re-initialize the STM."
  ([client]
     (zk/delete-all client cfg/*stm-node*)
     (init-stm client)))

(defn connect
  "Returns a ZooKeeper client, and initializes the STM if it doesn't already exist."
  ([& args]
     (let [client (apply zk/connect args)]
       (when-not (zk/exists client cfg/*stm-node*)
         (init-stm client))
       client)))

;; Distributed versions of Clojure's standard Ref functions

(defmacro dosync!!
  "Distributed version of Clojure's dosync macro."
  ([client & body]
     `(if (or (coll? '~client)
              (not (instance? org.apache.zookeeper.ZooKeeper ~client)))
        (throw (RuntimeException. "First argument to dosync!! must be a ZooKeeper client instance."))
        (do (tx/create-local-transaction ~client)
            (tx/run-in-transaction ~client (fn [] ~@body))))))

(defn ref-set!!
  "Distributed version of Clojure's ref-set function."
  ([ref value] (.setRef ref value)))

(defn alter!!
  "Distributed version of Clojure's alter function."
  ([ref f & args] (.alterRef ref f args)))

(defn commute!!
  "Distributed version of Clojure's commute function. Temporarily implemented
   using alter!! instead of the optimized semantics of Clojure's commute."
  ([ref f & args] (.alterRef ref f args)))

;; ZK and local Reference implementations

(defn zk-ref
  "Returns an instance of an Avout distributed Ref that uses a ZooKeeper data field
   to hold its state and Clojure's printer/reader (pr-str/read-string) for
   serialization. Note: ZooKeeper has a 1 megabyte limit on the size of data in its
   data fields."
  ([client name init-value & {:keys [validator]}]
     (let [r (doto (refs/distributed-ref client name
                                         (avout.refs.zk.ZKVersionedStateContainer.
                                          client
                                          (str cfg/*stm-node* cfg/REFS name)))
               (set-validator! validator))]
       (dosync!! client (ref-set!! r init-value))
       r))
  ([client name]
     ;; for connecting to an existing ref only
     (refs/distributed-ref client name
                           (avout.refs.zk.ZKVersionedStateContainer.
                             client
                             (str cfg/*stm-node* cfg/REFS name)))))

(defn local-ref
  "Returns an instance of an Avout Ref that holds its state locally, but can
   be used in dosync!! transactions with distributed Refs since Avout Refs
   cannot participate in dosync transactions with Clojure's in-memory Refs."
  ([client name init-value & {:keys [validator]}]
     (let [r (doto (refs/distributed-ref client name
                                         (avout.refs.local.LocalVersionedStateContainer.
                                           client
                                           (str cfg/*stm-node* cfg/REFS name) (atom {})))
               (set-validator! validator))]
       (dosync!! client (ref-set!! r init-value))
       r))
  ([client name]
     ;; for connecting to an existing ref only
     (refs/distributed-ref client name
                           (avout.refs.local.LocalVersionedStateContainer.
                             client
                             (str cfg/*stm-node* cfg/REFS name) (atom {})))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Versions of Clojure's Atom functions swap!, reset!, compare-and-set! for use with AtomReferences
;; Built-in Clojure functions that work against IRef work with AtomReferences, including
;; deref, the @ deref reader-macro, set-validator!, get-validator!, add-watch, and remove-watch

(defn swap!!
  "Cannot use standard swap! because Clojure expects a clojure.lang.Atom."
  ([atom f & args] (.swap atom f args)))

(defn reset!!
  "Cannot use standard reset! because Clojure expects a clojure.lang.Atom."
  ([atom new-value] (.reset atom new-value)))

(defn compare-and-set!!
  "Cannot use standard reset! because Clojure expects a clojure.lang.Atom."
  ([atom old-value new-value] (.compareAndSet atom old-value new-value)))

;; ZK-based atom implementation

(defn zk-atom
  "Returns an instance of an Avout distributed Atom that uses a ZooKeeper data field
   to hold its state and Clojure's printer/reader (pr-str/read-string) for
   serialization. Note: ZooKeeper has a 1 megabyte limit on the size of data in its
   data fields."
  ([client name init-value & {:keys [validator]}]
     (doto (atoms/distributed-atom client name (avout.atoms.zk.ZKStateContainer. client (str name "/data")))
       (set-validator! validator)
       (.reset init-value)))
  ([client name] ;; for connecting to an existing atom only
     (atoms/distributed-atom client name (avout.atoms.zk.ZKStateContainer. client (zk/create-all client (str name "/data"))))))

