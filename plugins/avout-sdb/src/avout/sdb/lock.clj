(ns avout.sdb.lock
  (:use avout.core
        avout.sdb.atom)
  (:import (java.util.concurrent.locks Lock)))

(deftype SDBLock [client domain-name name locked flag]
  Lock
  (lock [this] (throw (UnsupportedOperationException.)))

  (tryLock [this]
    (if @locked
      true
      (reset! locked (.compareAndSet flag false true))))

  (unlock [this] (when @locked
                   (do (.reset flag false)
                       (reset! locked false))))

  (lockInterruptibly [this] (throw (UnsupportedOperationException.)))

  (newCondition [this] (throw (UnsupportedOperationException.))))

(defn sdb-lock [client domain-name name]
  (let [flag (sdb-atom client domain-name name)
        lock (SDBLock. client domain-name name (atom false) flag)]
    (.compareAndSet flag nil false)
    lock))