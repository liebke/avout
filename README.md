# Mazurka

A Clojure library of distributed concurrency primitives built on Apache ZooKeeper.

Currently, there is a distributed implementation of java.util.concurrent.locks.Lock modeled after ReentrantLock. DistributedReentrantReadWriteLock will be the next distributed lock to implement. It will then be used to build a distributed implementation of Clojure's LockingTransaction, which then enables the creation of distributed versions of Clojure's concurrency primitives, Refs, Atoms, and Pods.

The plan for the first implementations of Refs and Atoms (ZKDataRef and ZKDataAtom) will use the ZooKeeper znode's data field on the distributed-lock used for the transaction to hold the serialized Clojure form (method of serialization TBD), and provide a mechanism for distributing the value to all the participating clients. This first phase provides idiomatic Clojure access to ZooKeeper data fields, the next phase will generalize the distributed serialization mechanism so that other methods (that don't have a 1M data size limit) can be used, e.g direct peer-to-peer serialization, HDFS, and distributed data stores.


## mazurka.locks

The mazurka.locks namespace contains an implementation of java.util.concurrent.locks.Lock, called ZKDistributedReentrantLock, that provides a distributed lock.

### Examples


First require mazurka.zookeeper and mazurka.locks.

    (require '(zookeeper :as zk))
    (use 'mazurka.locks)
    
Then get a ZooKeeper client.    

    (def client (zk/connect "127.0.0.1"))
    
Then create a ZKDistributedReentrantLock with the distributed-lock function.

    (def lock (distributed-lock client :lock-node "/lock"))
    
    (try (.lock lock)
         ... do something
	 (finally (.unlock lock)))

The lock method blocks until the lock is obtained. It is standard practice to call lock within a try block with a finally statement that unlocks it.
	 
You can use the with-lock macro, which is equivalent to Clojure's locking macro, but designed to work with java.util.concurrent.locks.Lock instead of the traditional monitor locks.

    (with-lock lock
      ... do something)
      
The tryLock method doesn't block while waiting for a lock, instead it returns a boolean indicating whether the lock was obtained.

    (try (.tryLock lock)
      ... do something
      (finally (.unlock lock)))
      
Or use the when-lock and if-lock macros.

    (when-lock lock
       (... do something)
       
    (if-lock lock
      (... got lock, do something)
      (... didn't get lock do something else))
      
The tryLock method can take a timeout duration and units.

    (try (.tryLock lock 10 java.util.concurrent.TimeUnit/SECONDS)
      ... do something
      (finally (.unlock lock)))

The macros when-lock-with-timeout and if-lock-with-timeout are also available.

    (when-lock-with-timeout lock
       (... do something)
       
    (if-lock-with-timeout lock
      (... got lock, do something)
      (... didn't get lock do something else))



## Roadmap

1. DistributedCondition
2. DistributedReentrantReadWriteLock
3. DistributedLockingTransaction
4. DistributedRef, an implementation of Clojure's IRef
5. DisributedAtom
6. DistributedPod

## References

* ZooKeeper http://zookeeper.apache.org/
* ZooKeeper: Wait-free coordination for Internet-scale systems http://www.usenix.org/event/atc10/tech/full_papers/Hunt.pdf

## License

Copyright (C) 2011 

Distributed under the Eclipse Public License, the same as Clojure.
