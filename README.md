# Treeherd

A Clojure DSL for Apache ZooKeeper and library of distributed concurrency primitives.

Currently, there is a distributed implementation of java.util.concurrent.locks.Lock modeled after ReentrantLock. DistributedReentrantReadWriteLock will be the next distributed lock to implement. It will then be used to build distributed implementations of Clojure's concurrency primitives, Refs, Atoms, and Pods.


## treeherd.zookeeper

The treeherd.zookeeper namespace contains the ZooKeeper DSL.


## treeherd.locks

The treeherd.locks namespace contains an implementation of java.util.concurrent.locks.Lock, called ZKDistributedReentrantLock, that provides a distributed lock.

### Examples


First require treeherd.zookeeper and treeherd.locks.

    (require '(treeherd.zookeeper :as zk))
    (use 'treeherd.locks)
    
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



# Roadmap

1. DistributedCondition
2. DistributedReentrantReadWriteLock
3. DistributedRef, an implementation of Clojure's IRef
4. DisributedAtom
5. DistributedPod

## License

Copyright (C) 2011 

Distributed under the Eclipse Public License, the same as Clojure.
