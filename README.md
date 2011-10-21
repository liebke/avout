# Treeherd

A Clojure distributed lock library and DSL for Apache ZooKeeper.

## treeherd.zookeeper

The treeherd.zookeeper namespace contains functions for working with ZooKeeper. 

## treeherd.locks

The treeherd.locks namespace contains an implementation of java.util.concurrent.locks.Lock, called ZKDistributedReentrantLock, that provides a distributed lock.

### Examples

First get a ZooKeeper client.

    (use '(treeherd client locks))
    (def client (connect "127.0.0.1"))
    
Then create a ZKDistributedReentrantLock with the distributed-lock function.

    (def lock (distributed-lock client :lock-node "/lock"))
    
    (try (.lock lock)
         ... do something
	 (finally (.unlock lock)))

The lock method blocks until the lock is obtained. It is standard practice to call lock within a try block with a finally statement that unlocks it.
	 
You can use the with-lock macro, which is equivalent to Clojure's locking macro, but designed to work with java.util.concurrent.locks.Lock instead of the traditional monitor locks.

    (with-lock lock
      ... do something)
      
The tryLock method doesn't block while waiting for a lock, as the try method does, but instead returns boolean indicating success in obtaining the lock.

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


Conditions Coming Soon ...

## License

Copyright (C) 2011 

Distributed under the Eclipse Public License, the same as Clojure.
