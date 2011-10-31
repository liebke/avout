# Avout

Avout is a Clojure library of distributed concurrency primitives built on Apache ZooKeeper.

Currently, there is a distributed implementation of java.util.concurrent.locks.Lock modeled after ReentrantLock. DistributedReentrantReadWriteLock will be the next distributed lock to implement. It will then be used to build a distributed implementation of Clojure's LockingTransaction, which then enables the creation of distributed versions of Clojure's concurrency primitives, Refs, Atoms, and Pods.


## avout.transaction


<img src="https://github.com/liebke/avout/raw/master/docs/images/avout-stm.png" />

## ZooKeeper Recipe for MVCC Locking Transaction using Instances of TransactionReference

To run a transaction:

1. Start a transaction by creating a persistent, sequential node **/stm/clock/t-** that represents the read-point for the transaction.
2. Create a new **ZKDistributedReentrantReadWriteLock** using **/ref-name/lock** as the lock-node, then acquire write-locks for each reference that will be altered during the transaction, and read-locks for those that will only be read (if any lock cannot be acquired or any other exception occurs during the remaining steps, end the transaction (and clean up) and then retry it (goto step 1) until success or RETRY_MAX is reached).
3. Find the most recent committed transaction-value node for each *TransactionReference* by finding the node **/ref-name/tvals/t-xxxxxxxxxx** such that the integer xxxxxxxxxx is less than, or equal to, the current read-point and **/stm/clock/t-xxxxxxxxxx/COMMITTED** exists. 
4. Make local copies of the current values for each *TransactionReference* at the latest committed point earlier than this transactions read-point by calling the *get* method on each *TransactionReference* passing the point extracted from the last committed transaction-value node.
5. Apply the respective functions (passed in via *alter* and *commute* functions) to the current values for each *TransactionReference*, updating the local cache.
6. Create a persistent, sequential node **stm/clock/t-** that represents the commit-point for this transaction.
7. Call the *set* method for each *TransactionReference*, passing the new value and the commit-point extracted from the node created in the previous step.
8. Once all the *TransactionReference* values have been updated, create a persistent node named **/tmp/clock/t-xxxxxxxxxx/COMMITTED** to indicate that the transaction has been committed, and all references with tvals at t-xxxxxxxxxx are committed.
9. release the locks on all refs in the transaction.


To invoke *deref* on a *TransactionReference* outside of a transaction: 

1. Create a new **ZKDistributedReentrantReadWriteLock** using **/ref-name/lock** as the lock-node, then acquire a read-lock on the reference.
2. Find the most recent committed transaction-value node for the *TransactionReference* by finding the node **/ref-name/tvals/t-xxxxxxxxxx** such that the integer xxxxxxxxxx is less than, or equal to, the most recent clock tick, **/stm/clock/t-xxxxxxxxxx**, where **/stm/clock/t-xxxxxxxxxx/COMMITTED** exists.
3. Invoke the *get* method on the *TransactionReference*
4. Release the read-lock



### Transaction Protocols

The following protocols can be used to create transaction references that can be used by avout.transaction.

    (defprotocol TransactionReference
      (set [this value point] "Returns the ZKDistributedReentrantReadWriteLock associated with this reference.")
      (get [this point] "Returns the value associated with given clock point."))
      
    (defprotocol Commute
      (commute [this f & args]))

    (defprotocol Alter
      (alter [this f & args]))

    (defprotocol Ensure
      (ensure [this]))

<img src="https://github.com/liebke/avout/raw/master/docs/images/transref.png" />


ZKRef implements the clojure.lang.IRef interface and the TransactionReference, Commute, Alter, and Ensure protocols.


## avout.locks

The avout.locks namespace contains an implementation of java.util.concurrent.locks.Lock, called ZKDistributedReentrantLock, that provides a distributed lock.

<img src="https://github.com/liebke/avout/raw/master/docs/images/locks.png" />

### Examples


First require avout.zookeeper and avout.locks.

    (require '(zookeeper :as zk))
    (use 'avout.locks)
    
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
