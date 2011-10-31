# About Avout

Avout is a Clojure library of distributed concurrency primitives (built on <a href="http://zookeeper.apache.org">ZooKeeper</a> with <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a>), including an extensible, distributed STM for managing the state of remote, heterogeneous resources with the same atomicity, consistency, and isolation that <a href="http://clojure.org/refs">Clojure's STM</a> provides when <a href="http://clojure.org/state">managing local state</a>.


## avout.transaction

### Transaction Reference Protocols

Transactions are system- and thread-local (as are Clojure's dosync transactions) but the transaction references (Refs) that participate and processes that access them may be distributed across multiple JVMs and/or systems. 

New types of Refs can be created by implementing the **TransactionReference** protocol.

    (defprotocol TransactionReference
      (name [this] "Returns the ZooKeeper node name associated with this reference.")
      (set [this value point] "Sets the transaction-value associated with the given clock point.")
      (get [this point] "Returns the value associated with given clock point."))
      
The default TransactionReference implementation, ZKRef, uses the ZooKeeper data fields of the reference's tval nodes (see ZooKeeper STM recipe below for details) to hold the values for the reference. Other implementations that access RESTful services, (No)SQL databases, or other data services can participate in transactions together.

**Commute**, **Alter**, and **Ensure** are optional protocols to support common reference behaviors.

    (defprotocol Commute
      (commute [this f & args]))

    (defprotocol Alter
      (alter [this f & args]))

    (defprotocol Ensure
      (ensure [this]))


A TransactionReference holds all the values it has been set to over its lifetime, keyed by the commit-point, i.e. the clock tick of the transaction manager, when the value was set. Uncommitted values may exist in a TransactionReference. The transaction manager can be queried to determine if the commit-point associated with a given value in a TransactionReference was in fact committed.

<img src="https://github.com/liebke/avout/raw/master/docs/images/transref.png" />


The following figure illustrates Clojure's standard MVCC transaction process.


<img src="https://github.com/liebke/avout/raw/master/docs/images/avout-stm.png" />


## ZooKeeper Recipe for MVCC Locking Transaction using Instances of TransactionReference

The Avout STM is built on <a href="http://zookeeper.apache.org">Apache ZooKeeper</a> with <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a>. The following is a recipe, in the style of <a href="http://zookeeper.apache.org/doc/trunk/recipes.html">Zookeeper Recipes and Solutions</a>, for building a distributed STM.

Setting values in a transaction:

1. Start a transaction by creating a persistent, sequential node **/stm/clock/t-** that represents the read-point for the transaction.
2. Create a new **ZKDistributedReentrantReadWriteLock** (which is based on <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#Shared+Locks">this recipe</a>) for each **TransactionReference** participating in the transaction using **/ref-name/lock** as the lock-node, where ref-name is the value returned from the **TransactionReference** *name* method; then acquire write-locks for each reference that will be altered during the transaction, and read-locks for those that will only be read. If any lock cannot be acquired or any other exception occurs during the remaining steps, end the transaction (and clean up) and then retry it (goto step 1) until success or RETRY_MAX is reached.
3. Find the most recent committed transaction-value node for each *TransactionReference* by finding the node **/ref-name/tvals/t-xxxxxxxxxx** such that the integer xxxxxxxxxx is less than, or equal to, the current read-point and **/stm/clock/t-xxxxxxxxxx/COMMITTED** exists. 
4. Make local copies of the current values for each *TransactionReference* at the latest committed point earlier than this transactions read-point by calling the *get* method on each *TransactionReference* passing the point extracted from the last committed transaction-value node.
5. Apply the respective functions (passed in via the *alter* and *commute* functions) to the current values for each *TransactionReference*, updating the local cache.
6. Create a persistent, sequential node **stm/clock/t-** that represents the commit-point for this transaction.
7. Call the *set* method for each *TransactionReference*, passing the new value and the commit-point extracted from the node created in the previous step.
8. Once all the *TransactionReference* values have been updated, create a persistent node named **/stm/clock/t-xxxxxxxxxx/COMMITTED** to indicate that the transaction has been committed, and all references with tvals at t-xxxxxxxxxx are committed.
9. release the locks on all refs in the transaction.


To invoke *deref* on a *TransactionReference* outside of a transaction: 

1. Create a new **ZKDistributedReentrantReadWriteLock** using **/ref-name/lock** as the lock-node, where ref-name is the value returned from the **TransactionReference** *name* method, then acquire a read-lock on the reference.
2. Find the most recent committed transaction-value node for the *TransactionReference* by finding the node **/ref-name/tvals/t-xxxxxxxxxx** such that the integer xxxxxxxxxx is less than, or equal to, the most recent clock tick, **/stm/clock/t-xxxxxxxxxx**, where **/stm/clock/t-xxxxxxxxxx/COMMITTED** exists.
3. Invoke the *get* method on the *TransactionReference*
4. Release the read-lock


## ZKRef

ZKRef implements the clojure.lang.IRef interface and the TransactionReference, Commute, Alter, and Ensure protocols. ZKRef uses the data field of the reference's tval nodes to hold its values.


## avout.locks

The avout.locks namespace contains an implementation of java.util.concurrent.locks.Lock, called ZKDistributedReentrantLock, and an implementation of java.util.concurrent.locks.ReadWriteLock, called ZKDistributedReentrantReadWriteLock.

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



## References

* <a href="http://clojure.org/state">Clojure State</a>
* <a href="http://clojure.org/refs">Clojure Refs and Transactions</a>
* <a href="http://en.wikipedia.org/wiki/Software_transactional_memory">Software Transactional Memory</a>
* <a href="http://en.wikipedia.org/wiki/Multiversion_concurrency_control">Multiversion Concurrency Control</a>
* <a href="http://en.wikipedia.org/wiki/Snapshot_isolation">Snapshot Isolation</a>
* <a href="http://zookeeper.apache.org/">ZooKeeper Website</a>
* <a href="http://www.usenix.org/event/atc10/tech/full_papers/Hunt.pdf">ZooKeeper: Wait-free coordination for Internet-scale systems</a>
* <a href="http://zookeeper.apache.org/doc/trunk/recipes.html">ZooKeeper Recipes and Solutions</a>


## License

Copyright (C) 2011 

Distributed under the Eclipse Public License, the same as Clojure.
