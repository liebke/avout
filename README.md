# About Avout

Avout is a Clojure library of distributed concurrency primitives (built on <a href="http://zookeeper.apache.org">ZooKeeper</a> with <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a>), including an extensible, distributed STM for managing the state of remote, heterogeneous resources with the same atomicity, consistency, and isolation that <a href="http://clojure.org/refs">Clojure's STM</a> provides when <a href="http://clojure.org/state">managing local state</a>.


## avout.transaction

### Transaction Reference Protocols

Transactions are system- and thread-local (as are Clojure's dosync transactions) but the transaction references (Refs) that participate and processes that access them may be distributed across multiple JVMs and/or systems. 

New types of Refs can be created by implementing the **ReferenceData** protocol.

    (defprotocol ReferenceData
      (name [this] "Returns the ZooKeeper node name associated with this reference.")
      (set [this value point] "Sets the transaction-value associated with the given clock point.")
      (get [this point] "Returns the value associated with given clock point."))
      
The default ReferenceData implementation, ZKRef, uses the ZooKeeper data fields of the reference's history nodes (see ZooKeeper STM recipe below for details) to hold the values for the reference. Other implementations that access RESTful services, (No)SQL databases, or other data services can participate in transactions together.

**TransactionReference** is a protocol that support common reference behaviors.

    (defprotocol TransactionReference
      (ref-set [this value])
      (commute [this f & args])
      (alter [this f & args])
      (ensure [this]))


A *ReferenceData* holds a limited history of the values it has been set to over its lifetime, keyed by the commit-point, i.e. the clock tick of the transaction manager, when the value was set. Uncommitted values may exist in a *ReferenceData*. The transaction manager can be queried to determine if the commit-point associated with a given value in a *ReferenceData* was in fact committed.

The following figure illustrates Clojure's standard MVCC transaction process.

<img src="https://github.com/liebke/avout/raw/master/docs/images/avout-stm.png" />


## ZooKeeper Recipe for Distributed MVCC STM

The Avout distributed STM is built on <a href="http://zookeeper.apache.org">Apache ZooKeeper</a> with <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a>. The following is a recipe, in the style of <a href="http://zookeeper.apache.org/doc/trunk/recipes.html">Zookeeper Recipes and Solutions</a>, for building a distributed STM modeled on Clojure's in-memory STM.


<img src="https://github.com/liebke/avout/raw/master/docs/images/deref.png" />


### Performing deref outside a transaction: (deref r) or @r

1. Create a new **ZKDistributedReentrantReadWriteLock** using **/ref-name/lock** as the lock-node, where ref-name is the value returned from the **ReferenceData** **name** method, then acquire a read-lock on the reference.

3. Find the most recently committed history-node for each reference by finding the node, **/ref-name/history/TXID-t-xxxxxxxxxx**, with the largest value of xxxxxxxxxx where the data field for **/stm/history/TXID** equals **COMMITTED**. If there is no committed value, throw an exception.

4. Invoke the *get* method on the *ReferenceData*, passing the value **t-xxxxxxxx** found in the previous step.

5. Release the read-lock on all references.



### Performing deref inside a transaction: (dosync (deref r)) or (dosync @r)

The following steps are performed repeatedly until **DONE** is set to true or until **RETRY_MAX** is exceeded.

1. Start a transaction by creating a persistent, sequential node **/stm/history/t-** that represents the **start-point** for the transaction, and will act as the transaction's ID (**TXID**), setting it's data field to the value **RUNNING**. Record the current system time as **start-time**.

2. For each reference participating in the transaction, create a new **ZKDistributedReentrantReadWriteLock** (which is based on <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#Shared+Locks">this recipe</a>) using **/ref-name/lock** as the lock-node, where ref-name is the value returned from the **ReferenceData** *name* method; then acquire write-locks for the reference. If any lock cannot be acquired, set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.

3. Find the most recently committed history-node for each reference by finding the node, **/ref-name/history/TXID-t-xxxxxxxxxx**, with the largest value of xxxxxxxxxx that is less than, or equal to, the current start-point and where the data field for **/stm/history/TXID** equals **COMMITTED**. If there is no point earlier than the start-point, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.
  * If the total number of history nodes, **N**, in the step above is greater than **MAX-HISTORY**, submit asynchronous delete requests for the earliest (**N** - **MAX_HISTORY**) nodes (ensure that the most recently committed history-node is not discarded because of a more recent uncommitted nodes).

4. Set the state of the transaction to COMMITTING by using **compare-and-set-data** (i.e. CAS) to set the data field of **/stm/history/TXID** to **COMMITTING** if and only if its current state is **RUNNING**.

5. Set **DONE** to true and the state of the transaction to **COMMITTED** by setting the data field of **/stm/history/TXID** to **COMMITTED**.

6. Release read-lock on references.



<img src="https://github.com/liebke/avout/raw/master/docs/images/ref-set.png" />

### Performing ref-set in a transaction: (dosync (ref-set r v))

The following steps are performed repeatedly until **DONE** is set to true or until **RETRY_MAX** is exceeded.

1. Start a transaction by creating a persistent, sequential node **/stm/history/t-** that represents the **start-point** for the transaction, and will act as the transaction's ID (**TXID**), setting it's data field to the value **RUNNING**. Record the current system time as **start-time**.

2. For each reference participating in the transaction, create a new **ZKDistributedReentrantReadWriteLock** (which is based on <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#Shared+Locks">this recipe</a>) using **/ref-name/lock** as the lock-node, where **ref-name** is the value returned from the **ReferenceData** **name** method; then acquire a write-lock for each reference. If any lock cannot be acquired, set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.

3. Find the most recently committed history-node for each reference by finding the node, **/ref-name/history/TXID-t-xxxxxxxxxx**, with the largest value of xxxxxxxxxx that is less than, or equal to, the current start-point and where the data field for **/stm/history/TXID** equals **COMMITTED**. If there is no point earlier than the start-point, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.
  * If the total number of history nodes, **N**, in the step above is greater than **MAX-HISTORY**, submit asynchronous delete requests for the earliest (**N** - **MAX_HISTORY**) nodes (ensure that the most recently committed history-node is not discarded because of a more recent uncommitted nodes).

4. Check if another transaction has tagged the reference by extracting the **TXID** value from the data field of **/ref-name**. If the value is not nil, determine if the other transaction is currently running by extracting its state from the data field of **/stm/history/TXID** and checking whether it's equal to either **RUNNING** or **COMMITTING**.
  * If the reference is tagged by a running transaction, calculate the time elapsed since the **start-time** recorded in step 1. If the elapsed time is greater than **BARGE-WAIT-TIME**, and the current transaction's **start-point** is earlier than the other transaction's start-point
    * then **barge** the other transaction by using **compare-and-set-data** (i.e. CAS) to set the data field of **/stm/history/OTHER-TXID** to **KILLED** if and only if its current state is **RUNNING**. If the CAS succeeded, tag the reference by setting the the data field of **/ref-name** to **TXID**, then unlock the write-lock on the reference. If the CAS failed, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.
    * else if the barge could not be attempted, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.
  
5. Set the state of the transaction to **COMMITTING** by using **compare-and-set-data** (i.e. CAS) to set the data field of **/stm/history/TXID** to **COMMITTING** if and only if its current state is **RUNNING**.

6. Acquire the distributed write-lock on the reference.

7. Create a persistent, sequential node **/stm/history/t-** that represents the **commit-point** for this transaction.

8. Validate the value.

9. Call the **set** method for each **ReferenceData**, passing the new (validated) value and the **commit-point**.

10. Notify watchers.

11. Set **DONE** to true and the state of the transaction to **COMMITTED** by setting the data field of **/stm/history/TXID** to **COMMITTED**.

12. release the write-lock on all refs in the transaction.


<img src="https://github.com/liebke/avout/raw/master/docs/images/alter.png" />



## ZKRef

ZKRef implements the clojure.lang.IRef interface and the ReferenceData, Commute, Alter, and Ensure protocols. ZKRef uses the data field of the reference's tval nodes to hold its values.


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
