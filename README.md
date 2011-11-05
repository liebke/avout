# About Avout

*Avout* extends Clojure's syntax and semantics for managing <a href="http://clojure.org/state">in-memory state</a> to heterogeneous types of distributed state by providing distributed and extendable versions of Clojure's <a href="http://clojure.org/atoms">Atoms</a>, <a href="http://clojure.org/refs">Refs</a>, and <a href="http://clojure.org/agents">Agents</a>. 

*Avout* is built on <a href="http://zookeeper.apache.org">ZooKeeper</a>, with <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a>, and also includes distributed implementations of <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/Lock.html">*java.util.concurrent.lock.Lock*</a>, <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/ReadWriteLock.html">*java.util.concurrent.lock.ReadWriteLock*</a>.


## Design of avout.atom

Atomic references depend on the ability to compare-and-swap (CAS) values, meaning only update the atom to a new value if it's current value is what I expect. Clojure's in-memory Atom uses a java.util.concurrent.atomic.AtomicReference to perform this function. 

ZooKeeper provides a mechanism for conditionally updating a node's data field based on version numbers. To build a CAS function, which I'll call compare-and-set-data, use the following procedure:

1. Call the data function on the node you want to update. This will return both its data value and a status map containing the current version number.

2. Compare the data to its expected value, if they are different, you're done.

3. If the current data value equals the expected value, then call set-data on the node with the new value and the node's current data version number. 
  * If the data hasn't been updated since you requested it, the version will not have changed and the update will work. 
  * If the data has been updated, the version number will have been automatically incremented, and the version number we provided won't match, so a KeeperException$BadVersionException will be thrown, catch it and call compare-and-set-data again, in case the updated data value is still equal to the expected-value.

Here's the implementation of compare-and-set-data from the zookeeper-clj library:

    (defn compare-and-set-data 
      ([client node expected-value new-value]
         (let [{:keys [data stat]} (data client node)
               version (:version stat)]
           (try
             (when (Arrays/equals data expected-value)
               (set-data client node new-value version))
             (catch KeeperException$BadVersionException e
               (compare-and-set-data client node expected-value new-value))))))
	      

If we abstract the CAS behavior into the following **AtomState** protocol,

    (defprotocol AtomState
      (getValue [this])
      (setValue [this value]))
      
we can swap out ZooKeeper as the atomic data holder (and swap out its 1M data size limit) with any other mechanism that supports the CAS semantics and the **AtomState** protocol, while still using ZooKeeper for managing notifications of the distributed set of watchers.

The ZooKeeper-backed **ZKAtom** implements the **AtomState** protocol, the **clojure.lang.IDeref** interface, and the following **AtomReference** protocol.

    (defprotocol AtomReference
      (swap [this f])
      (swap [this f & args])
      (reset [this new-value]))
      
To create a new type of atom backed by your favorite network-accessible, CAS-capable data store, implement the **AtomState** protocol, and pass an instance of it to **DistributedAtom**, which like ZKAtom implements the **clojure.lang.IDeref** interface, the **AtomReference** protocol, and the **AtomState** protocol, but just delegates calls to **compareAndSet** and **get** to the passed-in instance of **AtomState**. **DistributedAtom** is responsible for using ZooKeeper to notify watchers of changes to the atom, and providing implementations of deref, swap, and reset that use the passed-in **AtomState** instance to manage the value.

    (deftype DistributedAtom [client atomData]
      AtomState
      (compareAndSet [this expected-value new-value] (.compareAndSet atomData expected-value new-value))
      (get [this] (.get atomData))
      IDeref
      (deref [this] (.get atomData))
      AtomReference
      (swap [this f] ...)
      (swap [this f & args] ...)
      (reset [this new-value] ...))



## Design of avout.ref

### Transaction Reference Protocols

Transactions support synchronous change to multiple References. Transactions are system- and thread-local (as are Clojure's dosync transactions) but the transaction references (Refs) that participate and processes that access them may be distributed across multiple JVMs and/or systems. 

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

To create a new type of reference backed by your favorite network-accessible data store, implement the **ReferenceData** protocol, and pass an instance of it to **ZKTransactionReference**, which is responsible for executing the transaction process using your implementation of **ReferenceData** as the data container for reference values.

    (deftype ZKTransactionReference [client referenceData]
      TransactionReference
      (ref-set [this value] ...)
      (commute [this f & args] ...)
      (alter [this f & args] ...)
      (ensure [this] ...))

The following is an illustration of Ref values changing over time.

<img src="https://github.com/liebke/avout/raw/master/docs/images/ref-over-time.png" />

The following figure illustrates Clojure's standard MVCC transaction process.

<img src="https://github.com/liebke/avout/raw/master/docs/images/avout-stm.png" />


## ZooKeeper Recipe for A Distributed MVCC STM

The Avout distributed STM is built on <a href="http://zookeeper.apache.org">Apache ZooKeeper</a> with <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a>. The following is a recipe, in the style of <a href="http://zookeeper.apache.org/doc/trunk/recipes.html">Zookeeper Recipes and Solutions</a>, for building a distributed STM modeled on Clojure's in-memory STM.

The following ZooKeeper nodes are used by the STM.

* **/stm/history/t-** (persistent, sequential), will be one for every tick of the STM clock upto MAX-STM-HISTORY, read-points are used as TXIDs.
* **/ref-name/lock/n-** (ephemeral, sequential) used in the read-write lock protocol
* **/ref-name/history/TXID-COMMIT_POINT** (persistent) will be upto MAX-HISTORY of these nodes
* **/ref-name/txn/TXID** (ephemeral) will only be one at a time


The following nodes will need to be created the first time the STM runs.

* **/stm**
* **/stm/history**

The following nodes will need to be created for every new reference.

* **/ref-name** (persistent)
* **/ref-name/lock** (persistent)
* **/ref-name/history** (persistent)
* **/ref-name/txn** (persistent)


<img src="https://github.com/liebke/avout/raw/master/docs/images/deref.png" />


### Performing deref Outside a Transaction: (deref r) or @r

1. Create a new **ZKDistributedReentrantReadWriteLock** using **/ref-name/lock** as the lock-node, where ref-name is the value returned from the **ReferenceData** **name** method, then acquire a read-lock on the reference.

3. Find the most recently committed history-node for each reference by finding the node, **/ref-name/history/TXID-t-xxxxxxxxxx**, with the largest value of xxxxxxxxxx where the data field for **/stm/history/TXID** equals **COMMITTED**. If there is no committed value, throw an exception.

4. Invoke the *get* method on the *ReferenceData*, passing the value **t-xxxxxxxx** found in the previous step.

5. Release the read-lock on all references.

### ZooKeeper Reads and Writes for deref Outside Transaction

1. (1 W) create /ref-name/lock/n- (one per ref in transaction)
2. (1 R) children /ref-name/lock (one per ref in transaction)
3. (1 R) children /ref-name/history (one per ref in transaction)
4. (1+ R) data /stm/history/t-xxxxxxxxxx (may be multiple calls per reference)
5. (1+ D) optionally delete old history nodes (should usually be one once a ref history > MAX_HISTORY)
6. (1 R) get-data /ref-name/history/TXID-COMMIT-POINT
7. (1 D) delete /ref-name/lock/n-xxxxxxxxxx


### Performing deref Inside a Transaction: (dosync (deref r)) or (dosync @r)

The following steps are performed repeatedly until **DONE** is set to true or until **RETRY_MAX** is exceeded.

1. Start a transaction by creating a persistent, sequential node **/stm/history/t-** that represents the **start-point** for the transaction, and will act as the transaction's ID (**TXID**), setting it's data field to the value **RUNNING**. Record the current system time as **start-time**.

2. For each reference participating in the transaction, create a new **ZKDistributedReentrantReadWriteLock** (which is based on <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#Shared+Locks">this recipe</a>) using **/ref-name/lock** as the lock-node, where ref-name is the value returned from the **ReferenceData** *name* method; then acquire write-locks for the reference. If any lock cannot be acquired, set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.

3. Find the most recently committed history-node for each reference by finding the node, **/ref-name/history/TXID-t-xxxxxxxxxx**, with the largest value of xxxxxxxxxx that is less than, or equal to, the current start-point and where the data field for **/stm/history/TXID** equals **COMMITTED**. If there is no point earlier than the start-point, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.
  * If the total number of history nodes, **N**, in the step above is greater than **MAX-HISTORY**, submit asynchronous delete requests for the earliest (**N** - **MAX_HISTORY**) nodes (ensure that the most recently committed history-node is not discarded because of a more recent uncommitted nodes).

4. Set the state of the transaction to COMMITTING by using **compare-and-set-data** (i.e. CAS) to set the data field of **/stm/history/TXID** to **COMMITTING** if and only if its current state is **RUNNING**.

5. Set **DONE** to true and the state of the transaction to **COMMITTED** by setting the data field of **/stm/history/TXID** to **COMMITTED**.

6. Release read-lock on references.

### ZooKeeper Reads and Writes for deref Inside Transaction

1. (1 W) create /stm/history/t- with data (one per ref in transaction)
2. (1 W) create /ref-name/lock/n- (one per ref in transaction)
3. (1 R) children /ref-name/lock (one per ref in transaction)
4. (1 R) children /ref-name/history (one per ref in transaction)
5. (1+ R) data /stm/history/t-xxxxxxxxxx (may be multiple calls per reference)
6. (1+ D) optionally delete old history nodes (should usually be one once a ref history > MAX_HISTORY)
7. (1 R, 1 W) compare-and-set-data /stm/history/TXID (two calls, data, set-data)
8. (1 W) set-data /stm/history/TXID
9. (1 D) delete /ref-name/lock/n-xxxxxxxxxx


<img src="https://github.com/liebke/avout/raw/master/docs/images/ref-set.png" />

### Performing ref-set: (dosync (ref-set r v))

The following steps are performed repeatedly until **DONE** is set to true or until **RETRY_MAX** is exceeded.

1. Start a transaction by creating a persistent, sequential node **/stm/history/t-** that represents the **start-point** for the transaction, and will act as the transaction's ID (**TXID**), setting it's data field to the value **RUNNING**. Record the current system time as **start-time**.

2. For each reference participating in the transaction, create a new **ZKDistributedReentrantReadWriteLock** (which is based on <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#Shared+Locks">this recipe</a>) using **/ref-name/lock** as the lock-node, where **ref-name** is the value returned from the **ReferenceData** **name** method; then acquire a write-lock for each reference. If any lock cannot be acquired, set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.

3. Find the most recently committed history-node for each reference by finding the node, **/ref-name/history/TXID-t-xxxxxxxxxx**, with the largest value of xxxxxxxxxx that is less than, or equal to, the current start-point and where the data field for **/stm/history/TXID** equals **COMMITTED**. If there is no point earlier than the start-point, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.
  * If the total number of history nodes, **N**, in the step above is greater than **MAX-HISTORY**, submit asynchronous delete requests for the earliest (**N** - **MAX_HISTORY**) nodes (ensure that the most recently committed history-node is not discarded because of a more recent uncommitted nodes).

4. Check if another transaction has tagged the reference by listing the children of **/ref-name/txn**. If there is a child, use its name as a TXID to lookup its state by extracting the data field of **/stm/history/TXID** and checking whether it's equal to either **RUNNING** or **COMMITTING**, if so, the transaction is running.
  * If the reference is tagged by a running transaction, calculate the time elapsed since the **start-time** recorded in step 1. If the elapsed time is greater than **BARGE-WAIT-TIME**, and the current transaction's **start-point** is earlier than the other transaction's start-point
  * then **barge** the other transaction by using **compare-and-set-data** (i.e. CAS) to set the data field of **/stm/history/OTHER-TXID** to **KILLED** if and only if its current state is **RUNNING**. If the CAS succeeded, tag the reference by deleting **/ref-name/txn/OTHER-TXID** and creating the ephemeral node **/ref-name/txn/TXID** (by making it ephemeral, the tag will be released if the connection to ZooKeeper is lost), then unlock the write-lock on the reference. If the CAS failed, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.
  * else if the barge could not be attempted, then set the state of the transaction to **RETRY** by setting the data field of **/stm/history/TXID** to the value **RETRY**, clean up, and then go back to step 1 until success or **RETRY_MAX** is reached.

5. Release write-lock on references.

6. Set the state of the transaction to **COMMITTING** by using **compare-and-set-data** (i.e. CAS) to set the data field of **/stm/history/TXID** to **COMMITTING** if and only if its current state is **RUNNING**.

7. Acquire the distributed write-lock on the reference.

8. Create a persistent, sequential node **/stm/history/t-** that represents the **commit-point** for this transaction.

9. Validate the value.

10. Call the **set** method for each **ReferenceData**, passing the new (validated) value and the **commit-point**.

11. Notify watchers.

12. Set **DONE** to true and the state of the transaction to **COMMITTED** by setting the data field of **/stm/history/TXID** to **COMMITTED**.

13. release the write-lock on all refs in the transaction.


### ZooKeeper Reads and Writes for ref-set

1.  (1 W) create /stm/history/t- with data (one per ref in transaction)
2.  (1 W) create /ref-name/lock/n- (one per ref in transaction)
3.  (1 R) children /ref-name/lock (one per ref in transaction)
4.  (1 R) children /ref-name/history (one per ref in transaction)
5.  (1+ R) data /stm/history/t-xxxxxxxxxx (may be multiple calls per reference)
6.  (1+ D) optionally delete old history nodes (should usually be one once a ref history > MAX_HISTORY)
7.  (1 R) children /ref-name/txn
8.  (1- R) data /stm/history/OTHER-TXID (may not exist)
9.  (1- D) delete /ref-name/txn/OTHER-TXID (may not exist)
10.  (1 W) create /ref-name/txn/TXID
11. (1 D) delete /ref-name/lock/n-xxxxxxxxxx
12. (1 R, 1 W) compare-and-set-data /stm/history/TXID (two calls, data, set-data)
13. (1 W) create /ref-name/lock/n- (one per ref in transaction)
14. (1 R) children /ref-name/lock (one per ref in transaction)
15. (1 W) create /stm/history/t-
16. (1 W) set-data /ref-name/history/TXID-COMMIT-POINT ;; for ZKRefs only
17. (1 W) set-data /stm/history/TXID ;; for transaction state
18. (1 D) delete /ref-name/lock/n-xxxxxxxxxx



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
