# About Avout

**Avout** extends Clojure's syntax and semantics for managing <a href="http://clojure.org/state">in-memory state</a> to heterogeneous types of distributed state by providing distributed and extendable versions of Clojure's <a href="http://clojure.org/atoms">Atom</a> and <a href="http://clojure.org/refs">Ref</a>.

*Avout* Atoms and Refs are **ACI(D)**, providing the same promises of *atomicity*, *consistency*, and *isolation* as Clojure's in-memory versions, and optionally providing *durability*, depending on the type of Atom or Ref used.

*Avout* Atoms and Refs use <a href="http://zookeeper.apache.org">Apache ZooKeeper</a> to coordinate state change, but not necessarily to hold state. Out of the box, Avout contains an Atom, *zk-atom*, that does also use ZooKeeper data fields to store its state, and two types of Refs, *zk-ref*, also backed by ZooKeeper data fields, and *local-ref*, that is in fact not distributed but provides a mechanism for local Refs to participate in transactions with distributed Refs.

*Avout* lets you create new types of distributed Atoms and Refs using the **avout.state.StateContainer** and **avout.state.VersionedStateContainer** protocols, respectively; examples of which are the **MongoDB**-backed *mongo-atom* and *mongo-ref*, which can be found in the plugins directory.

*Avout* provides caching, so that multiple derefs of the same Atom/Ref will not need to hit either ZooKeeper nor the back-end state-store until the cache has been invalidated by a local or remote update to the Atom or Ref.

*Avout* Atoms and Refs implement Clojure's IRef interface, and therefore support functions that operate on IRefs, including: deref (and its reader-macro, @), set-validator!, add-watch, remove-watch.

*Avout* provides "double-bang" versions of the remaining core Atom and Ref functions (reset!, swap!, dosync, ref-set, alter, commute) for use with distributed Atoms and Refs: **reset!!, swap!!, dosync!!, ref-set!!, alter!!, commute!!**.


*Avout* is built on <a href="http://zookeeper.apache.org">ZooKeeper</a>, with <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a>, and also includes distributed implementations of <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/Lock.html">*java.util.concurrent.lock.Lock*</a>, <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/ReadWriteLock.html">*java.util.concurrent.lock.ReadWriteLock*</a>.

<img src="https://github.com/liebke/avout/raw/master/docs/images/ref-over-time.png" />

<img src="https://github.com/liebke/avout/raw/master/docs/images/avout-stm.png" />

# Quick Start

To get started, first load the avout.core namespace, and create a ZooKeeper client that will be passed to distributed Refs and Atoms when they are created and to dosync!! transactions when they are performed.

    (use 'avout.core)
    (def client (connect "127.0.0.1"))
    
The first time you use avout, you'll need to initialize it.

    (init-stm client)

Now create a distributed atom, you'll need to pass the ZooKeeper client, a ZooKeeper compliant name (must start with a slash), and an optional initial-value. Skip the initial value if you want to connect to an existing distributed Atom.
    
    (def a0 (zk-atom client "/a0" 0))
    
Deref it.

    @a0
    
Then you can use the swap!! and reset!! functions just as you would Clojure's built-in swap! and reset! functions, except you will need to pass the ZooKeeper client as the first parameter.

    (swap!! a0 inc)
    
    (reset!! a0 0)
    
    
Create a distributed Ref backed by ZooKeeper data fields

    (def r0 (zk-ref client "/r0" 0))
    (def r1 (zk-ref client "/r1" []))
    
    (dosync!! client
      (alter!! r0 inc)
      (alter!! r1 conj @r0))
      
    @r0
    
    @r1
      
    (dosync!! client
      (ref-set!! r0 0)
      (ref-set!! r1 []))
      

Move a member from one group to another, across two different ref types (zk and mongo), transactionally so that the member always exist in one group or the other, but never neither group nor both groups.

Define the two groups as refs (one zk-backed, the other mongodb-backed) that contain a map with one fields :members, which is a set of names.
    
    (def group1 (zk-ref client "/group1" {:members #{"david"}}))
    (def group2 (mongo-ref client "/group2" {:members #{"emma"}}))
    
    (dosync!! client
      (alter!! group1 update-in [:members] disj "david")
      (alter!! group2 update-in [:members] conj "david"))
      
    

# Clojure's Philosophy of State

Rich Hickey has spoken eloquently on <a href="http://clojure.org/state">mutable state</a> in his talk <a href="http://www.infoq.com/presentations/Are-We-There-Yet-Rich-Hickey">Are We There Yet</a>. To summarize, Rich and Alfred North Whitehead don't believe in mutable state, it's an illusion. Rather, there are only successions of causally-linked immutable values, and time is derived from the perception of these successions. Causaully-linked means the future is a function of the past; processes apply pure functions to immutable values to derive new immutable values, and we assign identity to these chains of values, and perceive change where there is none. 

As Rich has been known to do, he has provided precise, if not familiar, definitions to some familiar words that describe this model of identity, time, and mutable state.

* **Value**: an immutable magnitude, quantity, number, or composite of these
* **Identity**: a putative entity we associate with a series of causally related values (states) over time
* **State**: value of an identity at a moment in time
* **Time**: relative before/after ordering of causal values.

A consequence of this view is that we perceive only snapshots of an unchanging past, not a ever-changing present, and this is exactly how Clojure models time, identity, and mutable state using Atoms, Refs, and Agents.

Perception is massively parallel and uncoordinated, meaning you don't have to coordinate with others to observe the value of an identity, no locks or synchronized blocks or message queues, just grab the most recent snapshot of the identity's value and go.

Writes, on the other hand, must be coordinated. Since future values are derived by applying functions to an identity's most recent immutable value, writes must be atomic. Two processes that observe the same value for an identity cannot both write new values, one will win and the other will have to read the latest value and try their write again.


Despite having such a precise model of state and time, it is typically the case that you want model your programs with pure functions and immutable values, minimizing where ever possible mutable state. Of course, it's not always possible to eliminate mutable state, and that's where having such clear semantics and mechanisms for updating state becomes powerful.


Likewise, when writing distributed applications, a goal is to create components that are loosely coupled and which communicate with each other asynchronously, but this is also not always possible. There are times when you need coordinated access to state across systems in a distributed application, and this is where Avout Atoms and Refs come in.


# NOTES


Pure functions, immutable values, identity.

Functional programming eliminates time from consideration. This is not purely functional programming, because the progression of time must be contended with.
MVCC

atomicity, consistency, isolation, and optionally durability ACI(D)

values for points in time, all you're ever going to get is a value at a point in time. there is no such thing as a mutable value, just immutable values at different points in time. 

An identity is a concept applied to causally linked values over time.

The future is a function of the past. A process creates the future from the past.

Identities are a mental construct superimposed on a series of casually linked values.

time is a derivative of a these series of values.

continuity is a process creating successive values.

perception is massively parallel and uncoordinated.

* we are always perceiving the (unchanging!) past
* ignoring feedback, we like snapshots

Modeling time with functions and values

Changing state by applying functions to the value associated with an identity, and using the result as the new value of the identity.



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
