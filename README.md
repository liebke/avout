## Table of Contents

* <a href="#quick-start">Quick Start</a>
* <a href="#about-avout">About Avout</a>
* <a href="#state-in-clojure">Clojure's Philosophy of State Made Distributed</a>
* <a href="#avout-atoms-refs">Avout Atoms and Refs</a>
* <a href="#extending-avout">Extending Avout Atoms and Refs</a>
* <a href="#locks">Avout Distributed Locks</a>
* <a href="#running-zookeeper">Runnning ZooKeeper</a>
* <a href="#contributing">Contributing</a>
* <a href="#references">References</a>

<a name="about-avout" />
## About Avout

**Avout** provides distributed-state, Clojure-style by extending Clojure's syntax and semantics for managing <a href="http://clojure.org/state">in-memory state</a> to heterogeneous types of state that span multiple processes/systems by providing distributed and extendable versions of Clojure's <a href="http://clojure.org/atoms">Atom</a> and <a href="http://clojure.org/refs">Ref</a>.

*Avout* uses <a href="http://zookeeper.apache.org">ZooKeeper</a> and <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a> to coordinate state change, and also includes distributed implementations of <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/Lock.html">*java.util.concurrent.lock.Lock*</a>, <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/ReadWriteLock.html">*java.util.concurrent.lock.ReadWriteLock*</a>.

Below is some background on Avout's design, if your interests don't tend toward the philosophy of <a href="http://en.wikipedia.org/wiki/Alfred_North_Whitehead">Alfred North Whitehead</a>, you can jump directly to the <a href="#quick-start">Quick Start Guide</a>.

<a name="state-in-clojure" />
### Clojure's Philosophy of State Made Distributed

Rich Hickey has spoken eloquently on <a href="http://clojure.org/state">mutable state</a> in his talk <a href="http://www.infoq.com/presentations/Are-We-There-Yet-Rich-Hickey">"Are We There Yet?"</a>. To summarize, Rich and Alfred North Whitehead don't believe in mutable state, it's an illusion. Rather, there are only successions of causally-linked immutable values, and time is derived from the perception of these successions. Causally-linked means the future is a function of the past; processes apply pure functions to immutable values to derive new immutable values, and we assign identity to these chains of values, and perceive change where there is none. 

As Rich has been <a href="http://www.infoq.com/presentations/Simple-Made-Easy">known to do</a>, he has provided precise, if not familiar, definitions to some familiar words that describe this model of identity, time, and mutable state.

* **Value**: an immutable magnitude, quantity, number, or composite of these
* **Identity**: a putative entity we associate with a series of causally related values (states) over time
* **State**: value of an identity at a moment in time
* **Time**: relative before/after ordering of causal values.

A consequence of this view is that we perceive only snapshots of an unchanging past, not a ever-changing present, and this is exactly how Clojure models time, identity, and mutable state using Atoms, Refs, and Agents.

Perception is massively parallel and uncoordinated, meaning you don't have to coordinate with others to observe the value of an identity, no locks or synchronized blocks or message queues, just grab the most recent snapshot of the identity's value and go.

Writes, on the other hand, must be coordinated. Since future values are derived by applying functions to an identity's most recent immutable value, writes must be atomic. Two processes that observe the same value for an identity cannot both write new values, one will win and the other will have to read the latest value and try its write again. Below is an illustration of how Clojure's, and Avout's, STM performs updates to a Ref's state with two contending writers.

<img src="https://github.com/liebke/avout/raw/master/docs/images/stm.png" />

1. *Thread-1* creates a Ref, **r**, at time **t0** with a value of 0
2. *Thread-2* then derefs **r** and oberves the latest value 0 from **t0**.
3. *Thread-1* then begins a transaction at **t1**, the value of **r** is still 0. 
4. *Thread-2* starts begins its own transaction at **t2**, the value of **r** is also still 0.
5. *Thread-1* begins committing at **t3**, updating **r**'s **t0** value with the *inc* function.
6. *Thread-2* attempts to commit but fails because *Thread-1* beat it to the commit, so it starts over.
7. While *Thread-2* is running the transaction again, *Thread-1* starts a new transaction, but only observes the value of **r** in it, so it doesn't interfere with *Thread-2*'s commit. Note that observing a Ref's value outside of a transaction doesn't move time forward, but that observing it inside of transaction, even if you don't change it does.


### Transactions

**TODO**

<img src="https://github.com/liebke/avout/raw/master/docs/images/ref-over-time.png" />


### Designing with State

Despite having such a precise model of state and time, it is desirable to design your programs with pure functions and immutable values, minimizing wherever possible mutable state. Of course, it's not always possible to completely eliminate state, and that's where having such clear semantics and mechanisms for updating it becomes powerful.


Likewise, when designing distributed applications, it is desirable to create components that are loosely coupled and that communicate with each other asynchronously, but this too is also not always possible. There are times when you need coordinated access to state across systems in a distributed application, and this is where Avout Atoms and Refs come in.

<a name="avout-atoms-refs" />
### Avout Atoms and Refs

*Avout* Atoms and Refs are **ACI(D)**, providing the same promises of *atomicity*, *consistency*, and *isolation* as Clojure's in-memory versions, and optionally providing *durability*, depending on the type of Avout Atom or Ref used.

*Avout* Atoms and Refs use <a href="http://zookeeper.apache.org">Apache ZooKeeper</a> to coordinate state change, but not necessarily to hold state. Out of the box, Avout contains an Atom, *zk-atom*, that does also use ZooKeeper data fields to store its state, and two types of Refs, *zk-ref*, also backed by ZooKeeper data fields, and *local-ref*, that is in fact not distributed but provides a mechanism for local Refs to participate in transactions with distributed Refs.

**Extendable**

*Avout* lets you create new types of distributed Atoms and Refs using the **avout.state.StateContainer** and **avout.state.VersionedStateContainer** protocols, respectively; examples of which are the **MongoDB**-backed *mongo-atom* and *mongo-ref*, which can be found in the plugins directory.

**Caching**

*Avout* provides **caching**, so that multiple derefs of the same Atom/Ref will not need to hit either ZooKeeper nor the back-end state-store until the cache has been invalidated by a local or remote update to the Atom or Ref.

**Implements IRef**

*Avout* Atoms and Refs implement Clojure's IRef interface, and therefore support functions that operate on IRefs, including: deref (and its reader-macro, @), set-validator!, add-watch, remove-watch.

*Avout* provides "double-bang" versions of the remaining core Atom and Ref functions (reset!, swap!, dosync, ref-set, alter, commute) for use with distributed Atoms and Refs: **reset!!, swap!!, dosync!!, ref-set!!, alter!!, commute!!**.

### zk-atom and zk-ref

Both zk-atoms and zk-refs store their state in a ZooKeeper data field as a byte array; deserialization/serialization is performed using Clojure's Reader/Printer (read-str/pr-str). ZooKeeper limits the size of data stored in data fields to 1 megabyte. Because Clojure's printer/reader is used for serialization, only appropriate Clojure data structures can be used as values. However, the ability to implement other back-ends with different serializations schemes is a primary goal of Avout.

### local-ref

The local-ref stores its state in an in-memory Clojure Atom. Because its state is not network accessible, it can only be access from a single JVM, just like in-memory Refs, but unlike in-memory Refs it can participate in transactions with distributed Refs. This provides two benefits, 1) it provides a mechanism for keeping a value that only needs to be available locally in sync with distributed state using dosync!!, 2) since no serialization/deserialization is performed, arbitrary values can be used, including Java objects.

### mongo-atom and mongo-ref

In the plugins directory there is a Leiningen project called mongo-avout, which contains an implementation of a MongoDB-backed Atom, mongo-atom, and Ref, mongo-ref. Both support values consisting of any Clojure data structure supported in MongoDB with the <a href="https://github.com/aboekhoff/congomongo">Congomongo Library</a>.



<a name="extending-avout" />
### Extending Avout Atoms and Refs

The above three types of Atoms and Refs provide examples for implementing other types that use different back-end state-stores and serialization methods. State-stores may, or may not, provide durability. For instance, zk-atom, zk-ref, mongo-atom, and mongo-ref provide durability promises in addition to atomicity, consistency, and isolation, but local-ref does not. Other durable (e.g. (No)SQL databases, RESTful services) or non-durable (e.g. <a href="http://www.terracotta.org/">Terracotta</a>, RESTful services) can be used as the basis of new types of Atoms and Refs.

New types of Atoms are created by implementing the **avout.state.StateContainer** protocol.

    (defprotocol StateContainer
      (initStateContainer [this])
      (destroyStateContainer [this])
      (getState [this])
      (setState [this value]))

Once you have implemented a *StateContainer*, create a distributed Atom by passing it to the **avout.atoms.distributed-atom** function.

    (defn custom-atom [client name] 
      (distributed-atom client atom-name custom-state-container))

To create new Ref types, implement **avout.state.VersionedStateContainer**.

    (defprotocol VersionedStateContainer
      (initVersionedStateContainer [this])
      (destroyVersionedStateContainer [this])
      (getStateAt [this version])
      (setStateAt [this value version]))

Once you have implemented a *VersionedStateContainer*, create a distributed Ref by passing it to the **avout.refs.distributed-ref** function.

    (defn custom-ref [client name] 
      (distributed-ref client ref-name custom-versioned-state-container))


<a name="quick-start" />
## Quick Start

*Avout* Atoms and Refs implement Clojure's IRef interface, and therefore support functions that operate on IRefs, including: deref (and its reader-macro, @), set-validator!, add-watch, remove-watch.

*Avout* provides "double-bang" versions of the remaining core Atom and Ref functions (reset!, swap!, dosync, ref-set, alter, commute) for use with distributed Atoms and Refs: **reset!!, swap!!, dosync!!, ref-set!!, alter!!, commute!!**.

To get started, you'll need to <a href="#running-zookeeper">run ZooKeeper</a>, and include Avout as a dependency by adding the following to your Leiningen project.clj file:

    [avout "0.5.0-SNAPSHOT"]

Then load the avout.core namespace, and create a ZooKeeper client that will be passed to distributed Refs and Atoms when they are created and to dosync!! transactions when they are performed.

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

Define the two groups as refs (one zk-backed, the other mongodb-backed) that contain a map with one fields :members, which is a set of names (Note: mongo-ref and mongo-atom are part of a seperate project called mongo-avout, which lives in the plugins directory, you will need to install it in order to run the following example).
    
    (import 'avout.refs.mongo)
    
    (def group1 (zk-ref client "/group1" {:members #{"david"}}))
    (def group2 (mongo-ref client "/group2" {:members #{"emma"}}))
    
    (dosync!! client
      (alter!! group1 update-in [:members] disj "david")
      (alter!! group2 update-in [:members] conj "david"))
      
    

<a name="locks" />
## avout.locks

To get started, you'll need to <a href="#running-zookeeper">run ZooKeeper</a>.

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

    (try (.tryLock lock 10 java.util.concurrent.TimeUnit/MILLISECONDS)
      ... do something
      (finally (.unlock lock)))

The macros when-lock-with-timeout and if-lock-with-timeout are also available.

    (when-lock-with-timeout lock 10 java.util.concurrent.TimeUnit/MILLISECONDS
       (... do something)
       
    (if-lock-with-timeout lock 10 java.util.concurrent.TimeUnit/MILLISECONDS
      (... got lock, do something)
      (... didn't get lock do something else))


<a name="running-zookeeper"></a>
## Running ZooKeeper

Download Apache ZooKeeper from <a href="http://zookeeper.apache.org/releases.html">http://zookeeper.apache.org/releases.html</a>.

Unpack to $ZOOKEEPER_HOME (wherever you would like that to be).

Here's an example conf file for a standalone instance, by default ZooKeeper will look for it in $ZOOKEEPER_HOME/conf/zoo.cfg

    # The number of milliseconds of each tick
    tickTime=2000
    
    # the directory where the snapshot is stored.
    dataDir=/var/zookeeper
    
    # the port at which the clients will connect
    clientPort=2181
    
Ensure that the dataDir exists and is writable.
    
After creating and customizing the conf file, start ZooKeeper

    $ZOOKEEPER_HOME/bin/zkServer.sh start


<a name="contributing" />
## Contributing

Although Avout is not part of Clojure-Contrib, it follows the same guidelines for contributing, which includes signing a <a href="http://clojure.org/contributing">Clojure Contributor Agreement</a> (CA) before contributions can be accepted.

<a name="references" />
## References

* <a href="http://www.infoq.com/presentations/Are-We-There-Yet-Rich-Hickey">"Are We There Yet?"</a>
* <a href="http://clojure.org/state">Clojure State</a>
* <a href="http://clojure.org/refs">Clojure Refs and Transactions</a>
* <a href="http://en.wikipedia.org/wiki/Software_transactional_memory">Software Transactional Memory</a>
* <a href="http://en.wikipedia.org/wiki/Multiversion_concurrency_control">Multiversion Concurrency Control</a>
* <a href="http://en.wikipedia.org/wiki/Snapshot_isolation">Snapshot Isolation</a>
* <a href="http://zookeeper.apache.org/">ZooKeeper Website</a>
* <a href="http://www.usenix.org/event/atc10/tech/full_papers/Hunt.pdf">ZooKeeper: Wait-free coordination for Internet-scale systems</a>
* <a href="http://zookeeper.apache.org/doc/trunk/recipes.html">ZooKeeper Recipes and Solutions</a>


## License

Avout is Copyright © 2011 David Liebke and Relevance, Inc

Distributed under the Eclipse Public License, the same as Clojure.
