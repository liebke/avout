<a href="http://avout.io"><img width="550" src="http://avout.io/images/avout-logo.png" /></a>


<a name="about-avout" />
## About Avout

<p><a href="http://avout.io">Avout</a> brings Clojure's in-memory model of <a href="http://clojure.org/state">state</a> to distributed application development by providing a distributed implementation of Clojure's <a href="http://en.wikipedia.org/wiki/Multiversion_concurrency_control">Multiversion Concurrency Control</a> (MVCC) <a href="http://en.wikipedia.org/wiki/Software_transactional_memory">STM</a> along with distributable, durable, and extendable versions of Clojure's <a href="http://clojure.org/atoms">Atom</a> and <a href="http://clojure.org/refs">Ref</a> concurrency primitives.</p>

<p>Avout enables techniques that require synchronous, coordinated (i.e. transactional) management of distributed state (see also <a href="http://java.sun.com/developer/technicalArticles/tools/JavaSpaces/">JavaSpaces</a>), complementing approaches that focus on asynchronous, uncoordinated communication between distributed components, e.g. message queues (<a href="http://www.zeromq.org/">0MQ</a>, <a href="http://www.rabbitmq.com/">RabbitMQ</a>, <a href="http://www.jboss.org/hornetq">HornetQ</a>), event-driven approaches (<a href="http://www.jboss.org/netty">Netty</a>, <a href="https://github.com/ztellman/aleph">Aleph</a>), and actors (<a href="http://www.erlang.org/">Erlang</a>, <a href="http://akka.io">Akka</a>).</p>

<p>Much has been written [<a href="#references">1</a>, <a href="#references">2</a>, <a href="#references">3</a>] on functional programming and the advantages of designing programs that emphasize pure functions and immutable values, and that minimize or eliminate, wherever possible, mutable state. Of course, it's not always possible to completely eliminate the need for mutable state, and that's where Clojure's precise <a href="http://clojure.org/state">model of time, identity, and state</a> becomes powerful.</p>

<p>Likewise, when designing distributed applications, it is desirable to create components that are loosely coupled and that communicate with each other asynchronously, but this too is also not always possible. There are times when you need coordinated access to state across systems in a distributed application, and this is where Avout comes in.</p>

<p><em>Avout</em> uses <a href="http://zookeeper.apache.org">ZooKeeper</a> and <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a> to coordinate state change, and also includes distributed implementations of <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/Lock.html"><em>java.util.concurrent.lock.Lock</em></a> and <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/ReadWriteLock.html"><em>java.util.concurrent.lock.ReadWriteLock</em></a>.</p>

<h3>Using Avout</h3>

<p>To get started, you'll need to <a href="http://avout.io/index.html#running-zookeeper">run ZooKeeper</a>, and include Avout as a dependency by adding the following to your project.clj file:</p>

```clojure
[avout "0.5.3"]
```

<p>Below is the Avout equivalent of *Hello World*.</p>

```clojure
(use 'avout.core)
(def client (connect "127.0.0.1"))

(def r0 (zk-ref client "/r0" 0))
(def r1 (zk-ref client "/r1" []))

(dosync!! client
  (alter!! r0 inc)
  (alter!! r1 conj @r0))
```

<p>Start by creating a ZooKeeper client with the <strong>connect</strong> function, then create two ZooKeeper-backed distributed Refs using the <strong>zk-ref</strong> function. Finally, perform a <strong>dosync!!</strong> transaction that updates both Refs with <strong>alter!!</strong>. Using Avout isn't much different than using Clojure's in-memory Atoms and Refs.</p>

<p><em>Avout</em> Atoms and Refs implement Clojure's IRef interface, and therefore support functions that operate on IRefs, including: deref (and its reader-macro, @), set-validator!, add-watch, and remove-watch.</p>

<p><em>Avout</em> also provides <strong>"double-bang"</strong> versions of the remaining core Atom and Ref functions (<a href="http://clojuredocs.org/clojure_core/clojure.core/reset!">reset!</a>, <a href="http://clojuredocs.org/clojure_core/clojure.core/swap!">swap!</a>, <a href="http://clojuredocs.org/clojure_core/clojure.core/dosync">dosync</a>, <a href="http://clojuredocs.org/clojure_core/clojure.core/ref-set">ref-set</a>, <a href="http://clojuredocs.org/clojure_core/clojure.core/alter">alter</a>, <a href="http://clojuredocs.org/clojure_core/clojure.core/commute">commute</a>) for use with distributed Atoms and Refs, <strong>reset!!, swap!!, dosync!!, ref-set!!, alter!!, commute!!</strong>.</p>

<p><em>Note: Avout Refs cannot participate in in-memory dosync transactions, but Avout's **local-ref** provides the equivalent of an in-memory Ref that can participate in dosync!! transactions with distributed Refs.</em></p>

<h3>Extending Avout</h3>

<p>Two types of Atoms, zk-atom and mongo-atom, and three types of Refs, zk-ref, mongo-ref, and local-ref have been implemented, and Avout can be extended with additional types of Atoms and Refs that use different containers for their state, durable or not, including (No)SQL databases, (distributed) filesystems, in-memory data structures, and RESTful webservices. The types of values supported by each depends on both the backend store and the method of serialization used, and transactions containing different types of Avout Refs are supported.</p>

<p>New types of Atoms can be created by implementing the <strong>avout.state.StateContainer</strong> protocol,</p>

```clojure
(defprotocol StateContainer
  (initStateContainer [this])
  (destroyStateContainer [this])
  (getState [this])
  (setState [this value]))
```

<p>and new Ref types can be created by implementing <strong>avout.state.VersionedStateContainer</strong>.</p>

```clojure
(defprotocol VersionedStateContainer
  (initVersionedStateContainer [this])
  (destroyVersionedStateContainer [this])
  (getStateAt [this version])
  (setStateAt [this value version])
  (deleteStateAt [this version]))
```

<h3>More on Avout</h3>
<p>To learn more about Avout and distributed-state in Clojure, visit <a href="http://avout.io">avout.io</a></p>
<ul>
<li><a href="http://avout.io/#background">Background</a></li>
<li><a href="http://avout.io/#tutorial">Getting Started</a></li>
<li><a href="http://github.com/liebke/avout">Source Code</a></li>
</ul>

<a name="contributing" />
<h3>Contributing</h3>

Although Avout is not part of Clojure-Contrib, it follows the same guidelines for contributing, which includes signing a <a href="http://clojure.org/contributing">Clojure Contributor Agreement</a> (CA) before contributions can be accepted.


<h3>License</h3>

Avout is distributed under the Eclipse Public License, the same as Clojure.

<h3>Copyright</h3>

Avout is Copyright © 2011 David Liebke and Relevance, Inc


