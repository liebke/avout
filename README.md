<a href="http://avout.io"><img width="550" src="http://avout.io/images/avout-logo.png" /></a>


<a name="about-avout" />
## About Avout

<p>Much has been written on functional programming and the advantages of designing programs that emphasize pure functions and immutable values, and that minimize or eliminate, wherever possible, mutable state. Of course, it's not always possible to completely eliminate the need for mutable state, and that's where Clojure's precise <a href="http://clojure.org/state">model of time, identity, and state</a> becomes powerful.</p>

<p>Likewise, when designing distributed applications, it is desirable to create components that are loosely coupled and that communicate with each other asynchronously, but this too is also not always possible. There are times when you need coordinated access to state across systems in a distributed application, and this is where <a href="https://github.com/liebke/avout">Avout</a> comes in.</p>

<p><em>Avout</em> provides distributed-state, Clojure-style by extending Clojure's syntax and semantics for managing in-memory state to heterogeneous types of state that span multiple processes and/or systems by providing distributed and extendable versions of Clojure's <a href="http://clojure.org/atoms">Atom</a> and <a href="http://clojure.org/refs">Ref</a>.</p>

<p><em>Avout</em> uses <a href="http://zookeeper.apache.org">ZooKeeper</a> and <a href="https://github.com/liebke/zookeeper-clj">zookeeper-clj</a> to coordinate state change, and also includes distributed implementations of <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/Lock.html"><em>java.util.concurrent.lock.Lock</em></a> and <a href="http://download.oracle.com/javase/1,5,0/docs/api/java/util/concurrent/locks/ReadWriteLock.html"><em>java.util.concurrent.lock.ReadWriteLock</em></a>.</p>

<p>For more information, vist <a href="http://avout.io">avout.io</a>.</p>

<a name="contributing" />
## Contributing

Although Avout is not part of Clojure-Contrib, it follows the same guidelines for contributing, which includes signing a <a href="http://clojure.org/contributing">Clojure Contributor Agreement</a> (CA) before contributions can be accepted.


## License

Avout is distributed under the Eclipse Public License, the same as Clojure.

## Copyright

Avout is Copyright © 2011 David Liebke and Relevance, Inc


