(ns treeherd.election
  (:require [treeherd.client :as tc]))

(defn- next-lowest
  ([local-candidate sorted-candidates]
     (if (= local-candidate (first sorted-candidates)) ;; then the local-candidate is the leader
       local-candidate
       (loop [[current & remaining] sorted-candidates
              previous nil]
         (when current ;; if there is no current candidate, then the local-candidate is not in this list
           (if (= local-candidate current)
             previous
             (recur remaining current)))))))

(defn- sort-candidates
  ([unsorted-candidates]
     (when (seq unsorted-candidates)
       (let [path "/n-"
             path-size (count path)
             extract-id (fn [child-path] [child-path (Integer. (subs child-path path-size))])
             candidates (map first (sort-by second (map extract-id unsorted-candidates)))]
         candidates))))

(defn- set-leader
  ([client leader-node leader]
     (tc/delete-children client leader-node)
     (when leader (tc/create-all client (str leader-node "/" leader)))))

(defn create-candidate
  ([client election-node]
     (let [path (str election-node "/n-")
           candidate (tc/create-all client path :sequential? true)]
       (subs candidate (inc (count election-node))))))

(defn close-election
  ([client & {:keys [election-node leader-node]
              :or {election-node "/election"
                   leader-node "/leader"}}]
     (do
       (tc/delete-all client election-node)
       (tc/delete-all client leader-node))))

(defn monitor-election
  "Registers the client in an election, returning their ID.

  Examples:

    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))
    (close-election treeherd)

    (repeatedly 2 #(monitor-election treeherd (create-candidate treeherd \"/election\") \"/election\" \"/leader\"))

    (def local-candidate (create-candidate treeherd \"/election\"))
    (def leader (monitor-election treeherd local-candidate \"/election\" \"/leader\"))

    (repeatedly 2 #(monitor-election treeherd (create-candidate treeherd \"/election\") \"/election\" \"/leader\"))

    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))

"
  ([client local-candidate election-node leader-node & {:keys [election-watcher]}]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))]
       (future
         (locking mutex
           (loop [candidates (sort-candidates (tc/children client election-node))]
             (if (seq candidates) ;; if there are no candidates, close the election
               ;; when the node-to-watch is nil, then the local-candidate is no longer in the election, so exit
               (when-let [node-to-watch (next-lowest local-candidate candidates)]
                 (tc/exists client (str election-node "/" node-to-watch) :watcher watcher)
                 (when (= local-candidate node-to-watch) ;; then local-candidate is the new leader
                   (set-leader client leader-node local-candidate))
                 (when election-watcher (election-watcher node-to-watch))
                 (.wait mutex) ;; wait until zookeeper invokes the watcher, which calls notify
                 (recur (sort-candidates (tc/children client election-node))))
               (close-election client :election-node election-node :leader-node leader-node))))))))

(defn monitor-leader
  "

  Returns a reference to the leader node, which will be udpated as the leader changes.

  Examples:

    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))
    (close-election treeherd)

    ;; create a leader node
    (new-leader treeherd \"/leader\" \"n-0000001\")

    (def leader (monitor-leader treeherd \"/leader\"))
    @leader

    ;; now change the leader
    (update-leader treeherd \"/leader\" @leader \"n-0000002\")
    @leader

    (update-leader treeherd \"/leader\" @leader \"n-0000003\")
    @leader

    ;; delete leader node and check again
    (delete-all treeherd \"/leader\")
    @leader

"
  ([client leader-node & {:keys [leader-watcher retry-timeout]
                          :or {retry-timeout 500}}]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))
           leader-ref (ref nil)]
       (future
         (locking mutex
           (loop [leader (tc/children client leader-node :watcher watcher)]
             (cond
               ;; set leader-ref to nil and exit if there is no leader-node
               (false? leader)
                 (dosync (alter leader-ref (fn [_] nil)))
               ;; if there is a child node, update the leader-ref and invoke the leader-watcher
               (seq leader)
                 (do
                   (dosync (alter leader-ref (fn [_] (first leader))))
                   (when leader-watcher (leader-watcher (first leader)))
                   (.wait mutex) ;; wait until zookeeper invokes the watcher, which calls notify
                   (recur (tc/children client leader-node :watcher watcher)))
               :else ;; else sleep and then retry
                 (do
                   (dosync (alter leader-ref (fn [_] nil)))
                   (Thread/sleep retry-timeout)
                   (recur (tc/children client leader-node :watcher watcher)))))))
       leader-ref)))

(defn enter-election
  "

  Registers the client in an election, returning a map with the local-candidate node,
  a ref pointing to the leader.

  Examples:

    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))
    (close-election treeherd)

    (defn make-election-watcher [i] (fn [node-to-watch] (println \"node\" i \"is watching\" node-to-watch)))

    (dotimes [i 2] (enter-election treeherd :election-watcher (make-election-watcher i)))

    (def election-results (enter-election treeherd :election-watcher (make-election-watcher 2)))

    (dotimes [i 2] (enter-election treeherd :election-watcher (make-election-watcher (+ i 3))))

    (def leader (:leader election-results))
    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))

"
  ([client & {:keys [leader-node election-node leader-watcher election-watcher monitor-leader?]
              :or {leader-node "/leader"
                   election-node "/election"
                   monitor-leader? true}}]
     (let [local-candidate (create-candidate client election-node)]
       (monitor-election client local-candidate election-node leader-node :election-watcher election-watcher)
       {:local-candidate local-candidate
        :leader (when monitor-leader? (monitor-leader client leader-node :leader-watcher leader-watcher))})))

(defn leave-election
  ([client candidate & {:keys [election-node] :or {election-node "/election"}}]
     (tc/delete client (str election-node "/" candidate))))

