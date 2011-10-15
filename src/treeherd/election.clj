(ns treeherd.election
  (:require [treeherd.client :as tc]))

(defn next-lowest
  ([local-candidate sorted-candidates]
     (if (= local-candidate (first sorted-candidates))
       local-candidate
       (loop [[current & remaining] sorted-candidates
              previous nil]
         (when current
           (if (= local-candidate current)
             previous
             (recur remaining current)))))))

(defn sort-candidates
  ([unsorted-candidates]
     (when (seq unsorted-candidates)
       (let [path "/n-"
             path-size (count path)
             extract-id (fn [child-path] [child-path (Integer. (subs child-path path-size))])
             candidates (map first (sort-by second (map extract-id unsorted-candidates)))]
         candidates))))

(defn create-candidate
  ([client election-node]
     (let [path (str election-node "/n-")
           candidate (tc/create-all client path :sequential? true)]
       (subs candidate (inc (count election-node))))))

(defn set-leader
  ([client leadership-node leader]
     (tc/delete-children client leadership-node)
     (when leader (tc/create-all client (str leadership-node "/" leader)))))

(defn monitor-leader
  "Returns a reference to the leader node, which will be udpated as the leader changes.

  Examples:

    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))

    (delete-all treeherd \"/leader\")
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
  ([client leadership-node]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))
           leader-ref (ref nil)]
       (future
         (locking mutex
           (loop [leader (tc/children client leadership-node :watcher watcher)]
             (if (false? leader) ;; set leader-ref to nil and exit is there is no leadership-node
               (dosync (alter leader-ref (fn [_] nil)))
               (if (seq leader)
                 (do ;; if there is a child node, make it the leader
                   (dosync (alter leader-ref (fn [_] (first leader))))
                   (.wait mutex)
                   (recur (tc/children client leadership-node :watcher watcher)))
                 (do ;; else sleep, and check again
                   (dosync (alter leader-ref (fn [_] nil)))
                   (Thread/sleep 500)
                   (recur (tc/children client leadership-node :watcher watcher))))))))
       leader-ref)))

(defn monitor-election
  "Registers the client in an election, returning their ID.

  Examples:

    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))
    (delete-all treeherd \"/election\")
    (delete-all treeherd \"/leader\")

    (repeatedly 2 #(monitor-election treeherd \"/leader\" \"/election\" (create-candidate treeherd \"/election\")))

    (def local-candidate (create-candidate treeherd \"/election\"))
    (def leader (monitor-election treeherd \"/leader\" \"/election\" local-candidate))

    (repeatedly 2 #(monitor-election treeherd \"/leader\" \"/election\" (create-candidate treeherd \"/election\")))

    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))

"
  ([client leadership-node election-node local-candidate]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))]
       (future
         (locking mutex
           (loop [candidates (sort-candidates (tc/children client election-node))]
             (if (seq candidates)
               (do
                 (when-let [node-to-watch (next-lowest local-candidate candidates)]
                   (tc/exists client (str election-node "/" node-to-watch) :watcher watcher))
                 (set-leader client leadership-node (first candidates))
                 (.wait mutex)
                 (recur (sort-candidates (tc/children client election-node))))
               (set-leader client leadership-node nil))))))))


(defn enter-election
  "Registers the client in an election, returning a map with the local-candidate node,
   a ref pointing to the leader.

  Examples:

    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))
    (delete-all treeherd \"/election\")
    (delete-all treeherd \"/leader\")

    (repeatedly 2 #(enter-election treeherd))

    (def election-results (enter-election treeherd))

    (repeatedly 2 #(enter-election treeherd))

    (def leader (:leader election-results))
    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))
    @leader
    (delete treeherd (str \"/election/\" @leader))

"
  ([client & {:keys [leader-node election-node] :or {leader-node "/leader", election-node "/election"}}]
     (let [local-candidate (create-candidate client election-node)]
       (monitor-election client leader-node election-node local-candidate)
       {:local-candidate local-candidate, :leader (monitor-leader client leader-node)})))

(defn leave-election
  ([client id]
     (let [mutex (Object.)
           watcher (fn [event]
                     (do (println "leave-election: watched event: " event)
                         (locking mutex (.notify mutex))))]
       (tc/delete client id :watcher watcher))))