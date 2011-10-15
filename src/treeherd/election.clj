(ns treeherd.election
  (:require [treeherd.client :as tc]))

(defn next-lowest
  ([id sorted-candidates]
     (loop [[next & remaining] sorted-candidates
            previous nil]
       (if (= id next)
         (first previous)
         (recur remaining next)))))

(defn sort-candidates
  ([election-node id unsorted-candidates]
     (let [path "/n-"
           path-size (count path)
           extract-id (fn [child-path] [(str election-node "/" child-path) (Integer. (subs child-path path-size))])
           candidates (sort-by second (map extract-id unsorted-candidates))
           ;;node-to-watch (next-lowest id candidates)
           ]
;;       (println "get-candidates id node-to-watch " id node-to-watch)
;;       (if node-to-watch (tc/exists client node-to-watch :watcher watcher))
       candidates)))

(defn start-election
  "Registers the client in an election, returning their ID.

  Examples:

    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))
    (delete-all treeherd \"/election\")

     ;; create a few volunteers
    (repeatedly 2 #(enter-election treeherd \"/election\"))

    ;; enter electio
    (def election-results (enter-election treeherd \"/election\"))

    ;; create a few more
    (repeatedly 5 #(enter-election treeherd \"/election\"))

    (deref (:id election-results))
    (deref (:leader election-results))

    ;; now remove the current leader
    (leave-election treeherd (deref (:leader election-results)))
    ;; and look at the new leader
    (deref (:leader election-results))



    ;; ALT
    (use '(treeherd client election))
    (def treeherd (client \"127.0.0.1:2181\"))
    (delete-all treeherd \"/election\")


    (def candidate (create-candidate treeherd \"/election\"))

    (repeatedly 5 #(create-candidate treeherd \"/election\"))

    ;; (repeatedly 5 #(create treeherd \"/election/n-\" :sequential? true))

    (def leader (start-election treeherd \"/election\" candidate))
    @leader
    (delete treeherd @leader)
    @leader
    (delete treeherd @leader)
    @leader
    (delete treeherd @leader)
"
  ([client election-node local-candidate]
     (let [mutex (Object.)
           watcher (fn [event] (locking mutex (.notify mutex)))
           leader (fn [candidates] (-> candidates first first))
           leader-ref (ref nil)]
       (future
         (locking mutex
           (loop [candidates (tc/children client election-node :watcher watcher)]
             (if (seq candidates)
               (do
                 (dosync (alter leader-ref (fn [_] (leader (sort-candidates election-node local-candidate candidates)))))
                 (.wait mutex)
                 (recur (tc/children client election-node :watcher watcher)))
               (dosync (alter leader-ref (fn [_] nil)))))))
       leader-ref)))

(defn create-candidate
  ([client election-node]
     (when-not (tc/exists client election-node)
       (tc/create client election-node :persistent? true))
     (let [path (str election-node "/n-")
           name (tc/create client path :sequential? true)
           id (Integer. (subs name (count path)))]
       [name id])))

(defn leave-election
  ([client id]
     (let [mutex (Object.)
           watcher (fn [event]
                     (do (println "leave-election: watched event: " event)
                         (locking mutex (.notify mutex))))]
       (tc/delete client id :watcher watcher))))