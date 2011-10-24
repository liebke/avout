(ns treeherd.zookeeper.util
  (:require [treeherd.zookeeper :as zk]))

(defn extract-id
  "Returns an integer id associated with a sequential node"
  ([child-path]
     (let [zk-seq-length 10]
       (Integer. (subs child-path
                       (- (count child-path) zk-seq-length)
                       (count child-path))))))

(defn index-sequential-nodes
  "Sorts a list of sequential child nodes."
  ([unsorted-nodes]
     (when (seq unsorted-nodes)
       (map (fn [node] [(extract-id node) node]) unsorted-nodes))))

(defn sort-sequential-nodes
  "Sorts a list of sequential child nodes."
  ([unsorted-nodes]
     (map second (sort-by first (index-sequential-nodes unsorted-nodes)))))

(defn filter-children-by-pattern
  ([client dir pattern]
     (when-let [children (zk/children client dir)]
       (filter #(re-find pattern %) children))))

(defn filter-children-by-prefix
  ([client dir prefix]
     (filter-children-by-pattern client dir (re-pattern (str "^" prefix)))))