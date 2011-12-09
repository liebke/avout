(ns avout.transaction
  (:use avout.core))

(defn init-clock [initializer config]
  (datom "/stm/clock" 0 initializer config))

(defn next-point [clock]
  (swap!! clock inc))

(defn init-txn-info [clock initializer config]
  (let [txid (next-point clock)]
    (datom (str "/stm/txn/" txid) {:txid txid, :state :RUNNING} initializer config)))

(defn get-txn-info [txid initializer config]
  (datom (str "/stm/txn/" txid) initializer config))

(defn init-ref-info
  ([name initilizer config]
     (datom (str "/stm/refs/" name) {:history [], :txid nil} initilizer config)))

(defn get-ref-info [name initializer config]
  (datom (str "/stm/refs/" name) initializer config))

(defn current-state? [tinfo & states]
  (reduce #(or %1 (= (:state tinfo) %2)) false states))

(defn tagged? [rinfo initializer config]
  (when-let [txid (:txid rinfo)]
    (let [txn (get-txn-info txid initializer config)]
      (current-state? @txn :RUNNING :COMMITTING))))

(defn try-tag [ref-info txid initializer config]
  (let [new-info (swap!! ref-info
                         (fn [current-info]
                           (if (tagged? current-info initializer config)
                             current-info
                             (update-in current-info [:txid] identity txid))))]
    (= (:txid new-info) txid)))

(defn update-state [txn-info old-state new-state]
  (let [tinfo (swap!! txn-info update-in [:state] #(if (= old-state %) new-state old-state))]
    (= (:state tinfo) new-state)))

(defn set-commit-point [clock txn-info]
  (swap!! txn-info update-in [:commit-point] (fn [old-value] (next-point clock))))

