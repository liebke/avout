(ns avout.transaction
  (:use avout.core))

(defn init-stm [initializer config]
  (when-let [txids (datom "/stm/txns" initializer config)]
    (doseq [t txids] (.destroy (datom (str "/stm/txns/" t) initializer config))))
  (when-let [refs (datom "/stm/refs" initializer config)]
    (doseq [r refs] (.destroy (datom (str "/stm/refs/" r) initializer config))))
  {:clock  (datom "/stm/clock" 0 initializer config)
   :txns (datom "/stm/txns" [] initializer config)
   :refs (datom "/stm/refs" [] initializer config)
   :initializer initializer
   :config config})

(defn next-point [stm]
  (swap!! (:clock stm) inc))

(defn init-txn-info [stm]
  (let [txid (next-point stm)]
    (datom (str "/stm/txns/" txid) {:txid txid, :state :RUNNING} (:initializer stm) (:config stm))))

(defn get-txn-info [stm txid]
  (datom (str "/stm/txns/" txid) (:initializer stm) (:config stm)))

(defn init-ref-info
  ([stm name]
     (datom (str "/stm/refs/" name) {:history [], :txid nil} (:initializer stm) (:config stm))))

(defn get-ref-info [stm name]
  (datom (str "/stm/refs/" name) (:initializer stm) (:config stm)))

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

(defn set-commit-point [stm txn-info]
  (swap!! txn-info update-in [:commit-point] (fn [old-value] (next-point stm))))

