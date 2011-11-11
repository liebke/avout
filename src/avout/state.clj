(ns avout.state)


;; shared protocols

(defprotocol StateCache
  (setCache [this value])
  (getCache [this])
  (invalidateCache [this]))