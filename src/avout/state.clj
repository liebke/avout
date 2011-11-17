(ns avout.state)


;; shared protocols

(defprotocol Identity
  (getName [this])
  (init [this])
  (destroy [this]))


(defprotocol StateContainer
  (initStateContainer [this])
  (destroyStateContainer [this])
  (getState [this])
  (setState [this value]))

(defprotocol VersionedStateContainer
  (initVersionedStateContainer [this])
  (destroyVersionedStateContainer [this])
  (getStateAt [this version])
  (setStateAt [this value version]))

(defprotocol StateCache
  (setCache [this value])
  (setCacheAt [this value version])
  (getCache [this])
  (cachedVersion [this])
  (invalidateCache [this]))

