(ns avout.state)

(defprotocol Identity
  (getName [this])
  (init [this])
  (destroy [this]))

(defprotocol StateContainer
  (initStateContainer [this])
  (destroyStateContainer [this])
  (getState [this])
  (setState [this value])
  (compareAndSwap [this oldValue newValue]))

(defprotocol VersionedStateContainer
  (initVersionedStateContainer [this])
  (destroyVersionedStateContainer [this])
  (getStateAt [this version])
  (setStateAt [this value version])
  (deleteStateAt [this version]))

(defprotocol StateCache
  (setCache [this value])
  (setCacheAt [this value version])
  (getCache [this])
  (cachedVersion [this])
  (invalidateCache [this]))

