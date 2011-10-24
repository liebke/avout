(ns treeherd.util)

(defmacro try*
  "Unwraps the RuntimeExceptions thrown by Clojure, and rethrows its cause. Only accepts a single expression."
  ([expression & catches]
     `(try
        (try
          ~expression
          (catch Throwable e# (throw (.getCause e#))))
        ~@catches)))


