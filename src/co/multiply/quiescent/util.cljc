(ns ^:no-doc co.multiply.quiescent.util
  #?(:cljs (:require
             [clojure.test :refer [async]]))
  #?(:cljs (:require-macros co.multiply.quiescent.util)))


(defmacro if-cljs
  "Helper for switching between ClojureScript and Clojure implementations in macros."
  [then else]
  (if (contains? &env '&env)
    ;; Inside another macro - emit a runtime check
    `(if (:ns ~'&env) ~then ~else)
    ;; Direct use - check now
    (if (:ns &env) then else)))
