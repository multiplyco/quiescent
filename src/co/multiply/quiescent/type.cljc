(ns ^:no-doc co.multiply.quiescent.type
  #?(:cljs (:require-macros co.multiply.quiescent.type))
  (:require
    [co.multiply.quiescent.util :refer [if-cljs]])
  #?(:clj (:import
            [co.multiply.quiescent.impl TaskState ICancellable IStateful ITask]
            [clojure.lang Indexed]
            [java.util HashMap Iterator Set]
            [java.util.concurrent ConcurrentHashMap ConcurrentHashMap$KeySetView Future TimeoutException])))


;; ## Helpers
(defmacro vecCount
  [v]
  (if-cljs
    `(cljs.core/-count ~v)
    `(Indexed/.count ~v)))


(defmacro vecNth
  [v idx]
  (if-cljs
    `(cljs.core/-nth ~v ~idx)
    `(Indexed/.nth ~v ~idx)))


;; ## Task state
#?(:cljs (deftype TaskState [^boolean exceptional ^boolean cancelled result]))


(defmacro throw-boxed-error
  "Extract value from TaskState, throwing if exceptional.

   Used by deref: if the task completed exceptionally, throws the boxed exception.
   Otherwise returns the result value."
  [state]
  `(let [^TaskState state# ~state]
     (if (.-exceptional state#)
       (throw (.-result state#))
       (.-result state#))))


#?(:cljs (defprotocol ICancellable
           (doCancel [this msg])
           (doCancelDirect [this])
           (doCancelCascade [this])))


#?(:cljs (defprotocol IStateful
           (isExceptional [this])
           (isCancelled [this])
           (isCompelled [this])
           (getPhase [this])
           (getResult [this])
           (getSubscriptions [this])
           (getScope [this])
           (setScope [this new-scope])
           (atOrPast [this phase])))


;; ## ITask
#?(:cljs (defprotocol ITask
           (getNow [this default])
           (newQuiescenceProxy [this default])
           ;; Entry points
           (doRun [this f tf])
           ;; Phases
           (doGround [this v tf])
           (doTransform [this res tf])
           (doWrite [this task-state])
           (doSettle [this])
           (doQuiesce [this])
           ;; Subscription
           (doSubscribe [this sub])
           (doUnsubscribe [this sub])
           (runSubscriptions [this])
           ;; Chain transformations
           (doThen [this delegator f])
           (doHandle [this delegator f])
           (doCatch [this delegator f])
           (doCatchTyped [this delegator type-handler-pairs])
           ;; Chain side-effects
           (doOk [this delegator f])
           (doErr [this delegator f])
           (doDone [this delegator f])
           ;; Chain teardown/cleanup
           (doFinally [this delegator f])))


;; ## Task conversion/detection
(defprotocol ITaskable
  (taskable? [this] "Is this an async value that as-task handles specially?")
  (groundable? [this] "Should ground automatically resolve this?")
  (as-task [this] "Convert to a Task"))


;; ## Ground
(deftype Plain [v])


(defn plain
  "Wrap a value to indicate it contains no nested tasks and should skip grounding.

   This is an optimization for combinators that produce values from already-resolved
   task results (e.g., `qmerge`, `qjoin`). The `ground` phase checks for `Plain` and
   unwraps directly instead of walking the data structure.

   Internal use only - not exposed in the public API."
  [v]
  (Plain. v))


(defn plain?
  "Returns true if v is wrapped in Plain, indicating it needs no grounding."
  [v]
  (instance? Plain v))


(defn plain-val
  [^Plain p]
  (.-v p))


;; ## Errors
(defmacro unsupported-operation-exception
  [msg]
  (if-cljs
    `(js/Error. ~msg)
    `(new UnsupportedOperationException ^String ~msg)))


(defmacro illegal-argument-exception
  [msg]
  (if-cljs
    `(js/Error. ~msg)
    `(new IllegalArgumentException ^String ~msg)))


(defmacro timeout-exception
  [msg]
  (if-cljs
    `(js/Error. ~msg)
    `(new TimeoutException ^String ~msg)))
