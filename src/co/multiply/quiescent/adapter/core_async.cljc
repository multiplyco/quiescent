(ns co.multiply.quiescent.adapter.core-async
  (:require
    #?(:cljs [cljs.core.async.impl.channels :refer [ManyToManyChannel]])
    [clojure.core.async :as async]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.impl.executor :refer [delegate-virtual]]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.quiescent.type :as type :refer [ITaskable #?(:cljs TaskState)]]
    [co.multiply.quiescent.type.call :as call])
  #?(:clj (:import
            [clojure.core.async.impl.channels ManyToManyChannel]
            [co.multiply.quiescent.impl TaskState])))


(extend-protocol ITaskable
  ManyToManyChannel
  (taskable? [_] true)
  (groundable? [_]
    ;; Channels found in return values of tasks should not
    ;; be automatically converted to tasks and have their
    ;; return values inlined.
    false)
  (as-task [ch]
    (let [task (impl/-pending-task delegate-virtual)]
      (async/take! ch (fn [v] (delegate-virtual #(call/doApply task v))))
      task)))


(defn ^:no-doc get-state-result
  "Extract the result value from a TaskState.

   Returns the stored result, which may be either the successful return value
   or an exception object (check with `get-state-exceptional`)."
  [^TaskState state]
  (.-result state))


(defn ^:no-doc get-state-exceptional
  "Check if a TaskState represents an exceptional (failed) completion.

   Returns truthy if the task failed with an exception, falsy if it completed
   successfully. When truthy, the result from `get-state-result` is the exception."
  [^TaskState state]
  (.-exceptional state))


(defn as-chan
  "Convert a task to a core.async channel.

   Returns a channel that receives the task's result when it settles.
   If the task fails, the exception is thrown when taking from the channel.
   Note: nil results throw IllegalArgumentException (can't put nil on channel)."
  [t]
  (let [ch (async/chan 1)]
    (subs/subscribe-callback (type/as-task t) sm/phase-settling
      (fn [state]
        (async/put! ch state)
        (async/close! ch)))
    (async/go
      (let [state (async/<! ch)
            res   (get-state-result state)]
        (cond
          (get-state-exceptional state) (throw res)
          (nil? res) (throw (type/illegal-argument-exception "Task returned `nil`, but this cannot be put on channel."))
          :else res)))))
