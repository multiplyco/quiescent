(ns co.multiply.quiescent.adapter.core-async
  (:require
    [clojure.core.async :as async]
    [co.multiply.quiescent.impl :as impl])
  (:import
    [clojure.core.async.impl.channels ManyToManyChannel]
    [co.multiply.quiescent.impl TaskState]))


(extend-protocol impl/ITaskable
  ManyToManyChannel
  (taskable? [_] true)
  (groundable? [_]
    ;; Channels found in return values of tasks should not
    ;; be automatically converted to tasks and have their
    ;; return values inlined.
    false)
  (as-task [^ManyToManyChannel ch]
    (let [task (impl/-pending-task impl/virtual-executor)]
      (async/take! ch (fn [v] (impl/submit impl/virtual-executor #(.doApply task v))))
      task)))


(defn as-chan
  "Convert a task to a core.async channel.

   Returns a channel that receives the task's result when it settles.
   If the task fails, the exception is thrown when taking from the channel.
   Note: nil results throw IllegalArgumentException (can't put nil on channel)."
  [t]
  (let [ch (async/chan 1)]
    (impl/subscribe-callback (impl/as-task t) impl/phase-settling
      (fn [state]
        (async/put! ch state)
        (async/close! ch)))
    (async/go
      (let [^TaskState state (async/<! ch)
            res              (.-result state)]
        (cond
          (.-exceptional state) (throw res)
          (nil? res) (throw (IllegalArgumentException. "Task returned `nil`, but this cannot be put on channel."))
          :else res)))))
