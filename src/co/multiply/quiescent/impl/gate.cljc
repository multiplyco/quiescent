(ns ^:no-doc co.multiply.quiescent.impl.gate
  (:require
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.impl.executor :as executor]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.quiescent.type #?@(:cljs [:refer [ICancellable TaskState]])]
    [co.multiply.conc.integer :as integer :refer [new-integer]]
    [co.multiply.conc.queue :as queue :refer [new-queue]]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [ask scoping]])
  #?(:clj (:import
            [co.multiply.quiescent.impl ICancellable TaskState])))


(defprotocol IGate
  (tryRun [this])
  (enqueue [this body])
  (acquire [this])
  (release [this]))


(deftype QueueEntry [task f])


(defn dec-when-positive
  [prev]
  (if (pos? prev)
    (unchecked-dec-int prev)
    prev))


(def ^:dynamic *current-gate*)


(deftype Gate [control-task permits queue]
  ICancellable
  (doCancel [_this msg] (call/doCancel control-task msg))
  (doCancelDirect [_this] (call/doCancelDirect control-task))
  (doCancelCascade [_this] (call/doCancelCascade control-task))

  IGate
  (enqueue [this f]
    (cond
      ;; Cancelled: return cancelled task.
      (call/isCancelled control-task)
      (doto (impl/-pending-task executor/delegate-virtual)
        (call/doCancel "Gate cancelled."))

      ;; Reentrant: run immediately.
      (identical? (ask *current-gate* nil) this)
      (doto (impl/-pending-task executor/delegate-virtual)
        (call/doRun f))

      :else
      (let [task (scoping [impl/*this*    control-task
                           *current-gate* this]
                   (impl/-pending-task executor/delegate-virtual))]
        (queue/add queue (QueueEntry. task f))
        (acquire this)
        task)))


  (tryRun [this]
    (if-some [unit (queue/poll queue)]
      (let [task (.-task ^QueueEntry unit)
            f    (.-f ^QueueEntry unit)]
        (doto task
          (subs/subscribe-callback sm/phase-settling
            (fn [_state]
              (if (call/isCancelled control-task)
                (release this)
                (tryRun this))))
          (call/doRun f)))
      (release this)))

  (acquire [this]
    (when (and (not (queue/isEmpty queue))
            (pos? (integer/getAndUpdate permits dec-when-positive)))
      (tryRun this)))

  (release [this]
    (integer/incrementAndGet permits)
    (when-not (call/isCancelled control-task)
      (acquire this))))


(defmacro gate
  [n]
  `(Gate. (impl/-pending-task executor/delegate-sync) (new-integer ~n) (new-queue)))
