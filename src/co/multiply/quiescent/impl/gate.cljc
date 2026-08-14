(ns ^:no-doc co.multiply.quiescent.impl.gate
  #?(:cljs (:require-macros co.multiply.quiescent.impl.gate))
  (:require
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.impl.executor :as executor]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.conc.integer :as integer :refer [new-integer]]
    [co.multiply.conc.queue :as queue :refer [new-queue]]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [ask assoc-scope]]))


(defprotocol IGate
  (tryRun [this])
  (enqueue [this body])
  (acquire [this])
  (release [this]))


(deftype QueueEntry [task f])


;; Scope marker carried by every queued task: which gate admitted it, and the
;; task that holds the permit. Reentrant enqueues are only run immediately
;; while the holder has not settled — a settled holder has provably released
;; its permit, so nothing can deadlock on its descendants acquiring normally.
(deftype GateGrant [gate task])


(defn dec-when-positive
  [prev]
  (if (pos? prev)
    (unchecked-dec-int prev)
    prev))


(def ^:dynamic *current-gate*)


;; A gate is a coordinator, not a task: it is off the cancellation tree,
;; holds no claim on the tasks that run through it, and has no lifecycle of
;; its own — GC manages it, so it is safe to `def` globally. Cancellation of
;; gated work flows exclusively through each task's own parentage.
(deftype Gate [permits queue]
  IGate
  (enqueue [this f]
    (let [grant (ask *current-gate* nil)]
      ;; Reentrant while the enclosing gated task still holds its permit:
      ;; run immediately on that permit, so a body that awaits a nested
      ;; enqueue can't deadlock the gate.
      (if (and (some? grant)
            (identical? (.-gate ^GateGrant grant) this)
            (not (call/atOrPast (.-task ^GateGrant grant) sm/phase-settling)))
        (doto (impl/-pending-task executor/delegate-virtual)
          (call/doRun f))

        ;; The task keeps its natural parent (structured under its creator,
        ;; like any task; `compel` detaches). The gate takes no ownership:
        ;; cancel the creator to tear down its cohort of gated work.
        (let [task (impl/-pending-task executor/delegate-virtual)]
          (call/setScope task
            (assoc-scope (call/getScope task) *current-gate* (GateGrant. this task)))
          (queue/add queue (QueueEntry. task f))
          (acquire this)
          task))))


  (tryRun [this]
    ;; Entries cancelled while queued are already settled; subscribing to them
    ;; would fire synchronously, so skip them iteratively — recursing through a
    ;; long run of them would overflow the stack and wedge the gate.
    (loop []
      (if-some [unit (queue/poll queue)]
        (let [task (.-task ^QueueEntry unit)
              f    (.-f ^QueueEntry unit)]
          (if (call/atOrPast task sm/phase-settling)
            (recur)
            (doto task
              (subs/subscribe-callback sm/phase-settling
                (fn [_state] (tryRun this)))
              (call/doRun f))))
        (release this))))

  (acquire [this]
    (when (and (not (queue/isEmpty queue))
            (pos? (integer/getAndUpdate permits dec-when-positive)))
      (tryRun this)))

  (release [this]
    (integer/incrementAndGet permits)
    (acquire this)))


(defmacro gate
  [n]
  `(Gate. (new-integer ~n) (new-queue)))
