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
    [co.multiply.scoped :refer [ask assoc-scope with-scope]]))


(defprotocol IGate
  (enqueue [this body])
  (tryEnter [this body])
  (acquire [this])
  (release [this]))


(deftype QueueEntry [task f])


;; Each admitted task has its own scope marker, but reentrant tasks share a
;; reference count. The permit returns only after every borrower settles,
;; including compelled borrowers that outlive their original permit holder.
(deftype GateGrant [task users])


(defn dec-when-positive
  [prev]
  (if (pos? prev)
    (unchecked-dec-int prev)
    prev))


(def ^:dynamic *gate-grants*)


(defn- inc-when-positive
  [prev]
  (if (pos? prev)
    (unchecked-inc-int prev)
    prev))


(defn- borrow-permit
  [gate]
  ;; Keep all enclosing gates so A -> B -> A can reenter A. A settled scope
  ;; holder cannot lend its permit to new descendants. Even if it settles
  ;; after this check, the atomic retain cannot resurrect a released permit.
  (when-some [grant (get (ask *gate-grants* nil) gate)]
    (let [users (.-users ^GateGrant grant)]
      (when (and (not (call/atOrPast (.-task ^GateGrant grant) sm/phase-settling))
              (pos? (integer/getAndUpdate users inc-when-positive)))
        users))))


(defn- start-task
  [gate task f users]
  (call/setScope task
    (assoc-scope (call/getScope task) *gate-grants*
      ;; A queued task carries its creator's scope, not the releasing task's.
      (assoc (with-scope (call/getScope task) (ask *gate-grants* nil))
        gate (GateGrant. task users))))
  ;; Subscribe before dispatch. If cancellation already won, this fires
  ;; immediately and returns our share exactly once; doRun then cannot start.
  (subs/subscribe-callback task sm/phase-settling
    (fn [_state]
      (when (zero? (integer/decrementAndGet users))
        (release gate))))
  (try
    (call/doRun task
      (fn []
        ;; A CLJS microtask cannot be unscheduled. Check again when the
        ;; executor actually dispatches, after any intervening cancellation.
        (when-not (call/atOrPast task sm/phase-writing)
          (f))))
    (catch #?(:clj Throwable :cljs :default) e
      ;; Dispatch itself can fail before the task runner's error handler is
      ;; entered. Settle the task so its permit is not permanently lost.
      (call/doApply task nil e)))
  task)


;; A gate is a coordinator, not a task: it is off the cancellation tree,
;; holds no claim on the tasks that run through it, and has no lifecycle of
;; its own — GC manages it, so it is safe to `def` globally. Cancellation of
;; gated work flows exclusively through each task's own parentage.
(deftype Gate [permits queue draining]
  IGate
  (enqueue [this f]
    ;; Keep natural parentage: the gate admits work but never owns it.
    (let [task (impl/-pending-task executor/delegate-virtual)]
      (if-some [users (borrow-permit this)]
        (start-task this task f users)
        (let [link (queue/add queue (QueueEntry. task f))]
          ;; Reclaim cancelled closures even if no permit ever becomes free.
          ;; Removing an entry already polled by the drainer is harmless.
          (subs/subscribe-callback task sm/phase-settling
            (fn [_state] (queue/remove link)))
          (acquire this)))
      task))


  (tryEnter [this f]
    (let [task (impl/-pending-task executor/delegate-virtual)]
      (if-some [users (borrow-permit this)]
        (start-task this task f users)
        ;; Never enqueue this task or jump past visible waiters. The atomic
        ;; decrement reserves the permit before dispatching the body.
        (if (and (queue/isEmpty queue)
              (pos? (integer/getAndUpdate permits dec-when-positive)))
          (start-task this task f (new-integer 1))
          (call/doCancel task "Gate has no immediately available permit.")))
      task))

  (acquire [this]
    ;; One drainer dispatches queued tasks. Concurrent or synchronous releases
    ;; record another pass instead of recursively draining. Counting signals
    ;; also closes the race between observing an empty queue and exiting.
    (when (= 1 (integer/incrementAndGet draining))
      (loop []
        (loop []
          (when (and (not (queue/isEmpty queue))
                  (pos? (integer/getAndUpdate permits dec-when-positive)))
            (if-some [unit (queue/poll queue)]
              (start-task this (.-task ^QueueEntry unit) (.-f ^QueueEntry unit) (new-integer 1))
              ;; Cancellation may have removed the last entry after our check.
              (release this))
            (recur)))
        (when (pos? (integer/decrementAndGet draining))
          (recur)))))

  (release [this]
    (integer/incrementAndGet permits)
    (acquire this)))


(defmacro gate
  [n]
  `(Gate. (new-integer ~n) (new-queue) (new-integer 0)))
