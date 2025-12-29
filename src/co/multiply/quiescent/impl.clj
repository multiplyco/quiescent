(ns co.multiply.quiescent.impl
  "Task implementation: state machine, executors, and the Task type.

   Tasks progress through phases gated by CAS transitions on a MachineLatch.
   See `state-machine` for the lifecycle definition and `Task` for coordination details."
  (:refer-clojure :exclude [await promise #_with-bindings])
  (:require
    [clojure.core :as cc]
    [co.multiply.machine-latch :as ml]
    [co.multiply.pathling :as p]
    [co.multiply.scoped :refer [ask assoc-scope current-scope scoping with-scope]])
  (:import
    [clojure.lang IBlockingDeref IDeref IFn IPending]
    [java.lang Thread$Builder]
    [java.lang.ref WeakReference]
    [java.util HashMap Iterator Set]
    [java.util.concurrent CancellationException CompletableFuture ConcurrentHashMap ConcurrentHashMap$KeySetView ConcurrentLinkedQueue ExecutorService Executors Future ThreadFactory]
    [java.util.concurrent.atomic AtomicInteger]
    [java.util.function BiConsumer]))


;; # Globals
;; ################################################################################
(def ^:dynamic *this*
  "The currently executing task, used for parent-child registration.

   When a task's body executes, `*this*` is set in scope to that task. Any child
   tasks created during execution register themselves with `*this*` for cascade
   cancellation via `WeakReference`. This scope binding is set in `-live-task`'s runner."
  nil)


(declare -pending-task task?)


;; # Executor
;; ################################################################################
(defonce ^{:doc "Virtual thread executor for IO-bound tasks. Creates a new virtual thread per task.
                 Default executor for tasks - use for network calls, file IO, and blocking operations."}
  ^ExecutorService virtual-executor
  (Executors/newThreadPerTaskExecutor
    (reify ThreadFactory
      (newThread
        [_ runnable]
        (-> (Thread/ofVirtual)
          (Thread$Builder/.name "q-io")
          (Thread$Builder/.unstarted runnable))))))


(defn submit
  "Submit a callable to an executor. Returns a Future that can be used for cancellation."
  [^ExecutorService executor ^Runnable f]
  (.submit executor f))


;; # Types and state machine schema/latch
;; ################################################################################
(def phase-pending
  "Task created, not yet executing"
  :pending)


(def phase-running
  "Body executing on executor"
  :running)


(def phase-grounding
  "Body done, resolving nested tasks (ground)"
  :grounding)


(def phase-transforming
  "Applying transform function to grounded value."
  :transforming)


(def phase-writing
  "Writing final result to task state. All completion paths converge here."
  :writing)


(def phase-settling
  "Result available, running `then` callbacks and cascade cancel.

   When a task reaches settling:
   - Its result is available (deref returns)
   - Grounded children are *at least* settling (we observed their settling)
   - Cascade cancellation is attempted on registered children (best effort)

   We do NOT guarantee children are quiescent - only that they've started winding down.
   This is intentional: strong lifecycle coupling prevents GC, risks deadlocks, and
   doesn't match real-world async (you can't force a thread to stop immediately)."
  :settling)


(def phase-quiescent
  "Lifecycle complete. Parent has done its work; children may still be settling.

   Quiescent means this task's lifecycle is finished, not that the entire subtree
   has stopped. Registered children received cancel signals (if alive and not compelled),
   orphaned tasks are unknown (may be GC'd or still running)."
  :quiescent)


(def action-run
  "Transition pending → running. Task body begins execution."
  :run)


(def action-ground
  "Transition to grounding. Entry point for grounding:
   - running → grounding (after executing function)
   - pending → grounding (direct value via doApply)"
  :ground)


(def action-transform
  "Transition grounding → transforming. Applies transform function to grounded value."
  :transform)


(def action-write
  "Transition to writing. Writes final result to state. Entry from:
   - pending/running (cancellation or exception)
   - grounding (no transform)
   - transforming (after transform)"
  :write)


(def action-settle
  "Transition writing → settling. Run settling subscriptions."
  :settle)


(def action-quiesce
  "Transition settling → quiescent. Teardown complete."
  :quiesce)


(def ^:private state-machine
  "Task lifecycle state machine (7 phases, 6 actions).

   Phases progress linearly in the happy path:
     pending → running → grounding → transforming → writing → settling → quiescent

   But cancellation or error can jump from early phases directly to writing:
     pending ────────────────────────────────────────╮
     running ───────────────────────────────────────╮│
     grounding ────────────────────────────────────╮││
     transforming ────────────────────────────────╮│││
                                                  ↓↓↓↓
                                               writing → settling → quiescent

   All completion paths (success, error, cancel) converge at writing.

   Phase boundary guarantees: When a thread reaches phase B, all subscriptions
   for phase A have been fired (though not necessarily completed—we don't wait).
   This is enforced by MachineLatch CAS: only one thread wins each transition.

   Subscriptions run synchronously on the transitioning thread, providing
   predictable ordering without spawning overhead."
  {:states      [phase-pending phase-running phase-grounding phase-transforming phase-writing phase-settling phase-quiescent]
   :transitions {action-run       {phase-pending phase-running}
                 action-ground    {phase-running phase-grounding
                                   phase-pending phase-grounding}
                 action-transform {phase-grounding phase-transforming}
                 action-write     {phase-pending      phase-writing
                                   phase-running      phase-writing
                                   phase-grounding    phase-writing
                                   phase-transforming phase-writing}
                 action-settle    {phase-writing phase-settling}
                 action-quiesce   {phase-settling phase-quiescent}}})


(def ^:private create-task-latch
  "Creates a MachineLatch with schema for Task lifecycle.

   Begins in pending phase."
  (ml/machine-latch-factory state-machine))


;; Subscription payload can be:
;; - Function: Called with TaskState when phase is reached (for then, finally, ground callbacks)
;; - Task: Strong ref, cascade cancel when phase reached (explicit child registration)
;; - WeakReference[Task]: GC-friendly cascade cancel. Used for:
;;   - Parent-child relationships (normal child registration via *this*)
;;   - Backward cancellation in chains (downstream cancels upstream if still pending)
;;   If referent was collected, the cancel is silently skipped.
(deftype TaskState [^boolean exceptional ^boolean cancelled result])


(definterface ITask
  (getState [])
  (getScope [])
  (getNow [default])
  (^boolean awaitPhase [phase])
  (^boolean awaitPhaseMillis [phase ^long timeout-ms])
  (^boolean awaitPhaseDur [phase ^java.time.Duration timeout])
  ;; Entry points
  (doRun [f])
  (doRun [f tf])
  (doApply [v])
  (doApply [v e])
  (doApply [v e tf])
  ;; Phases
  (doGround [v tf])
  (doTransform [res tf])
  (doWrite [task-state])
  (doSettle [])
  (doQuiesce [])
  ;; Cancellation
  (doCancel [^String msg])
  (doCancelDirect [])
  (doCancelCascade [])
  ;; Subscription
  (doSubscribe [sub])
  (doUnsubscribe [sub])
  (runSubscriptions [])
  ;; Chain transformations
  (doThen [executor f])
  (doHandle [executor f])
  (doCatch [executor f])
  (doCatchTyped [executor type-handler-pairs])
  ;; Chain side-effects
  (doOk [executor f])
  (doErr [executor f])
  (doDone [executor f])
  ;; Chain teardown/cleanup
  (doFinally [executor f]))


(defprotocol ITaskable
  (taskable? [this] "Is this an async value that as-task handles specially?")
  (groundable? [this] "Should ground automatically resolve this?")
  (as-task [this] "Convert to a Task"))


(definterface ISubscription
  (phase [])
  (run [task-state]))


(deftype TeardownSubscription [phase ^ITask target]
  ISubscription
  (phase [_this] phase)
  (run [_this _state]
    (ITask/.doCancelCascade target)))


(deftype WeakTeardownSubscription [phase ^WeakReference target-ref]
  ISubscription
  (phase [_this] phase)
  (run [_this _state]
    (some-> target-ref (WeakReference/.get) (ITask/.doCancelCascade))))


(deftype CallbackSubscription [phase f]
  ISubscription
  (phase [_this] phase)
  (run [_this state]
    (try (f state)
      (catch Throwable e
        (throw (IllegalStateException. "Subscription threw during task lifecycle." e))))))


(deftype UnsubSubscription [phase target sub]
  ISubscription
  (phase [_this] phase)
  (run [_this _state]
    (ITask/.doUnsubscribe target sub)))


(deftype CancelFutureSubscription [phase fut-or-promise]
  ISubscription
  (phase [_this] phase)
  (run [_this state]
    (when (.-cancelled ^TaskState state)
      (if (instance? Future fut-or-promise)
        (Future/.cancel fut-or-promise true)
        (-> fut-or-promise (deref) (Future/.cancel true))))))


;; # Low-level helpers
;; ################################################################################
(defmacro subscribe-teardown-weak
  "Attach a subscription to `task`, tearing down task `target` when reaching `phase`."
  [task phase target]
  `(ITask/.doSubscribe ~task (WeakTeardownSubscription. ~phase (WeakReference. ~target))))


(defmacro subscribe-teardown
  "Attach a subscription to `task`, tearing down task `target` when reaching `phase`."
  [task phase target]
  `(ITask/.doSubscribe ~task (TeardownSubscription. ~phase ~target)))


(defmacro subscribe-callback
  "Attach a subscription to `task`, running `f` when reaching `phase`

   `f` receives the current state of `this` as its only argument."
  [task phase f]
  `(ITask/.doSubscribe ~task (CallbackSubscription. ~phase ~f)))


(defmacro subscribe-unsub
  "Attach a subscription to `task` that removes `sub` from `target` when `task` enters `phase`.

   Used for cleanup: when a child task settles, it removes its teardown subscription
   from the parent to prevent memory leaks in long-running parent tasks."
  [task phase target sub]
  `(ITask/.doSubscribe ~task (UnsubSubscription. ~phase ~target ~sub)))


(defmacro subscribe-cancel-future
  "Attach a subscription to `task`, cancelling the future (or promise
   containing a future) `fut-or-promise` when the `task` reaches `phase`."
  [task phase fut-or-promise]
  `(ITask/.doSubscribe ~task (CancelFutureSubscription. ~phase ~fut-or-promise)))


(defmacro throw-boxed-error
  "Extract value from TaskState, throwing if exceptional.

   Used by deref: if the task completed exceptionally, throws the boxed exception.
   Otherwise returns the result value."
  [state]
  `(let [^TaskState state# ~state]
     (if (.-exceptional state#)
       (throw (.-result state#))
       (.-result state#))))


(defn- filter-by-index
  "Returns a vector of elements from coll where (pred index) is true.

   Example: (filter-by-index even? [:a :b :c :d]) => [:a :c]"
  [pred coll]
  (into [] (comp
             (map-indexed (fn [idx v] (if (pred idx) v ::remove)))
             (remove (partial identical? ::remove)))
    coll))


;; # Grounding
;; ################################################################################

;; Sentinel wrapper indicating a value is already fully resolved and needs no grounding.
;; Used by combinators like `tmerge` and `tdo` that know their output contains no tasks.
(deftype Plain [v])


(defn plain
  "Wrap a value to indicate it contains no nested tasks and should skip grounding.

   This is an optimization for combinators that produce values from already-resolved
   task results (e.g., `qmerge`, `qdo`). The `ground` phase checks for `Plain` and
   unwraps directly instead of walking the data structure.

   Internal use only - not exposed in the public API."
  [v]
  (Plain. v))


(defn plain?
  "Returns true if v is wrapped in Plain, indicating it needs no grounding."
  [v]
  (instance? Plain v))


(defn- create-lookup-fn
  [matches-vec ^objects results-arr]
  (let [matches-count (unchecked-int (count matches-vec))
        m             (HashMap. matches-count)]
    (loop [idx (unchecked-int 0)]
      (if (< idx matches-count)
        (do (HashMap/.put m (matches-vec idx) (aget results-arr idx))
          (recur (unchecked-inc-int idx)))
        (partial HashMap/.get m)))))


(defn- ground
  "Resolve nested tasks in a value, replacing them with their results.

   Walks `v` looking for values that satisfy `groundable?` (Tasks, CompletableFutures,
   etc.). Found tasks are awaited in parallel. When all complete, they're replaced
   with their dereferenced values in the original data structure.

   Optimized by task count:
   - 0 tasks: Return immediately (no coordination overhead)
   - 1 task: Single subscription (minimal overhead)
   - N tasks: AtomicInteger countdown + object-array (full coordination)

   Callback signature:
   - Success: (on-complete grounded-value nil)
   - Failure: (on-complete nil exceptional-task-state)

   CAS semantics is handled by `on-complete`."
  [this v on-complete]
  (if (plain? v)
    (on-complete (.-v ^Plain v) nil)
    (let [{:keys [matches nav]} (p/path-when v groundable?)
          task-count (unchecked-int (count matches))]
      (case task-count
        ;; The result contained no inner tasks; return as is.
        0 (on-complete v nil)
        ;; There's exactly one inner task. Hand over execution to it via a subscription.
        1 (let [inner-task (as-task (first matches))]
            ;; When the inner task starts settling, remove the parent teardown.
            ;; It's no longer needed, since the parent can't cancel the child
            ;; after that anyway.
            (subscribe-unsub inner-task phase-settling this
              ;; When this task enters the settling phase for any reason, cancel the inner task.
              ;; Reference is held strongly: there are no other contenders to finish ahead of,
              ;; and so no use of a weak reference.
              (subscribe-teardown this phase-settling inner-task))
            ;; When the inner task produces a value, insert it on the data structure
            ;; in place of the task and call on-complete.
            (subscribe-callback inner-task phase-settling
              (fn inner-task-completion
                [^TaskState inner-task-state]
                (if (.-exceptional inner-task-state)
                  (on-complete nil inner-task-state)
                  (on-complete
                    (p/update-paths v nav (constantly (.-result inner-task-state)))
                    nil)))))
        ;; There are many inner tasks. Await them concurrently and then hand over execution
        ;; to the one that completes last.
        (let [task-done-count (AtomicInteger. task-count)
              task-results    (object-array task-count)]
          (loop [idx (unchecked-int 0)]
            (when (< idx task-count)
              (let [inner-task (as-task (matches idx))]
                (doto inner-task
                  ;; When the inner task starts settling, remove the parent teardown.
                  ;; It's no longer needed, since the parent can't cancel the child
                  ;; after that anyway.
                  (subscribe-unsub phase-settling this
                    ;; When this task enters the settling phase for any reason, cancel the inner task.
                    ;; References are held weakly so that they can be GC'd when tasks finish at different
                    ;; times.
                    (subscribe-teardown-weak this phase-settling inner-task))
                  ;; Runs when a value is available for the inner task.
                  (subscribe-callback phase-settling
                    (fn resolve-inner-task
                      [^TaskState inner-task-state]
                      (if (.-exceptional inner-task-state)
                        ;; An exception leads to immediate termination of ground, and the
                        ;; subsequent cancellation of all sibling inner tasks.
                        (on-complete nil inner-task-state)
                        (do (aset task-results idx (.-result inner-task-state))
                          (when (zero? (.decrementAndGet task-done-count))
                            ;; When all inner tasks have produced values, replace them within the
                            ;; data structure with their resolved values.
                            (on-complete
                              (p/update-paths v nav (create-lookup-fn matches task-results))
                              nil))))))))
              (recur (unchecked-inc-int idx)))))))))


;; # Task type
;; ################################################################################
;;
;; ## Double-CAS Coordination
;;
;; Two CAS operations serve distinct purposes:
;;
;; 1. **MachineLatch CAS** (ml/transition!): Grants execution ownership.
;;    "I get to do this phase's work." Only one thread wins; losers return false.
;;
;; 2. **State CAS** (AtomicReference): Eliminates write races for the result.
;;    Uses `latch` as sentinel. Once TaskState replaces it, no further writes
;;    are possible—the sentinel is gone.
;;
;; In doWrite, both CAS operations occur:
;;   (when (and (ml/transition! latch action-write)    ; Execution ownership
;;           (AtomicReference/.compareAndSet state latch task-state))  ; Write race
;;     ...)
;;
;; The MachineLatch CAS comes first. If it succeeds, this thread owns the phase.
;; The state CAS then writes the result. Since only the owner attempts the state
;; CAS, it's guaranteed to succeed (invariant, not race).
;;
(deftype Task [executor latch ^:volatile-mutable state subscriptions compelled scope]
  IDeref
  (deref [this]
    (Task/.awaitPhase this phase-settling)
    (throw-boxed-error state))

  IBlockingDeref
  (deref [this timeout-ms timeout-val]
    (if (Task/.awaitPhaseMillis this phase-settling ^long timeout-ms)
      (throw-boxed-error state)
      timeout-val))


  IPending
  (isRealized [_this]
    (ml/at-or-past? latch phase-settling))

  ITask
  ;; Info
  (getState [_this]
    (merge {:phase         (ml/get-state latch)
            :subscriptions subscriptions
            :compelled     compelled}
      (when (instance? TaskState state)
        {:exceptional (.-exceptional ^TaskState state)
         :cancelled   (.-cancelled ^TaskState state)
         :result      (.-result ^TaskState state)})))

  (getScope [_this] scope)

  (getNow [_this default]
    (if (instance? TaskState state)
      (throw-boxed-error state)
      default))

  ;; Waiting
  (^boolean awaitPhase [_this phase]
    (ml/await latch phase))

  (^boolean awaitPhaseMillis [_this phase ^long timeout-ms]
    (ml/await-millis latch phase timeout-ms))

  (^boolean awaitPhaseDur [_this phase ^java.time.Duration duration]
    (ml/await-dur latch phase duration))

  ;; ## Entry points
  (doRun [this f]
    (Task/.doRun this f nil))

  (doRun [this f tf]
    (if (ml/transition! latch action-run)
      (do (Task/.runSubscriptions this)
        ;; If executor is nil, run synchronously (caller asserts f is small/fast).
        ;; Otherwise, submit to executor with cancellation support.
        (letfn [(execute-function
                  []
                  (with-scope (assoc-scope scope *this* this)
                    (try (Task/.doGround this (f) tf)
                      (catch Throwable e
                        (Task/.doWrite this (TaskState. true false e))))))]
          (if executor
            (subscribe-cancel-future this phase-settling (submit executor execute-function))
            (execute-function)))
        true)
      false))

  (doApply [this v]
    (.doGround this v nil))

  (doApply [this v e]
    (if e
      (Task/.doWrite this (TaskState. true false e))
      (Task/.doGround this v nil)))

  (doApply [this v e tf]
    (if e
      (Task/.doWrite this (TaskState. true false e))
      (Task/.doGround this v tf)))

  (doGround [this res tf]
    (if (ml/transition! latch action-ground)
      (do (Task/.runSubscriptions this)
        (ground this res
          (fn do-ground
            [grounded-value exceptional-task-state]
            (cond
              ;; Grounding failed: an inner task returned its (exceptional) task state.
              exceptional-task-state (Task/.doWrite this exceptional-task-state)
              ;; Grounding was successful, and we have a transformation function.
              tf (Task/.doTransform this grounded-value tf)
              ;; Grounding was successful, there's no transformation function given.
              :else (Task/.doWrite this (TaskState. false false grounded-value)))))
        true)
      false))

  (doTransform [this res tf]
    (when (ml/transition! latch action-transform)
      (Task/.runSubscriptions this)
      ;; `tf` is an unknown function. If executor is nil, we run synchronously
      ;; (caller asserts tf is small/fast). Otherwise, submit to executor.
      (letfn [(run-transform-function
                []
                (with-scope (assoc-scope scope *this* this)
                  (try
                    (ground this (tf res)
                      (fn do-tf-ground
                        [grounded-value exceptional-task-state]
                        (if exceptional-task-state
                          (Task/.doWrite this exceptional-task-state)
                          (Task/.doWrite this (TaskState. false false grounded-value)))))
                    (catch Throwable e
                      (.doWrite this (TaskState. true false e))))))]
        (if executor
          (subscribe-cancel-future this phase-settling (submit executor run-transform-function))
          (run-transform-function)))))

  (doWrite [this task-state]
    (if (ml/transition! latch action-write)
      (do (set! state task-state)
        (.runSubscriptions this)
        (.doSettle this)
        true)
      false))

  (doSettle [this]
    (when (ml/transition! latch action-settle)
      (.runSubscriptions this)
      (.doQuiesce this)))

  (doQuiesce [this]
    (when (ml/transition! latch action-quiesce)
      (.runSubscriptions this)))

  ;; Cancellation
  (doCancel [this ^String msg]
    (.doWrite this (TaskState. true true (CancellationException. msg))))

  (doCancelDirect [this]
    (doto (-pending-task virtual-executor)
      (Task/.doRun
        (fn []
          (or (.doCancel this "Task cancelled directly.")
            (do
              (Task/.awaitPhase this phase-quiescent)
              false))))))

  ;; Cascade cancel from parent. Compelled tasks resist - they were marked important
  ;; at creation time and should complete even if parent is cancelled.
  (doCancelCascade [this]
    (and (not compelled) (.doCancel this "Task cancelled via cascade.")))

  ;; ## Subscriptions

  ;; Run synchronously on the thread that triggered the phase transition.
  ;; This provides predictable ordering but means callbacks should be fast.
  ;;
  ;; Phase boundary guarantee: When a thread reaches phase B, all subscriptions
  ;; for phase A have been fired off (not necessarily completed—we don't wait).
  ;; This is stronger than "eventually consistent" but avoids GC/deadlock issues.
  ;;
  ;; We read state once at iteration start - this is safe because state is write-once.
  ;; By the time settling-phase subscriptions run, state is already a TaskState.
  ;;
  ;; Multiple threads may call runSubscriptions concurrently (lifecycle transitions vs
  ;; doSubscribe). We gate callback execution on successful CHM remove, ensuring each
  ;; subscription fires exactly once. Exceptions are logged and swallowed to ensure
  ;; remaining subscriptions still run.
  ;;
  (runSubscriptions [this]
    (or (Set/.isEmpty subscriptions)
      (with-scope scope
        (try
          (let [iter (Iterable/.iterator subscriptions)]
            (while (.hasNext iter)
              (let [sub (Iterator/.next iter)]
                (when (and (ml/at-or-past? latch (ISubscription/.phase sub))
                        (ConcurrentHashMap$KeySetView/.remove subscriptions sub))
                  (ISubscription/.run sub state)))))
          (catch Throwable e
            (Task/.doWrite this (TaskState. true false e)))))))

  (doSubscribe [_this sub]
    (ConcurrentHashMap$KeySetView/.add subscriptions sub)
    (when (and (ml/at-or-past? latch (ISubscription/.phase sub))
            (ConcurrentHashMap$KeySetView/.remove subscriptions sub))
      (with-scope scope
        (ISubscription/.run sub state)))
    sub)

  (doUnsubscribe [_this sub]
    (ConcurrentHashMap$KeySetView/.remove subscriptions sub))


  ;; ## Chaining
  ;;
  ;; All chaining methods create a new "link" task that runs when the source settles.
  ;;
  ;; In general, we skip starting a new thread if a chain operation can't execute given
  ;; the state conditions. For example, if the state is exceptional, `doThen` can't possibly
  ;; execute, so we don't even attempt it. Instead, we just reference the state of `this`
  ;; synchronously in `link`.
  ;;
  ;; Backward cancellation: If the link task is cancelled before `this` task settles,
  ;; we attempt to cancel `this` task too. This uses WeakReference so GC isn't blocked.
  ;; The cancel can only succeed if this task hasn't reached settling yet - once
  ;; settling begins, cancellation is no longer possible (enforced by MachineLatch).

  (doThen [this executor f]
    (let [link (-pending-task executor)]
      ;; Start `then` on the chained `link` task when `this` has a result.
      (subscribe-callback this phase-settling
        (fn setup-then
          [^TaskState state]
          (if (.-exceptional state)
            ;; If the state is unhandleable, don't do any additional work. Just
            ;; reference the existing state, and move on.
            (ITask/.doWrite link state)
            ;; If the state is handleable, run the function.
            (ITask/.doRun link (fn then [] (f (.-result state)))))))
      ;; If the chained `link` task is settling, attempt to cancel `this`.
      ;; Can only happen if `link` is cancelled, since otherwise it can't
      ;; reach the `settling` phase without `this` reaching `settling` first,
      ;; which is an uncancellable phase (action-write can't transition from settling).
      (doto link (subscribe-teardown-weak phase-settling this))))

  (doCatch [this executor f]
    (let [link (-pending-task executor)]
      (subscribe-callback this phase-settling
        (fn setup-catch
          [^TaskState state]
          (if (and (.-exceptional state) (not (.-cancelled state)))
            (ITask/.doRun link (fn catch [] (f (.-result state))))
            (ITask/.doWrite link state))))
      (doto link (subscribe-teardown-weak phase-settling this))))

  (doCatchTyped [this executor type-handler-pairs]
    (assert (even? (count type-handler-pairs)) "Must have receive an even number of arguments.")
    (assert (every? class? (filter-by-index even? type-handler-pairs)) "Every function must have an associated class.")
    (assert (every? fn? (filter-by-index odd? type-handler-pairs)) "Every class must have an associated function.")
    (let [link (-pending-task executor)]
      (subscribe-callback this phase-settling
        (fn setup-catch
          [^TaskState state]
          (let [result (.-result state)]
            (cond
              (not (.-exceptional state))
              (ITask/.doWrite link state)

              (.-cancelled state)
              (ITask/.doWrite link state)

              :else
              ;; Find the first matching pair given, then run the function.
              (let [c (count type-handler-pairs)]
                (loop [idx (unchecked-int 0)]
                  (if (< idx c)
                    (let [err-class   (get type-handler-pairs idx)
                          handler-idx (unchecked-inc-int idx)]
                      (if (instance? err-class result)
                        (let [handler (get type-handler-pairs handler-idx)]
                          (ITask/.doRun link (fn catch [] (handler result))))
                        (recur (unchecked-inc-int handler-idx))))
                    ;; If there are no matches, reference the existing state and move on.
                    (ITask/.doWrite link state))))))))
      (doto link (subscribe-teardown-weak phase-settling this))))


  (doHandle [this executor f]
    (let [link (-pending-task executor)]
      (subscribe-callback this phase-settling
        (fn setup-handle
          [^TaskState state]
          (if (.-cancelled state)
            (ITask/.doWrite link state)
            (ITask/.doRun link
              (fn handle
                []
                (if (.-exceptional state)
                  (f nil (.-result state))
                  (f (.-result state) nil)))))))
      (doto link (subscribe-teardown-weak phase-settling this))))


  (doOk [this executor f]
    (let [link  (-pending-task nil)
          scope (ITask/.getScope link)]
      (subscribe-callback this phase-settling
        (fn setup-ok
          [^TaskState state]
          (if (.-exceptional state)
            (ITask/.doWrite link state)
            (subscribe-cancel-future link phase-settling
              (submit executor
                (fn ok
                  []
                  (with-scope (assoc-scope scope *this* link)
                    (try (f (.-result state))
                      (ITask/.doWrite link state)
                      (catch Throwable e
                        (ITask/.doApply link nil e))))))))))
      (doto link (subscribe-teardown-weak phase-settling this))))


  (doErr [this executor f]
    (let [link  (-pending-task nil)
          scope (ITask/.getScope link)]
      (subscribe-callback this phase-settling
        (fn setup-err
          [^TaskState state]
          (if (and (.-exceptional state) (not (.-cancelled state)))
            (subscribe-cancel-future link phase-settling
              (submit executor
                (fn err
                  []
                  (with-scope (assoc-scope scope *this* link)
                    (try (f (.-result state))
                      (ITask/.doWrite link state)
                      (catch Throwable e
                        (ITask/.doApply link nil e)))))))
            (ITask/.doWrite link state))))
      (doto link (subscribe-teardown-weak phase-settling this))))


  (doDone [this executor f]
    (let [link  (-pending-task nil)
          scope (ITask/.getScope link)]
      (subscribe-callback this phase-settling
        (fn setup-done
          [^TaskState state]
          (if (.-cancelled state)
            (ITask/.doWrite link state)
            (subscribe-cancel-future link phase-settling
              (submit executor
                (fn done
                  []
                  (with-scope (assoc-scope scope *this* link)
                    (try
                      (if (.-exceptional state)
                        (f nil (.-result state))
                        (f (.-result state) nil))
                      (ITask/.doWrite link state)
                      (catch Throwable e
                        (ITask/.doApply link nil e))))))))))
      (doto link (subscribe-teardown-weak phase-settling this))))


  (doFinally [this executor f]
    (let [link  (-pending-task nil)
          scope (ITask/.getScope link)]
      (subscribe-callback this phase-settling
        (fn setup-finally
          [^TaskState state]
          (submit executor
            (fn finally
              []
              (with-scope (assoc-scope scope *this* link)
                (let [result    (.-result state)
                      cancelled (.-cancelled state)]
                  (try
                    (if (.-exceptional state)
                      (f nil result cancelled)
                      (f result nil cancelled))
                    (Task/.doWrite link state)
                    (catch Throwable e
                      (Task/.doApply link nil e)))))))))
      (doto link (subscribe-teardown-weak phase-settling this)))))


;; # Task construction
;; ################################################################################
(defn -pending-task
  "Create a new Task in pending state. The latch is used as the initial sentinel
   value in the state AtomicReference, allowing CAS to detect first write."
  (^Task [executor]
   (-pending-task executor false))
  (^Task [executor compelled]
   (let [latch  (create-task-latch)
         t      (Task. executor
                  latch
                  nil
                  (ConcurrentHashMap/newKeySet)
                  compelled
                  (current-scope))
         parent (ask *this*)]
     (when parent
       ;; Unsubscribe from the parent, cleaning up the subscription, when the child settles.
       ;; If this is not done properly, there will be memory leaks. Consider a task that
       ;; runs for the entire lifetime of a program and spawns subtasks. If the children
       ;; didn't remove their teardown connections from the parent, the parent would collect
       ;; an unbounded number of stale subscriptions forever.
       (subscribe-unsub t phase-settling parent
         ;; Register with parent for cascade cancellation. When parent settles,
         ;; cascade cancel is best-effort: child may be compelled (resists),
         ;; or already past cancellable phases (no-op).
         (subscribe-teardown parent phase-settling t)))
     t)))


(defn interrupted?
  []
  (.isInterrupted (Thread/currentThread)))


(defn -do-run
  [t f]
  (if (interrupted?)
    (doto t (Task/.doCancel "Task created on interrupted thread."))
    (doto t (Task/.doRun f))))


(defmacro do-runner
  [executor & body]
  `(doto (-pending-task ~executor)
     (-do-run (fn runner# [] ~@body))))


(defmacro do-applier
  "Create a task that grounds a value (and optionally transforms it).

   Unlike `-live-task` which runs a function, this applies a value directly.
   The value is grounded (nested tasks resolved in parallel), then optionally
   transformed by `tf`. Used by coordination functions like `then`, `qmerge`,
   and `qdo` to await multiple values and apply a combining function.

   Arities:
     `[executor v]`       - Ground v, no transform
     `[executor v e]`     - If e, fail with e; otherwise ground v
     `[executor v e tf]`  - If e, fail with e; otherwise ground v then apply tf"
  ([executor v]
   `(doto (-pending-task ~executor) (Task/.doApply ~v)))
  ([executor v e]
   `(doto (-pending-task ~executor) (Task/.doApply ~v ~e)))
  ([executor v e tf]
   `(doto (-pending-task ~executor) (Task/.doApply ~v ~e ~tf))))


;; # Promise definition
;; ################################################################################
(deftype Promise [^Task task]
  IDeref
  (deref [_this] (deref task))

  IBlockingDeref
  (deref [_this timeout-ms timeout-val] (deref task timeout-ms timeout-val))


  IPending
  (isRealized [_this] (.isRealized task))

  IFn
  (invoke [this v] (.doApply task v) this)

  ITask
  ;; Info
  (getState [_this] (.getState task))
  (getScope [_this] (.getScope task))
  (getNow [_this default] (.getNow task default))

  ;; Waiting
  (^boolean awaitPhase [_this phase] (.awaitPhase task phase))
  (^boolean awaitPhaseMillis [_this phase ^long timeout-ms] (.awaitPhaseMillis task phase timeout-ms))
  (^boolean awaitPhaseDur [_this phase ^java.time.Duration duration] (.awaitPhaseDur task phase duration))

  ;; ## Entry points
  (doRun [_this _f] (throw (Exception. "Promises can't run functions.")))
  (doRun [_this _f _tf] (throw (Exception. "Promises can't run functions.")))
  (doApply [_this v] (.doApply task v nil nil))
  (doApply [_this v e] (.doApply task v e nil))
  (doApply [_this _v _e _tf] (throw (Exception. "Promises can't run functions.")))
  (doGround [_this res tf] (.doGround task res tf))
  (doTransform [_this res tf] (.doTransform task res tf))
  (doWrite [_this task-state] (.doWrite task task-state))
  (doSettle [_this] (.doSettle task))
  (doQuiesce [_this] (.doQuiesce task))
  ;; Cancellation is not supported on promises - they are externally controlled
  (doCancel [_this ^String msg] (.doCancel task msg))
  (doCancelDirect [_this]
    (throw (UnsupportedOperationException.
             "Cannot cancel a Promise. Promises are externally controlled - use fail to complete with an error.")))
  (doCancelCascade [_this] (.doCancelCascade task))
  (runSubscriptions [_this] (.runSubscriptions task))
  (doSubscribe [_this sub] (.doSubscribe task sub))
  (doUnsubscribe [_this sub] (.doUnsubscribe task sub))
  (doThen [_this executor f] (.doThen task executor f))
  (doCatch [_this executor f] (.doCatch task executor f))
  (doCatchTyped [_this executor type-handler-pairs]
    (.doCatch task executor type-handler-pairs))
  (doHandle [_this executor f] (.doHandle task executor f))
  (doOk [_this executor f] (.doOk task executor f))
  (doErr [_this executor f] (.doErr task executor f))
  (doDone [_this executor f] (.doDone task executor f))
  (doFinally [_this executor f] (.doFinally task executor f)))


;; # Task conversion
;; ################################################################################
(extend-protocol ITaskable
  Task
  (taskable? [_] true)
  (groundable? [_] true)
  (as-task [t] t)

  Promise
  (taskable? [_] true)
  (groundable? [_] true)
  (as-task [p] p)

  CompletableFuture
  (taskable? [_] true)
  (groundable? [_] true)
  (as-task [^CompletableFuture cf]
    ;; `nil` is used as the executor since it never will be used (doApply path).
    (let [task (doto (-pending-task nil)
                 (subscribe-cancel-future phase-settling cf))]
      (CompletableFuture/.whenCompleteAsync cf
        ^BiConsumer (fn [v e] (ITask/.doApply task v e))
        virtual-executor)
      task))


  Future
  (taskable? [_] true)
  (groundable? [_] true)
  (as-task [^Future future]
    (doto (-pending-task virtual-executor)
      (ITask/.doRun #(.get future))
      (subscribe-cancel-future phase-settling future)))

  nil
  (taskable? [_] false)
  (groundable? [_] false)
  (as-task [_] (do-applier nil nil))

  Object
  (taskable? [_] false)
  (groundable? [_] false)
  (as-task [obj] (do-applier nil obj)))


;; # Task coordination
;; ################################################################################
(defn race
  ([tasks]
   (race tasks nil))
  ([tasks {:keys [tf release]}]
   (let [tasks (mapv as-task tasks)]
     (cond
       ;; Racing nothing returns nil.
       (empty? tasks)
       (do-applier nil nil)

       ;; In a race with one task, the task given wins (or throws).
       (= 1 (count tasks))
       (first tasks)

       :else
       (let [winner     (-pending-task nil)
             task-latch (AtomicInteger. (count tasks))
             errors     (ConcurrentLinkedQueue.)]
         (doseq [t tasks]
           ;; Register participating tasks to be torn down when `winner` settles.
           (subscribe-teardown winner phase-settling t)
           ;; Set up the race.
           (subscribe-callback t phase-settling
             (fn [^TaskState state]
               (cond
                 ;; Success - race to apply value
                 (not (.-exceptional state))
                 (let [result (.-result state)]
                   (when-not (Task/.doApply winner result nil tf)
                     (when release
                       (submit virtual-executor
                         (fn []
                           (with-scope (ITask/.getScope t)
                             (release result)))))))

                 ;; Exception - collect and check if last
                 :else
                 (do
                   (.add errors (.-result state))
                   (when (zero? (AtomicInteger/.decrementAndGet task-latch))
                     ;; All tasks failed - combine errors
                     (let [all-errors (vec errors)]
                       (Task/.doApply winner nil
                         (if (= 1 (count all-errors))
                           (first all-errors)
                           (ex-info "All tasks failed." {:errors all-errors}))))))))))
         winner)))))


(comment
  (scoping [hello :dog]
    @(do-runner virtual-executor
       (let [#_#_inner-task (do-runner virtual-executor)]
         (plain
           {:hello  (ask hello)
            :parent (ask *this*)
            #_#_:task-scope (ITask/.getScope inner-task)
            #_#_:current-scope (current-scope)}))))

  (scoping [hello :dog]
    @(do-runner nil
       (current-scope)))

  #__)
