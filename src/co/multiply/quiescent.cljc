(ns co.multiply.quiescent
  "Composable async tasks with automatic parallelization and grounding."
  #?(:cljs (:require-macros co.multiply.quiescent))
  (:refer-clojure :exclude [await promise time])
  (:require
    [clojure.core]
    [co.multiply.machine-latch :as ml]
    [co.multiply.machine-latch.impl :as ml.impl]
    [co.multiply.pathling :as p]
    [co.multiply.quiescent.impl :as impl :refer [-pending-task do-applier do-runner #?(:cljs Promise) #?(:cljs Task)]]
    [co.multiply.quiescent.impl.executor :refer [delegate-cpu delegate-scheduled delegate-virtual delegate-sync]]
    [co.multiply.quiescent.impl.gate :as gate]
    [co.multiply.quiescent.impl.race :as race]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.quiescent.type :as type #?@(:cljs [:refer [ICancellable ITask TaskState]])]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [ask scoping current-scope with-scope assoc-scope]])
  #?(:clj (:import
            [co.multiply.quiescent.impl Promise Task]
            [co.multiply.quiescent.impl ICancellable ITask TaskState]
            [java.time Duration]
            [java.util.concurrent CancellationException CompletableFuture Semaphore TimeoutException])))


;; # Thread control
;; ################################################################################
#?(:clj (defn conditionally-wait
          "Sleep for the specified duration, if positive.

           Accepts milliseconds (positive number) or java.time.Duration (positive).
           Zero, negative, nil, and other values are no-ops.

           Avoids unnecessary scheduler interaction when no wait is needed."
          [ms-or-dur]
          (cond
            (and (number? ms-or-dur) (pos? ms-or-dur))
            (Thread/sleep (long ms-or-dur))

            (and (instance? Duration ms-or-dur)
              (not (Duration/.isZero ms-or-dur))
              (not (Duration/.isNegative ms-or-dur)))
            (Thread/sleep ^Duration ms-or-dur)

            :else nil)))


#?(:clj (defn semaphore
          "Creates a semaphore. By default, `fairness` is set to true, which
           means that allocations will be first-in first-out."
          (^Semaphore [n]
           (Semaphore. (int n) true))
          (^Semaphore [n ^Boolean fair]
           (Semaphore. (int n) fair))))


#?(:clj (defn release
          "Release a permit back to the semaphore."
          [sem]
          (Semaphore/.release sem)))


#?(:clj (defn acquire
          "Acquire a permit from the semaphore, blocking until one is available."
          [sem]
          (ml.impl/assert-virtual! (Thread/currentThread))
          (Semaphore/.acquire sem)))


#?(:clj (defmacro with-semaphore
          "Execute body while holding a semaphore permit. Releases on completion or exception."
          [^Semaphore sem & body]
          `(do (acquire ~sem)
             (try ~@body
               (finally
                 (release ~sem))))))


(defn gate
  "Create a gate that limits concurrent task execution to `n` at a time.

   A gate acts like a semaphore: tasks created with [[gate-task]] wait for
   an available permit before running. When a task completes (success, error,
   or cancellation), its permit is released for the next waiting task.

   Gates participate in structured concurrency: cancelling a gate cancels
   all tasks created through it."
  [n]
  (gate/gate n))


(defmacro gate-task
  "Create a task that runs through `gate`, waiting for an available permit.
   The task executes `body` when a permit is acquired.

   Returns immediately with a task that will eventually contain the result of `body`.

   Example:
   ```clojure
   (let [g (gate 2)]  ; max 2 concurrent
     (qfor [url urls]
       (gate-task g
         (fetch url))))
   ```"
  [gate & body]
  `(gate/enqueue ~gate (fn enqueued# [] ~@body)))


#?(:cljs (defn abort-signal
           "Create and return an [AbortSignal](https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal)
            tied to the current task's lifecycle.

            Each invocation creates a new AbortController and returns its signal. The signal
            is aborted when the task settles (completes, fails, or is cancelled).

            Example:

            ```clojure
            (q (js/fetch url #js {:signal (abort-signal)}))
            ;; If the task is cancelled, completes, or fails, the fetch will abort.
            ```

            Throws if called outside a task scope."
           []
           (if-some [task (ask impl/*this* nil)]
             (let [controller (js/AbortController.)]
               (subs/subscribe-callback task sm/phase-settling
                 #(.abort controller))
               (.-signal controller))
             (throw (js/Error. "Abort signal can only be used in task scope.")))))


#?(:clj  (defn interrupted?
           "Check if the current or specified thread is interrupted."
           ([]
            (.isInterrupted (Thread/currentThread)))
           ([^Thread thread]
            (.isInterrupted thread)))
   :cljs (defn aborted?
           "Returns true if the abort signal has been triggered."
           [signal]
           (.-aborted signal)))


#?(:clj  (defn comply-interrupt
           "If interruption has been requested in the current thread,
            throw an `InterruptedException`."
           []
           (when (interrupted?) (throw (InterruptedException.))))
   :cljs (defn comply-abort
           "If abort signal has been triggered, throw."
           [signal]
           (.throwIfAborted signal)))


#?(:clj (defn throw-on-platform-park!
          "Configure whether awaiting from a platform thread throws an exception.
           When true (default), parking a platform thread raises IllegalStateException.
           Set to false for testing or when platform thread parking is intentional."
          [bool]
          (alter-var-root #'ml/*assert-virtual* (constantly (boolean bool)))))


#?(:clj (defn deref-cpu
          "Temporarily bypass the restriction that Tasks can't be parked on platform threads."
          [t]
          (scoping [ml/*assert-virtual* false]
            (deref t))))


;; # Task dereferencing
;; ################################################################################
(defn get-now
  "Get the current value if completed, otherwise return default.
   Non-blocking alternative to deref. Throws if completed exceptionally.

   Analogous to `CompletableFuture.getNow()`. Useful for:
   - Polling task status without blocking
   - Optimistic reads in hot paths
   - Conditional logic based on completion

   Example:

   ```clojure
   (let [result (get-now task :not-ready)]
     (if (= result :not-ready)
       (log/info \"Still waiting...\")
       (process result)))
   ```"
  [t default]
  (call/getNow (type/as-task t) default))


(defn task?
  "Returns true if v is a Task, false otherwise.

   Note: Returns false for Promises. Use `(satisfies? ITask v)` to check for both."
  [v]
  (instance? Task v))


(defn taskable?
  "Returns true if v is an async primitive that Quiescent handles specially.

   True for: Task, Promise, CompletableFuture (CLJ), Future (CLJ), js/Promise (CLJS).
   False for: nil, plain values, collections.

   Note: core.async channels become taskable when the optional
   `co.multiply.quiescent.adapter.core-async` namespace is required."
  [v]
  (type/taskable? v))


(defn exceptional?
  "Returns true if the task has completed with an exception.
   Returns false if task is not completed or completed successfully.

   Analogous to `CompletableFuture.isCompletedExceptionally()`. Useful for:

   - Checking error state without dereferencing
   - Conditional error handling logic
   - Metrics and monitoring

   Example:

   ```clojure
   (when (completed-exceptionally? task)
     (log/error \"Task failed\" {:task task}))
   ```"
  [t]
  (call/isExceptional (type/as-task t)))


(defn cancelled?
  "True if the given task is cancelled."
  [t]
  (call/isCancelled (type/as-task t)))


;; # Task construction
;; ################################################################################
(defmacro q
  "Execute body synchronously and return a task containing the result.

   Runs on the calling thread. If the result contains nested tasks, they are
   awaited in parallel and their values inlined (grounding)."
  [& body]
  `(do-runner delegate-sync ~@body))


(defmacro task
  "Execute body asynchronously on a virtual thread and return a task.

   Returns immediately; body runs in the background. Deref blocks until
   the task settles. Nested tasks in the result are grounded."
  [& body]
  `(do-runner delegate-virtual ~@body))


(defmacro cpu-task
  "Execute body on the platform thread pool and return a task.

   Use for CPU-bound work that shouldn't run on virtual threads. Returns
   immediately; body runs in the background. Nested tasks in the result
   are grounded."
  [& body]
  `(do-runner delegate-cpu ~@body))


(defn as-task
  "Convert an async primitive to a Task.

   Supports CompletableFuture (CLJ), Future (CLJ), js/Promise (CLJS), and
   core.async channels (when adapter is loaded). Tasks and Promises pass
   through unchanged. Plain values are wrapped in an immediately-settled task.

   Use this to bridge external async APIs into Quiescent's task model."
  [v]
  (type/as-task v))


(defn failed-task
  "Create a task that is already completed with an exception.

   Analogous to `CompletableFuture.failedFuture()`. Useful for:
   - Short-circuiting with known errors
   - Testing error handling paths
   - Returning failed tasks from conditional logic

   Example:

   ```clojure
   (if valid?
     (fetch-data)
     (failed-task (ex-info \"Invalid input\" {:code 400})))
   ```"
  [e]
  (doto (-pending-task delegate-sync) (call/doApply nil e)))


(defn ^:no-doc -compel
  [v]
  ;; Attempt to convert to task, ie. only Task, CompletableFuture, etc.
  (let [t (if (type/groundable? v) (type/as-task v) v)]
    (if (task? t)
      ;; If we successfully converted to a Task, proceed
      (let [moat (-pending-task delegate-sync true)]
        ;; Forward direct cancellation from moat to inner task
        (subs/subscribe-callback moat sm/phase-settling
          (fn [^TaskState state]
            (when (.-cancelled state)
              (call/doCancel t "Task cancelled directly."))))
        ;; Moat grounds with the inner task as value.
        (doto moat (call/doApply t)))
      ;; If task conversion was unsuccessful, throw. This includes Promise, which
      ;; must not ever be compelled, or it may become orphaned and become a resource leak.
      (throw (type/illegal-argument-exception (str "Can only compel Tasks, not '" (type v) "'"))))))


(defmacro compel
  "Wrap a task to make it immune to cascading cancellation.

   Returns a compelled wrapper task that observes the inner task. The wrapper:
   - Ignores cascade cancellation from parent contexts
   - Propagates direct cancellation to the inner task
   - Creates a 'moat' that blocks cascade but allows direct cancel

   If the task is created inline, it's also compelled. If it's pre-existing,
   only the wrapper is compelled, but direct cancel on the wrapper still
   propagates to the inner task.

   Throws `IllegalArgumentException` if given a Promise. Promises are externally
   controlled and compelling them would block teardown on something you don't
   control, possibly leading to resource leaks.

   Example:

   ```clojure
   (let [slow-task (compel (s3/PUT large-file))]
     ;; Parent cascade-cancelled -> slow-task ignores it
     ;; Direct cancel on slow-task -> cancels the S3 PUT
     slow-task)
   ```"
  [t]
  `(scoping [impl/*this* nil]
     (-compel ~t)))


;; # Chaining
;; ################################################################################
(defn then
  "Chain a function after one or more tasks complete.

   Single task:
     `(then task f)` - Calls the equivalent of `(f @task)`

   Multiple tasks:
     `(then t1 t2 t3 f)` - Waits for all tasks in parallel, calls the equivalent of `(f @t1 @t2 @t3)`"
  ([t f] (call/doThen (type/as-task t) delegate-virtual f))
  ([t1 t2 f] (do-applier delegate-virtual [t1 t2] nil (partial apply f)))
  ([t1 t2 t3 f] (do-applier delegate-virtual [t1 t2 t3] nil (partial apply f)))
  ([t1 t2 t3 t4 f] (do-applier delegate-virtual [t1 t2 t3 t4] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 f] (do-applier delegate-virtual [t1 t2 t3 t4 t5] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 f] (do-applier delegate-virtual [t1 t2 t3 t4 t5 t6] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 f] (do-applier delegate-virtual [t1 t2 t3 t4 t5 t6 t7] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 f] (do-applier delegate-virtual [t1 t2 t3 t4 t5 t6 t7 t8] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 f] (do-applier delegate-virtual [t1 t2 t3 t4 t5 t6 t7 t8 t9] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 f] (do-applier delegate-virtual [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 & more]
   (do-applier delegate-virtual (into [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11] (butlast more)) nil (partial apply (last more)))))


(defn then-cpu
  "Chain a function after one or more tasks complete.

   Executes on the platform thread pool.

   Single task:
     `(then task f)` - Calls the equivalent of `(f @task)`

   Multiple tasks:
     `(then t1 t2 t3 f)` - Waits for all tasks in parallel, calls the equivalent of `(f @t1 @t2 @t3)`"
  ([t f] (call/doThen (type/as-task t) delegate-cpu f))
  ([t1 t2 f] (do-applier delegate-cpu [t1 t2] nil (partial apply f)))
  ([t1 t2 t3 f] (do-applier delegate-cpu [t1 t2 t3] nil (partial apply f)))
  ([t1 t2 t3 t4 f] (do-applier delegate-cpu [t1 t2 t3 t4] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 f] (do-applier delegate-cpu [t1 t2 t3 t4 t5] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 f] (do-applier delegate-cpu [t1 t2 t3 t4 t5 t6] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 f] (do-applier delegate-cpu [t1 t2 t3 t4 t5 t6 t7] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 f] (do-applier delegate-cpu [t1 t2 t3 t4 t5 t6 t7 t8] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 f] (do-applier delegate-cpu [t1 t2 t3 t4 t5 t6 t7 t8 t9] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 f] (do-applier delegate-cpu [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 & more]
   (do-applier delegate-cpu (into [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11] (butlast more)) nil (partial apply (last more)))))


(defn catch
  "Handle errors from a task with a recovery function.

   Arities:
     `(catch task f)` - Handle any exception
     `(catch task Type f)` - Handle specific exception type (CLJ) or matching predicate (CLJS)
     `(catch task Type1 f1 Type2 f2 ...)` - Handle multiple types/predicates (first match wins)

   In CLJ, pass exception classes (e.g., `IllegalArgumentException`).
   In CLJS, pass predicates (e.g., `#(= :bad-arg (:type (ex-data %)))`).

   The recovery function receives the exception and can:
   - Return a fallback value (task succeeds with that value)
   - Throw a new exception (task fails with new error)

   If the task succeeds, the handler is not called and the value passes through.
   Cancellation always propagates through catch without invoking handlers.

   Multi-pair catch provides exclusive-or semantics like try/catch: only one handler
   runs, and if that handler throws, the exception propagates (not caught by later
   pairs in the same catch). Use chained catch calls for nesting semantics where
   a handler's exception can be caught downstream.

   Examples:

   ```clojure
   ;; Catch all exceptions
   (catch task
     (fn [e]
       (log/warn \"Task failed\" e)
       :default-value))

   ;; Catch specific type
   (catch task TimeoutException
     (fn [e] :timed-out))

   ;; Multiple types (exclusive-or, like try/catch)
   (catch task
     IllegalArgumentException (fn [e] :bad-arg)
     IOException (fn [e] :io-error)
     Throwable (fn [e] :other))
   ```"
  ([t f]
   (call/doCatch (type/as-task t) delegate-virtual f))
  ([t e f]
   (call/doCatchTyped (type/as-task t) delegate-virtual [e f]))
  ([t e1 f1 e2 f2]
   (call/doCatchTyped (type/as-task t) delegate-virtual [e1 f1 e2 f2]))
  ([t e1 f1 e2 f2 e3 f3]
   (call/doCatchTyped (type/as-task t) delegate-virtual [e1 f1 e2 f2 e3 f3]))
  ([t e1 f1 e2 f2 e3 f3 e4 f4]
   (call/doCatchTyped (type/as-task t) delegate-virtual [e1 f1 e2 f2 e3 f3 e4 f4]))
  ([t e1 f1 e2 f2 e3 f3 e4 f4 e5 f5]
   (call/doCatchTyped (type/as-task t) delegate-virtual [e1 f1 e2 f2 e3 f3 e4 f4 e5 f5]))
  ([t e1 f1 e2 f2 e3 f3 e4 f4 e5 f5 & pairs]
   (call/doCatchTyped (type/as-task t) delegate-virtual (into [e1 f1 e2 f2 e3 f3 e4 f4 e5 f5] pairs))))


(defn catch-cpu
  "Handle errors from a task with a recovery function.

   Executes on the platform thread pool. CLJ only.

   Arities:
     `(catch-cpu task f)` - Handle any Throwable
     `(catch-cpu task Type f)` - Handle specific exception type
     `(catch-cpu task Type1 f1 Type2 f2 ...)` - Handle multiple types (first match wins)

   The recovery function receives the exception and can:
   - Return a fallback value (task succeeds with that value)
   - Throw a new exception (task fails with new error)

   If the task succeeds, the handler is not called and the value passes through.
   Cancellation always propagates through catch without invoking handlers.

   Multi-pair catch provides exclusive-or semantics like try/catch: only one handler
   runs, and if that handler throws, the exception propagates (not caught by later
   pairs in the same catch). Use chained catch calls for nesting semantics where
   a handler's exception can be caught downstream.

   Examples:

   ```clojure
   ;; Catch all exceptions
   (catch-cpu task
     (fn [e]
       (log/warn \"Task failed\" e)
       :default-value))

   ;; Catch specific type
   (catch-cpu task TimeoutException
     (fn [e] :timed-out))

   ;; Multiple types (exclusive-or, like try/catch)
   (catch-cpu task
     IllegalArgumentException (fn [e] :bad-arg)
     IOException (fn [e] :io-error)
     Throwable (fn [e] :other))
   ```"
  ([t f]
   (call/doCatch (type/as-task t) delegate-cpu f))
  ([t e f]
   (call/doCatchTyped (type/as-task t) delegate-cpu [e f]))
  ([t e1 f1 e2 f2]
   (call/doCatchTyped (type/as-task t) delegate-cpu [e1 f1 e2 f2]))
  ([t e1 f1 e2 f2 e3 f3]
   (call/doCatchTyped (type/as-task t) delegate-cpu [e1 f1 e2 f2 e3 f3]))
  ([t e1 f1 e2 f2 e3 f3 e4 f4]
   (call/doCatchTyped (type/as-task t) delegate-cpu [e1 f1 e2 f2 e3 f3 e4 f4]))
  ([t e1 f1 e2 f2 e3 f3 e4 f4 e5 f5]
   (call/doCatchTyped (type/as-task t) delegate-cpu [e1 f1 e2 f2 e3 f3 e4 f4 e5 f5]))
  ([t e1 f1 e2 f2 e3 f3 e4 f4 e5 f5 & pairs]
   (call/doCatchTyped (type/as-task t) delegate-cpu (into [e1 f1 e2 f2 e3 f3 e4 f4 e5 f5] pairs))))


(defn handle
  "Handle both success and error cases with a single function.

   The function receives `[value error]`:
   - On success: `[value nil]` (value may be nil)
   - On error: `[nil exception]`

   Returns the result of calling the function.

   This is more fundamental than `done` - use when you need to return
   a different value based on success/error.

   **Cancellation**: Does NOT run when task is cancelled. Use `finally` for
   cleanup that must run on cancellation.

   Example:

   ```clojure
   (handle task
     (fn [v e]
       (if e
         (log-and-return-default e)
         (process v))))
   ```"
  [t f]
  (call/doHandle (type/as-task t) delegate-virtual f))


(defn handle-cpu
  "Handle both success and error cases with a single function.

   Executes on the platform thread pool.

   The function receives `[value error]`:
   - On success: `[value nil]` (value may be nil)
   - On error: `[nil exception]`

   Returns the result of calling the function.

   This is more fundamental than `done` - use when you need to return
   a different value based on success/error.

   **Cancellation**: Does NOT run when task is cancelled. Use `finally` for
   cleanup that must run on cancellation.

   Example:

   ```clojure
   (handle-cpu task
     (fn [v e]
       (if e
         (log-and-return-default e)
         (process v))))
   ```"
  [t f]
  (call/doHandle (type/as-task t) delegate-cpu f))


(defn ok
  "Run a side-effecting function after a task completes successfully.

   Calls `(f value)` when the task succeeds.

   The function is for side effects:
   - Return value is ignored (original task value passes through)
   - If `f` throws, that exception fails the task chain
   - Errors and cancellation skip calling the function

   Use for logging, metrics, or triggering downstream effects on success."
  [t f]
  (call/doOk (type/as-task t) delegate-virtual f))


(defn ok-cpu
  "Run a side-effecting function after a task completes successfully.

   Executes on the platform thread pool.

   Calls `(f value)` when the task succeeds.

   The function is for side effects:
   - Return value is ignored (original task value passes through)
   - If `f` throws, that exception fails the task chain
   - Errors and cancellation skip calling the function

   Use for logging, metrics, or triggering downstream effects on success."
  [t f]
  (call/doOk (type/as-task t) delegate-cpu f))


(defn err
  "Run a side-effecting function when a task fails. Mirror of `ok`.

   Calls `(f exception)` when the task completes with an error.
   Successful tasks pass through unchanged without calling `f`.

   The function is for side effects:
   - Return value is ignored (original exception passes through)
   - If `f` throws, that exception replaces the original

   **Cancellation**: Does NOT run when task is cancelled. Cancellation is a control
   signal, not an error. Use `finally` for cleanup that must run on cancellation."
  [t f]
  (call/doErr (type/as-task t) delegate-virtual f))


(defn err-cpu
  "Run a side-effecting function when a task fails. Mirror of `ok`.

   Executes on the platform thread pool.

   Calls `(f exception)` when the task completes with an error.
   Successful tasks pass through unchanged without calling `f`.

   The function is for side effects:
   - Return value is ignored (original exception passes through)
   - If `f` throws, that exception replaces the original

   **Cancellation**: Does NOT run when task is cancelled. Cancellation is a control
   signal, not an error. Use `finally` for cleanup that must run on cancellation."
  [t f]
  (call/doErr (type/as-task t) delegate-cpu f))


(defn done
  "Run a side-effecting function when a task completes with success or error.

   The function receives `[value error]`:
   - On success: `[value nil]` (value may be nil)
   - On error: `[nil exception]`

   The return value is ignored - the original task result passes through unchanged.
   If `f` throws, that exception fails the task chain.

   This is the side-effecting counterpart to `handle`:
   - `handle`: runs on success/error, returns transformed value
   - `done`: runs on success/error, passes through original result

   **Cancellation**: Does NOT run when task is cancelled. Use `finally` for
   cleanup that must run on cancellation."
  [t f]
  (call/doDone (type/as-task t) delegate-virtual f))


(defn done-cpu
  "Run a side-effecting function when a task completes with success or error.

   Executes on the platform thread pool.

   The function receives `[value error]`:
   - On success: `[value nil]` (value may be nil)
   - On error: `[nil exception]`

   The return value is ignored - the original task result passes through unchanged.
   If `f` throws, that exception fails the task chain.

   This is the side-effecting counterpart to `handle`:
   - `handle`: runs on success/error, returns transformed value
   - `done`: runs on success/error, passes through original result

   **Cancellation**: Does NOT run when task is cancelled. Use `finally` for
   cleanup that must run on cancellation."
  [t f]
  (call/doDone (type/as-task t) delegate-cpu f))


(defn finally
  "Run a function after a task completes, regardless of outcome.

   The function receives `[value error cancelled]`:
   - On success: `[value nil false]` (value may be nil)
   - On error: `[nil exception false]`
   - On cancellation: `[nil CancellationException true]`

   The return value is ignored - the original task result passes through unchanged.
   Exceptions from the function propagate and fail the task chain.

   **Cancellation**: `finally` is the ONLY handler guaranteed to run when a task is
   cancelled. All other handlers (`ok`, `err`, `done`, `catch`, `handle`, `then`) skip
   execution on cancellation. Use `finally` when you need code to run regardless
   of how the task ends—especially for resource cleanup.

   Example:

   ```clojure
   (finally task
     (fn [v e cancelled]
       (release-resource)
       (when cancelled
         (log/info \"Task was cancelled\"))))
   ```"
  [t f]
  (call/doFinally (type/as-task t) delegate-virtual f))


(defn finally-cpu
  "Run a function after a task completes, regardless of outcome.

   Executes on the platform thread pool.

   The function receives `[value error cancelled]`:
   - On success: `[value nil false]` (value may be nil)
   - On error: `[nil exception false]`
   - On cancellation: `[nil CancellationException true]`

   The return value is ignored - the original task result passes through unchanged.
   Exceptions from the function propagate and fail the task chain.

   **Cancellation**: `finally` is the ONLY handler guaranteed to run when a task is
   cancelled. All other handlers (`ok`, `err`, `done`, `catch`, `handle`, `then`) skip
   execution on cancellation. Use `finally` when you need code to run regardless
   of how the task ends—especially for resource cleanup.

   Example:

   ```clojure
   (finally-cpu task
     (fn [v e cancelled]
       (release-resource)
       (when cancelled
         (log/info \"Task was cancelled\"))))
   ```"
  [t f]
  (call/doFinally (type/as-task t) delegate-cpu f))


;; # Coordination
;; ################################################################################
(defn cancel
  "Attempt to cancel a task, promise, or gate. Returns a `Task[Boolean]`.

   The returned task settles with:
   - `true`: This cancel call won the race and successfully cancelled the target
   - `false`: The target was already settled, or another cancel call won first

   The returned task settles when the target reaches quiescent phase,
   regardless of the boolean result. This enables coordination code to wait
   for complete teardown before proceeding.

   Cancellation propagates to chained tasks as cancellation (not failure),
   meaning `cancelled?` will return true and only `finally` handlers run.

   Usage:

   ```clojure
   (cancel task)   ; Fire-and-forget: attempt cancellation without blocking
   @(cancel task)  ; Wait for target to reach quiescent, returns boolean
   ```"
  [t]
  (call/doCancelDirect
    (cond
      (#?(:clj instance? :cljs cljs.core/satisfies?) ICancellable t)
      t

      (type/taskable? t)
      (type/as-task t)

      :else
      (throw (ex-info "Cannot cancel: target is not cancellable" {:target t})))))


(defn race
  "Race multiple tasks, returning the first successful result.

   Returns a Task that completes with the value of whichever task settles first
   with a non-exceptional result. Losing tasks are cancelled (unless compelled).

   If all tasks fail, returns an exception. If one task fails, returns the
   combined errors as ex-info with :errors key."
  [& tasks]
  (race/race tasks))


(defn race-stateful
  "Race tasks that produce stateful resources, with cleanup for losers.

   Like `race`, but handles the edge case where multiple tasks complete
   simultaneously. When two tasks both produce a value before a winner is
   determined, one wins and the other's value is passed to `release`.

   Args:
     - `release` Function called with each orphaned value (realized but lost).
                 Only called for non-nil results.
     - `tasks`   Tasks to race

   Example:

   ```clojure
   (race-stateful #(.close %) alloc-a alloc-b alloc-c)
   ```

   Returns a Task that completes with the first successful result.
   Losing tasks are cancelled. If all tasks fail, returns the combined errors."
  [release & tasks]
  (race/race tasks {:release release}))


(defmacro qfor
  "Map over a collection eagerly, executing body for each element.

   Returns a Task containing a vector of results. If the body returns tasks,
   they execute in parallel and are awaited concurrently.

   Does not automatically wrap the body in a task.

   Unlike `for`, does not support `:let`, `:when`, or `:while` modifiers.

   Example:

   ```clojure
   @(qfor [id user-ids]
      (fetch-user id))
   ;; => [{:id 1 ...} {:id 2 ...} ...]
   ```"
  [[bind-to coll] & body]
  `(co.multiply.quiescent/q (mapv (fn per-item# [~bind-to] ~@body) ~coll)))


(def ^:private plain-merge
  (comp type/plain (partial apply merge)))


(defn qmerge
  "Takes a map where the values may be tasks. If multiple maps are given, merges them.

   Returns a Task containing a map with all nested tasks resolved to their values.

   Prefer this over `(then task-a task-b merge)` - it skips the grounding phase
   since the result is known to contain only resolved values."
  ([] (type/as-task {}))
  ([m] (type/as-task m))
  ([m1 m2] (do-applier delegate-sync [m1 m2] nil plain-merge))
  ([m1 m2 m3] (do-applier delegate-sync [m1 m2 m3] nil plain-merge))
  ([m1 m2 m3 m4] (do-applier delegate-sync [m1 m2 m3 m4] nil plain-merge))
  ([m1 m2 m3 m4 m5] (do-applier delegate-sync [m1 m2 m3 m4 m5] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6] (do-applier delegate-sync [m1 m2 m3 m4 m5 m6] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7] (do-applier delegate-sync [m1 m2 m3 m4 m5 m6 m7] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8] (do-applier delegate-sync [m1 m2 m3 m4 m5 m6 m7 m8] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8 m9] (do-applier delegate-sync [m1 m2 m3 m4 m5 m6 m7 m8 m9] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8 m9 m10] (do-applier delegate-sync [m1 m2 m3 m4 m5 m6 m7 m8 m9 m10] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8 m9 m10 & ms]
   (do-applier delegate-sync (into [m1 m2 m3 m4 m5 m6 m7 m8 m9 m10] ms) nil plain-merge)))


(def ^:private plain-last
  (comp type/plain
    (fn [v]
      (let [c (type/vecCount v)]
        (when-not (zero? c)
          (type/vecNth v (dec c)))))))


(defn qdo
  "Await all tasks, but only return the output of the last one.

   If one task throws, all are cancelled."
  ([] (type/as-task nil))
  ([t] (type/as-task t))
  ([t1 t2] (do-applier delegate-sync [t1 t2] nil plain-last))
  ([t1 t2 t3] (do-applier delegate-sync [t1 t2 t3] nil plain-last))
  ([t1 t2 t3 t4] (do-applier delegate-sync [t1 t2 t3 t4] nil plain-last))
  ([t1 t2 t3 t4 t5] (do-applier delegate-sync [t1 t2 t3 t4 t5] nil plain-last))
  ([t1 t2 t3 t4 t5 t6] (do-applier delegate-sync [t1 t2 t3 t4 t5 t6] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7] (do-applier delegate-sync [t1 t2 t3 t4 t5 t6 t7] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8] (do-applier delegate-sync [t1 t2 t3 t4 t5 t6 t7 t8] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9] (do-applier delegate-sync [t1 t2 t3 t4 t5 t6 t7 t8 t9] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] (do-applier delegate-sync [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 & ts]
   (do-applier delegate-sync (into [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] ts) nil plain-last)))


;; # Timing and lifecycle
;; ################################################################################
(defn sleep
  "Create a task that sleeps for the specified duration, then returns `default`.

   Args:
     - `ms-or-duration` Sleep duration as either:
       - Long: non-negative milliseconds (0 = immediate return)
       - `java.time.Duration`: non-negative duration
     - `default` Optional value to return after sleep (default: nil)
       - If a function, calls it and returns its result
       - If a Throwable, fails the task with that exception

   Returns a task that completes after the specified duration.
   Throws for negative or unsupported duration values.

   Example:

   ```clojure
   @(sleep 100)                       ; Sleep 100ms, return nil
   @(sleep 100 :done)                 ; Sleep 100ms, return :done
   @(sleep 100 #(rand-int 10))        ; Sleep 100ms, then call function
   @(sleep (Duration/ofSeconds 1))    ; Sleep 1 second, return nil
   @(sleep 100 (Exception. \"Boom\")) ; Sleep 100ms, then throw
   ```"
  ([ms-or-duration]
   (sleep ms-or-duration nil))
  ([ms-or-duration default]
   (let [task (-pending-task delegate-virtual)]
     (doto task
       (subs/subscribe-cancel-exec sm/phase-settling
         (delegate-scheduled ms-or-duration
           (fn runner
             []
             (cond
               (fn? default) (call/doRun task default)
               (instance? #?(:clj Throwable :cljs js/Error) default) (call/doApply task nil default)
               :else (call/doApply task default)))))))))


(defn timeout
  "Asynchronous timeout that races a task against a sleep timer.

   Roughly equivalent to `(deref task ms-or-dur default)`, except it's asynchronous
   and doesn't block the calling thread unless dereferenced.

   Args:
     - `t` Task to race against timeout
     - `ms-or-dur` Timeout duration as either:
       - Long: milliseconds
       - `java.time.Duration`: duration object
     - `default` Optional value/behavior on timeout:
       - Value: returned on timeout
       - Function: executed and its result returned
       - Exception: thrown on timeout
       - Omitted: throws `TimeoutException`

   Returns a task that completes with either the task's result or the timeout `default`.

   Example:

   ```clojure
   @(timeout my-task 1000)                   ; Throw TimeoutException after 1s
   @(timeout my-task 1000 :timed-out)        ; Return :timed-out after 1s
   @(timeout my-task 1000 #(rand-int 10))    ; Call fallback fn on timeout
   @(timeout my-task (Duration/ofSeconds 5)) ; Throw after 5 seconds
   ```"
  ([t ms-or-dur]
   (timeout t ms-or-dur (type/timeout-exception (str "Task timed out after " ms-or-dur "."))))
  ([t ms-or-dur default]
   (race/race [t (sleep ms-or-dur ::timeout)]
     {:tf (fn handle-default
            [res]
            (if (= ::timeout res)
              (cond
                (fn? default) (default)
                (instance? #?(:clj Throwable :cljs js/Error) default) (throw default)
                :else default)
              res))})))


(defn monitor
  "Non-destructive monitoring wrapper that observes a task without affecting its outcome.

   Unlike `timeout`, this does NOT race the task or change its result. Instead, it runs
   a side-effect function if the task hasn't completed within the specified duration.
   The original task continues running and its result is returned unchanged.

   This is useful for diagnostics: log warnings when operations are slow without
   actually timing them out or affecting their execution.

   Args:
     - `t` Task to monitor
     - `ms-or-dur` Duration after which to trigger side effect:
       - Long: milliseconds
       - `java.time.Duration`: duration object
     - `side-effect-fn` Function (or value) passed to timeout's default parameter.
       If a function, it's called when the timeout fires.
       Exceptions from the side effect propagate and fail the task.

   Returns the original task unchanged - result and timing are unaffected.

   Example:

   ```clojure
   (-> (slow-operation)
     (monitor 5000
       #(log/warn \"Operation exceeded 5s\")))
   ```"
  [t ms-or-dur side-effect-fn]
  (qdo (timeout (co.multiply.quiescent/compel t) ms-or-dur side-effect-fn)
    ;; Return `t` uncompelled to allow outside cancellations to cascade.
    t))


(defn time
  "Measures the duration of a task and reports it via a side-effect function.

   The side-effect function receives four arguments: the task's value (or nil),
   exception (or nil), cancelled flag, and elapsed milliseconds as a number.

   By default, timing starts when `time` is called. To include task construction
   time in the measurement, capture the current time beforehand and pass it as
   `start-ms`.

   Args:
     - `t` Task to measure
     - `start-ms` Optional starting time in ms (default: current time at call)
     - `side-effect-fn` Function called with `[value exception cancelled ms]`

   Returns the task unchanged.

   Example:

   ```clojure
   ;; Basic usage - timing starts when `time` is attached
   (-> (fetch-user id)
     (time (fn [v e c ms]
             (log/info \"fetch-user took\" ms \"ms\"))))

   ;; Include construction time
   (let [start (System/currentTimeMillis)]  ; or js/performance.now in CLJS
     (-> (fetch-user id)
       (time start
         (fn [v e c ms]
           (log/info \"Total time:\" ms \"ms\")))))
   ```"
  ([t side-effect-fn]
   (time t #?(:clj (System/currentTimeMillis) :cljs (js/performance.now)) side-effect-fn))
  ([t start side-effect-fn]
   (finally t
            (fn [v e c]
              (side-effect-fn v e c
                #?(:clj  (- (System/currentTimeMillis) start)
                   :cljs (- (js/performance.now) start)))))))


(defn- default-validate
  [v e]
  (if (instance? #?(:clj Throwable :cljs js/Error) e)
    (throw e)
    v))


(defn retry
  "Takes a function that returns a task. Tries to rerun that task until it succeeds
   or runs out of retries, according to the options given.

   `f` is a function that takes one argument: a bool that is `false` for the first
   attempt, and `true` for any subsequent attempts.

   Fails immediately without retries if `f` throws (i.e., if task construction fails).

   If `validate` is given, it will be run on the result with `[value exception]`, and
   retries will issue depending on if `validate` throws. If `validate` does not throw,
   its return value is considered viable."
  ([f]
   (retry f nil))
  ([f {:keys [retries backoff-ms backoff-factor retry-callback validate]
       :or   {retries        3
              backoff-ms     2000
              backoff-factor 2
              validate       default-validate
              retry-callback (constantly nil)}
       :as   args}]
   (-> args (get ::retrying) (true?) (f)
     (type/as-task)
     (handle validate)
     (catch
       (fn [e]
         (if (zero? retries)
           ;; Rethrow error if there are no more retries.
           (throw e)
           ;; Recursive call. Grounds into `catch` (yielding the `catch` thread).
           (sleep backoff-ms
             (fn []
               (qdo
                 (retry-callback e retries backoff-ms)
                 (retry f
                   {::retrying      true
                    :retries        (dec retries)
                    :backoff-ms     (long (* backoff-ms backoff-factor))
                    :backoff-factor backoff-factor
                    :validate       validate
                    :retry-callback retry-callback}))))))))))


(def ^:no-doc constantly-false (constantly false))


(defn await
  "Re until a task has settled (has a result available).

   Returns true when settled, false if timeout expires.
   Without timeout, blocks indefinitely.

   Unlike `deref`, does not throw if the task failed or was cancelled.

   Args:
     - `t`         The task to await
     - `ms-or-dur` Optional timeout as milliseconds (long) or `java.time.Duration`

   Example:

   ```clojure
   (await task)                            ; Block until settled
   (await task 1000)                       ; With 1 second timeout
   (await task (Duration/ofSeconds 5))     ; With Duration timeout
   ```"
  ([t]
   (call/newQuiescenceProxy (type/as-task t) true))
  ([t ms-or-dur]
   (timeout (await t) ms-or-dur constantly-false)))


;; # Async let
;; ################################################################################
(defn- expr->symbols
  [expr]
  (set (p/find-when expr symbol? {:include-keys true})))


(def ^:private prepare-extraction-xf
  (comp
    (partition-all 2)
    (map-indexed (fn structure
                   [idx [from to]]
                   {:idx      (gensym (str "form-" idx))
                    :from     from
                    :provides (expr->symbols from)
                    :to       to
                    :requires (expr->symbols to)}))))


(defn- perform-extraction
  ([] {:provided-by (sorted-map)
       :forms       (sorted-map)
       :depends     (sorted-map)
       :bind-order  []
       :body-idx    nil})
  ([m] m)
  ([{:keys [provided-by forms depends bind-order body-idx]} {:keys [idx from provides requires] :as form}]
   (let [valid-provides (disj provides '&)
         valid-requires (into #{} (filter provided-by) requires)]
     {:provided-by (reduce (fn [m sym]
                             (assoc m sym idx))
                     provided-by
                     valid-provides)
      :depends     (assoc depends idx
                     (-> (select-keys provided-by valid-requires)
                       (vals)
                       (set)))
      :bind-order  (conj bind-order idx)
      :body-idx    (or body-idx (when (= ::body from) idx))
      :forms       (assoc forms idx (assoc form :requires valid-requires :provides valid-provides))})))


(defn ^:no-doc build-dependency-graph
  "Build dependency graph from let bindings and body.

   Args:
     let-form - Complete let form: (let [bindings...] body...)

   Returns:
     Map with :forms, :depends, :body-idx, :provided-by"
  [bindings-form]
  (transduce prepare-extraction-xf perform-extraction bindings-form))


(defn ^:no-doc generate-task-let
  "Generate code for qlet from dependency analysis.

   Takes the dependency graph and generates a let expression where:
   - Forms with no dependencies use their expression directly
   - Forms with dependencies generate (q/then deps... (fn [bindings...] expr))
   - Returns the body task (not dereferenced)

   Args:
     analysis - Result from build-dependency-graph

   Returns:
     Generated code form"
  [{:keys [forms depends bind-order body-idx]}]
  (let [;; Create lookup for sorting by original binding order
        order-lookup      (zipmap bind-order (range))
        sort-by-order     (fn [deps] (sort-by order-lookup deps))

        ;; Use original binding order (already valid dependency order)
        all-form-ids      bind-order
        binding-form-ids  (filterv (partial not= body-idx) all-form-ids)

        ;; Generate bindings for all non-body forms
        bindings          (into []
                            (mapcat
                              (fn [form-id]
                                (let [to   (get-in forms [form-id :to])
                                      deps (get depends form-id)]
                                  (if (empty? deps)
                                    ;; No dependencies - use expression directly (no task wrapping)
                                    [form-id to]
                                    ;; Has dependencies - generate t/then with deps sorted by original order
                                    (let [sorted-deps  (vec (sort-by-order deps))
                                          dep-bindings (mapv #(get-in forms [% :from]) sorted-deps)
                                          temp-params  (mapv (fn [_] (gensym "param")) sorted-deps)]
                                      [form-id
                                       `(then ~@sorted-deps
                                          (fn [~@temp-params]
                                            (let [~@(interleave dep-bindings temp-params)]
                                              ~to)))])))))
                            binding-form-ids)

        ;; Generate body
        body-deps         (vec (sort-by-order (get depends body-idx)))
        body-dep-bindings (mapv #(get-in forms [% :from]) body-deps)
        body-temp-params  (mapv (fn [_] (gensym "param")) body-deps)
        body-expr         (get-in forms [body-idx :to])]

    ;; Return complete let expression that evaluates to the body task
    `(task
       (let [~@bindings]
         ~(if (empty? body-deps)
            ;; Body has no deps - just wrap in q
            `(q ~@body-expr)
            ;; Body depends on forms - generate final q/then
            `(then ~@body-deps
               (fn [~@body-temp-params]
                 (let [~@(interleave body-dep-bindings body-temp-params)]
                   ~@body-expr))))))))


(defmacro qlet
  "Async let with automatic dependency analysis and parallel execution.

   Like `let`, but analyzes dependencies between bindings and executes
   independent bindings in parallel using Quiescent.

   Returns a Task (not dereferenced) - use `@` to get the value or chain with `q/then`.

   Example:

   ```clojure
   (q/qlet [user (fetch-user id)          ; Starts immediately
            posts (fetch-posts user-id)   ; Parallel with user fetch
            profile (process-user user)   ; Waits for user
            result (combine profile posts)] ; Waits for both
     result)
   ```

   Bindings are NOT wrapped in tasks automatically - use `q/task` explicitly if needed:

   ```clojure
   (q/qlet [data {:a 1 :b 2}           ; Plain value, no thread
            slow (q/task (slow-fn))]   ; Explicit task
     ...)
   ```"
  [bindings & body]
  (-> (conj bindings ::body body)
    build-dependency-graph
    generate-task-let))


(defmacro if-qlet
  "Async if-let for tasks. Awaits test expression, and if truthy, binds its value to form.

   Like `if-let`, but the test expression can be a task. Awaits the test result,
   and if truthy, binds that value to the binding form and evaluates the then clause.
   Otherwise evaluates the else clause.

   Returns a task.

   Syntax: `(if-qlet [binding-form test-expr] then-expr else-expr)`

   Example:

   ```clojure
   (if-qlet [user (fetch-user id)]
     (process-user user)    ; Executes if user is truthy
     (handle-not-found))    ; Executes if user is nil/false
   ```"
  [bindings pos-case neg-case]
  (assert (vector? bindings) "Must receive a binding vector.")
  (assert (= 2 (count bindings)) "Must have exactly 2 forms in binding vector.")
  `(then ~(bindings 1)
     (fn [test#]
       (if-let [~(bindings 0) test#]
         ~pos-case
         ~neg-case))))


(defmacro when-qlet
  "Async when-let for tasks. Awaits test expression, and if truthy, binds its value to form.

   Like `when-let`, but the test expression can be a task. Awaits the test result,
   and if truthy, binds that value to the binding form and evaluates the body expressions.
   Returns nil if the test is falsy.

   Returns a task.

   Syntax: `(when-qlet [binding-form test-expr] body-expr*)`

   Example:

   ```clojure
   (when-qlet [user (fetch-user id)]
     (log/info \"Processing user\" user)
     (process-user user))   ; Executes if user is truthy, returns nil otherwise
   ```"
  [bindings & body]
  (assert (vector? bindings) "Must receive a binding vector.")
  (assert (= 2 (count bindings)) "Must have exactly 2 forms in binding vector.")
  `(then ~(bindings 1)
     (fn [test#]
       (when-let [~(bindings 0) test#]
         ~@body))))


;; # Promise construction
;; ################################################################################
(defn promise
  "Create a Promise with externally controlled resolution.

   A Promise is functionally identical to a Task (implements `ITask`),
   but unlike `task` which executes immediately, a Promise's resolution is controlled
   externally via `deliver`, `fail`, or `cancel`. In both Clojure and ClojureScript,
   the promise result can be set by executing the promise as a function of a value.

   Chaining operations work as with tasks. Promises can be cancelled, which
   propagates cancellation to chained tasks.

   If a Task is set as the promise value, the task is resolved, and the value that
   it contains is set as the value of the promise.

   ```clojure
   (def p (q/promise))
   (deliver p :result)  ; Delivers :result to the promise
   (p :result)          ; also works
   (cancel p)           ; cancels the promise if not yet settled
   ```

   Example:

   ```clojure
   (let [p (q/promise)]
     (future (Thread/sleep 1000) (p :done))
     (-> p
       (then (fn [v] (str \"Got: \" v)))
       (ok println)))  ; Prints \"Got: done\" after 1 second
   ```"
  []
  (Promise. (-pending-task delegate-sync)))


(defn promise?
  "Returns true if p is a Promise, false otherwise."
  [p]
  (instance? Promise p))


(defn fail
  "Complete a promise with an exception.

   Primarily useful for completing promises when bridging callback-based APIs
   that have separate success/error paths.

   Example:

   ```clojure
   (let [p (promise)]
     (some-callback-api
       {:on-success (fn [result] (p result))
        :on-error   (fn [error] (fail p error))})
     p)
   ```"
  [p e]
  (doto p (call/doApply nil e)))


;; # Task conversion
;; ################################################################################
#?(:clj  (defn as-cf
           "Convert a task to a CompletableFuture.

            The returned future completes when the task settles, with the same
            value, exception, or cancellation status.

            Structured concurrency boundary: CompletableFutures do not participate
            in Quiescent's structured concurrency. Continuations attached to the
            returned CF (via .thenApply, .thenCompose, etc.) run detached from any
            task lineage - effectively at root level. Tasks created within those
            continuations will start their own independent structured concurrency
            trees."
           [t]
           (let [cf    (CompletableFuture.)
                 scope (assoc-scope (current-scope) impl/*this* nil)]
             (subs/subscribe-callback (type/as-task t) sm/phase-settling
               (fn [^TaskState state]
                 (with-scope scope
                   (cond
                     (.-cancelled state)
                     (CompletableFuture/.cancel cf true)

                     (.-exceptional state)
                     (CompletableFuture/.completeExceptionally cf (.-result state))

                     :else
                     (CompletableFuture/.complete cf (.-result state))))))
             cf))
   :cljs (defn as-jsp
           "Convert a task to a JavaScript Promise.

            The returned promise resolves when the task settles, with the same
            value or rejection. Note: JS Promises cannot represent cancellation
            distinctly - a cancelled task will reject with the cancellation error.

            Structured concurrency boundary: JavaScript Promises do not participate
            in Quiescent's structured concurrency. Continuations attached to the
            returned Promise (via .then, .catch, etc.) run detached from any task
            lineage - effectively at root level. Tasks created within those
            continuations will start their own independent structured concurrency
            trees."
           [t]
           (let [scope (assoc-scope (current-scope) impl/*this* nil)]
             (js/Promise.
               (fn [resolve reject]
                 (subs/subscribe-callback (type/as-task t) sm/phase-settling
                   (fn [^TaskState state]
                     ;; Likely redundant since continuations are put on the microtask queue,
                     ;; but kept for safety.
                     (with-scope scope
                       (if (.-exceptional state)
                         (reject (.-result state))
                         (resolve (.-result state)))))))))))
