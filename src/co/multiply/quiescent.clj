(ns co.multiply.quiescent
  "Composable async tasks with automatic parallelization and grounding."
  (:refer-clojure :exclude [await promise])
  (:require
    [clojure.core]
    [co.multiply.machine-latch :as ml]
    [co.multiply.pathling :as p]
    [co.multiply.quiescent.impl :as impl :refer [-pending-task do-applier do-runner]]
    [co.multiply.scoped :refer [scoping]])
  (:import
    [co.multiply.quiescent.impl ITask Promise Task TaskState]
    [java.time Duration]
    [java.util.concurrent CancellationException CompletableFuture ExecutorService Executors ScheduledExecutorService Semaphore ThreadFactory TimeUnit TimeoutException]
    [java.util.concurrent.atomic AtomicLong]))


;; # Executors
;; ################################################################################
(defonce ^{:doc    "Virtual thread executor for IO-bound tasks. Creates a new virtual thread per task.
                 Default executor for tasks - use for network calls, file IO, and blocking operations."
           :no-doc true}
  ^ExecutorService virtual-executor impl/virtual-executor)


(defonce ^{:doc    "Fixed thread pool for CPU-bound tasks. Use for compute-intensive work
                 that should not block virtual threads. Pool size scales with available CPUs."
           :no-doc true}
  ^ExecutorService cpu-executor
  (let [counter   (AtomicLong. 0)
        n-cpus    (.. Runtime getRuntime availableProcessors)
        n-threads (* 2 n-cpus)]
    (Executors/newFixedThreadPool n-threads
      (reify ThreadFactory
        (newThread
          [_ runnable]
          (doto (Thread. runnable)
            (Thread/.setName (str "q-cpu-" (AtomicLong/.getAndIncrement counter)))
            (Thread/.setDaemon true)))))))


(defonce ^{:doc    "Single-thread executor for scheduling delayed tasks (sleep, timeout)."
           :no-doc true}
  scheduling-executor
  (Executors/newSingleThreadScheduledExecutor
    (reify ThreadFactory
      (newThread
        [_ r]
        (doto (Thread. r "q-se")
          (.setDaemon true))))))


;; # Thread control
;; ################################################################################
(defn conditionally-wait
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

    :else nil))


(defn semaphore
  "Creates a semaphore. By default, `fairness` is set to true, which
   means that allocations will be first-in first-out."
  (^Semaphore [n]
   (Semaphore. (int n) true))
  (^Semaphore [n ^Boolean fair]
   (Semaphore. (int n) fair)))


(defn release
  "Release a permit back to the semaphore."
  [sem]
  (Semaphore/.release sem))


(defn acquire
  "Acquire a permit from the semaphore, blocking until one is available."
  [sem]
  (ml/assert-virtual! (Thread/currentThread))
  (Semaphore/.acquire sem))


(defmacro with-semaphore
  "Execute body while holding a semaphore permit. Releases on completion or exception."
  [^Semaphore sem & body]
  `(do (acquire ~sem)
     (try ~@body
       (finally
         (release ~sem)))))


(defn interrupted?
  "Check if the current or specified thread is interrupted."
  ([]
   (.isInterrupted (Thread/currentThread)))
  ([^Thread thread]
   (.isInterrupted thread)))


(defn comply-interrupt
  "If interruption has been requested in the current thread,
   throw an `InterruptedException`."
  []
  (when (interrupted?) (throw (InterruptedException.))))


(defn throw-on-platform-park!
  "Configure whether awaiting from a platform thread throws an exception.
   When true (default), parking a platform thread raises IllegalStateException.
   Set to false for testing or when platform thread parking is intentional."
  [bool]
  (alter-var-root #'ml/*assert-virtual* (constantly (boolean bool))))


(defn deref-cpu
  "Temporarily bypass the restriction that Tasks can't be parked on platform threads."
  [t]
  (scoping [ml/*assert-virtual* false]
    (deref t)))


;; # Task dereferencing
;; ################################################################################
(defn get-state
  "Return a map of debugging information about a task."
  [t]
  (.getState ^ITask (impl/as-task t)))


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
  (ITask/.getNow (impl/as-task t) default))


(defn task?
  "Returns true if v is a Task, false otherwise.

   Note: Returns false for Promises. Use `(satisfies? ITask v)` to check for both."
  [v]
  (instance? Task v))


(defn taskable?
  "Returns true if v can be converted to a Task via `as-task`.

   True for: Task, Promise, CompletableFuture, Future, core.async channel.
   False for: nil, plain values, collections."
  [v]
  (impl/taskable? v))


(defn as-task
  "Convert a value to a Task.

   - Task/Promise: returned as-is
   - CompletableFuture: wrapped, completes when CF completes
   - Future: wrapped, blocks virtual thread until Future completes
   - core.async channel (optional): wrapped, completes when channel delivers
   - Other values: wrapped in an already-completed Task"
  [v]
  (impl/as-task v))


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
  (-> (as-task t) (get-state) (:exceptional) (true?)))


(defn cancelled?
  "True if the given task is cancelled."
  [t]
  (-> (as-task t) (get-state) (:cancelled) (true?)))


(defn await
  "Block until a task reaches the specified phase.

   Phases (in order): `:pending` `:running` `:grounding` `:transforming` `:writing` `:settling` `:quiescent`

   Returns true when the phase is reached, false if timeout expires.
   Without timeout, blocks indefinitely until phase is reached.

   Args:
     `t`         - The task to await
     `phase`     - Target phase keyword (e.g., `:settling`, `:quiescent`)
     `ms-or-dur` - Optional timeout as milliseconds (long) or `java.time.Duration`

   Must be called from a virtual thread (unless `*assert-virtual*` is false).

   Example:

   ```clojure
   (await task :settling)              ; Block until result available
   (await task :quiescent 1000)        ; With 1 second timeout
   (await task :quiescent (Duration/ofSeconds 5))
   ```"
  ([t phase]
   (ITask/.awaitPhase t phase))
  ([t phase ms-or-dur]
   (if (instance? Duration ms-or-dur)
     (ITask/.awaitPhaseDur t phase ms-or-dur)
     (ITask/.awaitPhaseMillis t phase ms-or-dur))))


;; # Task construction
;; ################################################################################
(defmacro q
  "Execute the body in _the current thread_, then return a task
   containing the return value.

   If the return value is a regular clojure data structure that
   contains tasks, awaits the completion of all contained tasks
   before becoming realized."
  [& body]
  `(let [body# (do ~@body)]
     ;; Don't create a task if the body throws.
     ;; Pass `nil` as the executor. `q` runs on the current thread and will
     ;; never use an executor.
     (doto (-pending-task nil) (Task/.doApply body#))))


(defmacro task
  "Create and run a task on the virtual thread executor.
   Returns the Task immediately; body executes asynchronously.
   Deref (@) blocks until the task settles and returns the result or throws."
  [& body]
  `(do-runner virtual-executor ~@body))


(defmacro cpu-task
  "Execute a CPU-bound task on the platform thread pool.
   Use this for compute-intensive work without blocking operations."
  [& body]
  `(do-runner cpu-executor ~@body))


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
  (doto (-pending-task nil) (Task/.doApply nil e)))


(defn ^:no-doc -compel
  [v]
  ;; Attempt to convert to task, ie. only Task, CompletableFuture, etc.
  (let [t (if (impl/groundable? v) (impl/as-task v) v)]
    (if (task? t)
      ;; If we successfully converted to a Task, proceed
      (let [moat (-pending-task nil true)]
        ;; Forward direct cancellation from moat to inner task
        (impl/subscribe-callback moat impl/phase-settling
          (fn [^TaskState state]
            (when (.-cancelled state)
              (Task/.doCancel t "Task cancelled directly."))))
        ;; Moat grounds with the inner task as value.
        (doto moat (Task/.doApply t)))
      ;; If task conversion was unsuccessful, throw. This includes Promise, which
      ;; must not ever be compelled, or it may become orphaned and become a resource leak.
      (throw (IllegalArgumentException. (format "Can only compel Tasks, not '%s'" (class v)))))))


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
     `(then task f)` - Calls `(f @task)`

   Multiple tasks:
     `(then t1 t2 t3 f)` - Waits for all tasks in parallel, calls `(f @t1 @t2 @t3)`"
  ([t f] (ITask/.doThen (impl/as-task t) virtual-executor f))
  ([t1 t2 f] (do-applier virtual-executor [t1 t2] nil (partial apply f)))
  ([t1 t2 t3 f] (do-applier virtual-executor [t1 t2 t3] nil (partial apply f)))
  ([t1 t2 t3 t4 f] (do-applier virtual-executor [t1 t2 t3 t4] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 f] (do-applier virtual-executor [t1 t2 t3 t4 t5] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 f] (do-applier virtual-executor [t1 t2 t3 t4 t5 t6] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 f] (do-applier virtual-executor [t1 t2 t3 t4 t5 t6 t7] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 f] (do-applier virtual-executor [t1 t2 t3 t4 t5 t6 t7 t8] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 f] (do-applier virtual-executor [t1 t2 t3 t4 t5 t6 t7 t8 t9] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 f] (do-applier virtual-executor [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 & more]
   (do-applier virtual-executor (into [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11] (butlast more)) nil (partial apply (last more)))))


(defn then-cpu
  "Chain a function after one or more tasks complete.

   Executes on the platform thread pool.

   Single task:
     `(then task f)` - Calls `(f @task)`

   Multiple tasks:
     `(then t1 t2 t3 f)` - Waits for all tasks in parallel, calls `(f @t1 @t2 @t3)`"
  ([t f] (ITask/.doThen (impl/as-task t) cpu-executor f))
  ([t1 t2 f] (do-applier cpu-executor [t1 t2] nil (partial apply f)))
  ([t1 t2 t3 f] (do-applier cpu-executor [t1 t2 t3] nil (partial apply f)))
  ([t1 t2 t3 t4 f] (do-applier cpu-executor [t1 t2 t3 t4] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 f] (do-applier cpu-executor [t1 t2 t3 t4 t5] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 f] (do-applier cpu-executor [t1 t2 t3 t4 t5 t6] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 f] (do-applier cpu-executor [t1 t2 t3 t4 t5 t6 t7] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 f] (do-applier cpu-executor [t1 t2 t3 t4 t5 t6 t7 t8] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 f] (do-applier cpu-executor [t1 t2 t3 t4 t5 t6 t7 t8 t9] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 f] (do-applier cpu-executor [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] nil (partial apply f)))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 & more]
   (do-applier cpu-executor (into [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11] (butlast more)) nil (partial apply (last more)))))


(defn catch
  "Handle errors from a task with a recovery function.

   Arities:
     `(catch task f)` - Handle any Throwable
     `(catch task Type f)` - Handle specific exception type
     `(catch task Type1 f1 Type2 f2 ...)` - Handle multiple types (first match wins)

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
   (ITask/.doCatch (impl/as-task t) virtual-executor f))
  ([t err1 f1]
   (ITask/.doCatchTyped (impl/as-task t) virtual-executor [err1 f1]))
  ([t err1 f1 err2 f2]
   (ITask/.doCatchTyped (impl/as-task t) virtual-executor [err1 f1 err2 f2]))
  ([t err1 f1 err2 f2 err3 f3]
   (ITask/.doCatchTyped (impl/as-task t) virtual-executor [err1 f1 err2 f2 err3 f3]))
  ([t err1 f1 err2 f2 err3 f3 err4 f4]
   (ITask/.doCatchTyped (impl/as-task t) virtual-executor [err1 f1 err2 f2 err3 f3 err4 f4]))
  ([t err1 f1 err2 f2 err3 f3 err4 f4 err5 f5]
   (ITask/.doCatchTyped (impl/as-task t) virtual-executor [err1 f1 err2 f2 err3 f3 err4 f4 err5 f5]))
  ([t err1 f1 err2 f2 err3 f3 err4 f4 err5 f5 & pairs]
   (ITask/.doCatchTyped (impl/as-task t) virtual-executor (into [err1 f1 err2 f2 err3 f3 err4 f4 err5 f5] pairs))))


(defn catch-cpu
  "Handle errors from a task with a recovery function.

   Executes on the platform thread pool.

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
   (ITask/.doCatch (impl/as-task t) cpu-executor f))
  ([t err1 f1]
   (ITask/.doCatchTyped (impl/as-task t) cpu-executor [err1 f1]))
  ([t err1 f1 err2 f2]
   (ITask/.doCatchTyped (impl/as-task t) cpu-executor [err1 f1 err2 f2]))
  ([t err1 f1 err2 f2 err3 f3]
   (ITask/.doCatchTyped (impl/as-task t) cpu-executor [err1 f1 err2 f2 err3 f3]))
  ([t err1 f1 err2 f2 err3 f3 err4 f4]
   (ITask/.doCatchTyped (impl/as-task t) cpu-executor [err1 f1 err2 f2 err3 f3 err4 f4]))
  ([t err1 f1 err2 f2 err3 f3 err4 f4 err5 f5]
   (ITask/.doCatchTyped (impl/as-task t) cpu-executor [err1 f1 err2 f2 err3 f3 err4 f4 err5 f5]))
  ([t err1 f1 err2 f2 err3 f3 err4 f4 err5 f5 & pairs]
   (ITask/.doCatchTyped (impl/as-task t) cpu-executor (into [err1 f1 err2 f2 err3 f3 err4 f4 err5 f5] pairs))))



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
  (ITask/.doHandle (impl/as-task t) virtual-executor f))


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
  (ITask/.doHandle (impl/as-task t) cpu-executor f))


(defn ok
  "Run a side-effecting function after a task completes successfully.

   Calls `(f value)` when the task succeeds.

   The function is for side effects:
   - Return value is ignored (original task value passes through)
   - If `f` throws, that exception fails the task chain
   - Errors and cancellation skip calling the function

   Use for logging, metrics, or triggering downstream effects on success."
  [t f]
  (ITask/.doOk (impl/as-task t) virtual-executor f))


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
  (ITask/.doOk (impl/as-task t) cpu-executor f))


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
  (ITask/.doErr (impl/as-task t) virtual-executor f))


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
  (ITask/.doErr (impl/as-task t) cpu-executor f))


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
  (ITask/.doDone (impl/as-task t) virtual-executor f))


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
  (ITask/.doDone (impl/as-task t) cpu-executor f))


(defn finally
  "Run a function after a task completes, regardless of outcome.

   The function receives `[value error cancelled?]`:
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
  (ITask/.doFinally (impl/as-task t) virtual-executor f))


(defn finally-cpu
  "Run a function after a task completes, regardless of outcome.

   Executes on the platform thread pool.

   The function receives `[value error cancelled?]`:
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
  (ITask/.doFinally (impl/as-task t) cpu-executor f))


;; # Coordination
;; ################################################################################
(defn cancel
  "Attempt to cancel a task. Returns a `Task[Boolean]`.

   The returned task settles with:
   - `true`: This cancel call won the race and successfully cancelled the task
   - `false`: The task was already settled, or another cancel call won first

   The returned task settles when the target task reaches quiescent phase,
   regardless of the boolean result. This enables coordination code to wait
   for complete teardown before proceeding.

   Usage:

   ```clojure
   (cancel task)   ; Fire-and-forget: attempt cancellation without blocking
   @(cancel task)  ; Wait for target to reach quiescent, returns boolean
   ```

   Throws `UnsupportedOperationException` if called on a Promise. Promises are
   externally controlled - use `fail` to complete with an error instead."
  [t]
  (ITask/.doCancelDirect t))


(defn race
  "Race multiple tasks, returning the first successful result.

   Returns a Task that completes with the value of whichever task settles first
   with a non-exceptional result. Losing tasks are cancelled (unless compelled).

   If all tasks fail, returns an exception. If one task fails, returns the
   combined errors as ex-info with :errors key."
  [& tasks]
  (impl/race tasks))


(defn race-stateful
  "Race tasks that produce stateful resources, with cleanup for losers.

   Like `race`, but handles the edge case where multiple tasks complete
   simultaneously. When two tasks both produce a value before a winner is
   determined, one wins and the other's value is passed to `release`.

   Args:
     `release` - Function called with each orphaned value (realized but lost)
     `tasks`   - Tasks to race

   Example:

   ```clojure
   (race-stateful #(.close %) alloc-a alloc-b alloc-c)
   ```

   Returns a Task that completes with the first successful result.
   Losing tasks are cancelled. If all tasks fail, returns the combined errors."
  [release & tasks]
  (impl/race tasks {:release release}))


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
  `(q (mapv (fn per-item# [~bind-to] ~@body) ~coll)))


(def ^:private plain-merge
  (comp impl/plain (partial apply merge)))


(defn qmerge
  "Takes a map where the values may be tasks. If multiple maps are given, merges them.

   Returns a Task containing a map with all nested tasks resolved to their values.

   Prefer this over `(then task-a task-b merge)` - it skips the grounding phase
   since the result is known to contain only resolved values."
  ([] (q {}))
  ([m] (q m))
  ([m1 m2] (do-applier nil [m1 m2] nil plain-merge))
  ([m1 m2 m3] (do-applier nil [m1 m2 m3] nil plain-merge))
  ([m1 m2 m3 m4] (do-applier nil [m1 m2 m3 m4] nil plain-merge))
  ([m1 m2 m3 m4 m5] (do-applier nil [m1 m2 m3 m4 m5] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6] (do-applier nil [m1 m2 m3 m4 m5 m6] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7] (do-applier nil [m1 m2 m3 m4 m5 m6 m7] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8] (do-applier nil [m1 m2 m3 m4 m5 m6 m7 m8] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8 m9] (do-applier nil [m1 m2 m3 m4 m5 m6 m7 m8 m9] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8 m9 m10] (do-applier nil [m1 m2 m3 m4 m5 m6 m7 m8 m9 m10] nil plain-merge))
  ([m1 m2 m3 m4 m5 m6 m7 m8 m9 m10 & ms]
   (do-applier nil (into [m1 m2 m3 m4 m5 m6 m7 m8 m9 m10] ms) nil plain-merge)))


(def ^:private plain-last
  (comp impl/plain last))


(defn qdo
  "Await all tasks, but only return the output of the last one.

   If one task throws, all are cancelled."
  ([] (q nil))
  ([t] (q t))
  ([t1 t2] (do-applier nil [t1 t2] nil plain-last))
  ([t1 t2 t3] (do-applier nil [t1 t2 t3] nil plain-last))
  ([t1 t2 t3 t4] (do-applier nil [t1 t2 t3 t4] nil plain-last))
  ([t1 t2 t3 t4 t5] (do-applier nil [t1 t2 t3 t4 t5] nil plain-last))
  ([t1 t2 t3 t4 t5 t6] (do-applier nil [t1 t2 t3 t4 t5 t6] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7] (do-applier nil [t1 t2 t3 t4 t5 t6 t7] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8] (do-applier nil [t1 t2 t3 t4 t5 t6 t7 t8] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9] (do-applier nil [t1 t2 t3 t4 t5 t6 t7 t8 t9] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] (do-applier nil [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] nil plain-last))
  ([t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 & ts]
   (do-applier nil (into [t1 t2 t3 t4 t5 t6 t7 t8 t9 t10] ts) nil plain-last)))


;; # Timing and lifecycle
;; ################################################################################
(defn sleep
  "Create a task that sleeps for the specified duration, then returns `default`.

   Args:
     `ms-or-duration` - Sleep duration as either:
                        - Long: non-negative milliseconds (0 = immediate return)
                        - `java.time.Duration`: non-negative duration
     `default`        - Optional value to return after sleep (default: nil)
                        - If a function, calls it and returns its result
                        - If a Throwable, fails the task with that exception

   Returns a task that completes after the specified duration.
   Throws for negative or unsupported duration values.

   Example:

   ```clojure
   @(sleep 100)                      ; Sleep 100ms, return nil
   @(sleep 100 :done)                ; Sleep 100ms, return :done
   @(sleep (Duration/ofSeconds 1))   ; Sleep 1 second, return nil
   @(sleep 100 (Exception. \"Boom\")) ; Sleep 100ms, then throw
   ```"
  ([ms-or-duration]
   (sleep ms-or-duration nil))
  ([ms-or-duration default]
   (let [nanos  (long (cond
                        (nat-int? ms-or-duration)
                        (* (long ms-or-duration) 1000000)

                        (and (instance? Duration ms-or-duration) (not (Duration/.isNegative ms-or-duration)))
                        (Duration/.toNanos ^Duration ms-or-duration)

                        :else
                        (throw (IllegalArgumentException. (format "Unsupported time unit '%s'. Use non-negative Long or Duration." (class ms-or-duration))))))
         task   (-pending-task virtual-executor)
         runner (ScheduledExecutorService/.schedule scheduling-executor
                  ^Runnable (fn runner
                              []
                              (cond
                                (fn? default) (ITask/.doRun task default)
                                (instance? Throwable default) (ITask/.doApply task nil default)
                                :else (ITask/.doApply task default)))
                  nanos TimeUnit/NANOSECONDS)]
     (doto task
       (impl/subscribe-cancel-future impl/phase-settling runner)))))


(defn timeout
  "Asynchronous timeout that races a task against a sleep timer.

   Roughly equivalent to `(deref task ms-or-dur default)`, except:
   - Asynchronous: doesn't block the calling thread unless dereferenced. Returns a task.
   - `default` is optional. If omitted, throws a `TimeoutException` on timeout.
      - If a function is given, executes it on timeout, and returns its result.
      - If an exception is given, throws it on timeout.

   Args:
     `t`              - Task to race against timeout
     `ms-or-dur`      - Timeout duration as either:
                        - Long: milliseconds
                        - `java.time.Duration`: duration object
     `default`        - Optional value/behavior on timeout:
                        - Value: returned on timeout
                        - Function: executed and its result returned
                        - Exception: thrown on timeout
                        - Omitted: throws `TimeoutException`

   Returns a task that completes with either the task's result or the timeout default.

   Example:

   ```clojure
   @(timeout my-task 1000)                     ; Throw TimeoutException after 1s
   @(timeout my-task 1000 :timed-out)          ; Return :timed-out after 1s
   @(timeout my-task (Duration/ofSeconds 5))   ; Throw after 5 seconds
   ```"
  ([t ms-or-dur]
   (timeout t ms-or-dur (TimeoutException. (str "Task timed out after " ms-or-dur "."))))
  ([t ms-or-dur default]
   (impl/race [t (sleep ms-or-dur ::timeout)]
     {:tf (fn handle-default
            [res]
            (if (= ::timeout res)
              (cond
                (fn? default) (default)
                (instance? Throwable default) (throw default)
                :else default)
              res))})))


(defn monitor
  "Non-destructive monitoring wrapper that observes a task without affecting its outcome.

   Unlike `timeout`, this does NOT race the task or change its result. Instead, it runs
   a side-effect function if the task hasn't completed within the specified duration.
   The original task continues running and its result is returned unchanged.

   This is useful for diagnostics: log warnings when operations are slow without
   actually timing them out or affecting their execution.

   Implementation: Uses `compel` to create a protected wrapper around the task, then
   races that wrapper against a timeout. When the timeout fires, it cancels the wrapper
   but not the original task. The watchdog runs 'on the side' and is discarded.

   Args:
     - `t`              Task to monitor
     - `ms-or-dur`      Duration after which to trigger side effect:
                        - Long: milliseconds
                        - `java.time.Duration`: duration object
     - `side-effect-fn` Function (or value) passed to timeout's default parameter.
                        If a function, it's called when the timeout fires.
                        Exceptions from the side effect are caught and logged.

   Returns the original task unchanged - result and timing are unaffected.

   Example:

   ```clojure
   (-> (slow-operation)
     (monitor 5000
       #(log/warn \"Operation exceeded 5s\")))
   ```

   The side effect runs in a fire-and-forget manner - exceptions are logged but don't
   affect the monitored task."
  [t ms-or-dur side-effect-fn]
  (timeout (compel t) ms-or-dur
    (fn []
      (try (side-effect-fn)
        (catch Throwable _e))
      ;; Regardless of side-effect-fn, return `t`. Will be grounded on return.
      t)))


(defn- default-validate
  [v e]
  (if (instance? Throwable e)
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
     (impl/as-task)
     (handle validate)
     (catch
       (fn [e]
         (if (or (zero? retries) (interrupted?)
               (instance? CancellationException e))
           ;; Rethrow error if:
           ;; 1. No more retries left.
           ;; 2. Thread has been interrupted.
           ;; 3. Task has been cancelled.
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
   externally.

   Chaining operations on promises work as with tasks.

   ```clojure
   (def p (q/promise))
   (deliver p :result)  ; Delivers :result to the promise
   (p :result)          ; also works
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
  (Promise. (-pending-task nil)))


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
  (doto p (Promise/.doApply nil e)))


;; # Task conversion
;; ################################################################################
(defn as-cf
  "Convert a task to a CompletableFuture.

   The returned future completes when the task settles, with the same
   value, exception, or cancellation status."
  [t]
  (let [cf (CompletableFuture.)]
    (doto (impl/as-task t)
      (impl/subscribe-callback impl/phase-settling
        (fn [^TaskState state]
          (cond
            (.-cancelled state)
            (CompletableFuture/.cancel cf true)

            (.-exceptional state)
            (CompletableFuture/.completeExceptionally cf (.-result state))

            :else
            (CompletableFuture/.complete cf (.-result state))))))
    cf))
