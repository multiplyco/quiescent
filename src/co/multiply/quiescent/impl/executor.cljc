(ns ^:no-doc co.multiply.quiescent.impl.executor
  #?(:cljs (:require
             [co.multiply.scoped :refer [ask]]))
  #?(:clj (:import
            [clojure.lang DynamicClassLoader RT]
            [java.lang Thread$Builder]
            [java.time Duration]
            [java.util.concurrent ExecutorService Executors ForkJoinPool ForkJoinPool$ForkJoinWorkerThreadFactory ScheduledExecutorService ThreadFactory ThreadPerTaskExecutor TimeUnit]
            [java.util.concurrent.atomic AtomicLong])))


#?(:clj (defonce ^{:doc "Classloader captured at namespace load and set as the context classloader on every
                         executor thread below. Worker threads are constructed lazily on whatever thread
                         submits the first task, so without this they inherit that submitter's context
                         classloader. When the submitter is a thread whose context loader can't resolve
                         Clojure's own classes (e.g. a Rama module/daemon loader), lazy class generation on
                         the worker - Specter dynamic-path eval, runtime reflection, agent fns - fails with
                         `ClassNotFoundException: clojure.lang.AFunction`. baseLoader at load time is a
                         DynamicClassLoader that can resolve clojure.lang.* (and the loaded app classes).

                         The captured loader is wrapped in a DynamicClassLoader so that by-name lookups from
                         task threads (Class/forName via the context loader, e.g. nippy thaw) can also find
                         classes compiled mid-session. In an AOT-compiled host baseLoader is the plain
                         application loader, which neither delegates down to REPL-created loaders nor knows
                         Clojure's global class cache - so a defrecord eval'd on a deployed host would be
                         unresolvable by name inside tasks. Any DynamicClassLoader instance consults that
                         static cache during loadClass, so an empty wrapper bridges the gap; in a
                         source-loaded JVM it adds one inert parent hop."}
          ^ClassLoader base-classloader
          (DynamicClassLoader. (RT/baseLoader))))


#?(:clj (defonce ^{:doc "Virtual thread executor for IO-bound tasks. Creates a new virtual thread per task.
                         Default executor for tasks - use for network calls, file IO, and blocking operations."}
          ^ThreadPerTaskExecutor virtual-executor
          (Executors/newThreadPerTaskExecutor
            (reify ThreadFactory
              (newThread
                [_ runnable]
                (doto (-> (Thread/ofVirtual)
                        (Thread$Builder/.name "q-io")
                        (Thread$Builder/.unstarted runnable))
                  (Thread/.setContextClassLoader base-classloader)))))))


(defn delegate-virtual
  "Queue work for async execution on virtual threads (CLJ) or microtask queue (CLJS).

   Use for IO-bound operations: network calls, file IO, and other blocking work.
   This is the default delegator for tasks.

   Returns a cancellation handle:
   - CLJ: Future (cancel via Future/.cancel)
   - CLJS: Function that aborts the task's AbortController"
  [f]
  #?(:clj  (ExecutorService/.submit virtual-executor ^Runnable f)
     :cljs (do (js/queueMicrotask f) nil)))


#?(:clj (defonce ^{:doc "Work stealing thread pool for CPU-bound tasks. Use for compute-intensive work
                         that should not block. Pool size scales with available CPUs."}
          ^ForkJoinPool cpu-executor
          (let [counter (AtomicLong. 0)
                n-cpus  (Runtime/.availableProcessors (Runtime/getRuntime))]
            (ForkJoinPool. n-cpus
              (reify ForkJoinPool$ForkJoinWorkerThreadFactory
                (newThread
                  [_ pool]
                  (doto (ForkJoinPool$ForkJoinWorkerThreadFactory/.newThread ForkJoinPool/defaultForkJoinWorkerThreadFactory pool)
                    (Thread/.setName (str "q-cpu-" (AtomicLong/.getAndIncrement counter)))
                    (Thread/.setContextClassLoader base-classloader))))
              nil false))))


(defn delegate-cpu
  "Queue work for execution on the CPU thread pool (CLJ only).

   Use for compute-intensive operations that should not block.
   Throws in ClojureScript - use delegate-virtual instead.

   Returns a Future (CLJ) for cancellation."
  [f]
  #?(:clj  (ExecutorService/.submit cpu-executor ^Runnable f)
     :cljs (throw (js/Error. "CPU executor not supported in ClojureScript."))))


#?(:clj (defonce ^{:doc "Single-thread executor for scheduling delayed tasks (sleep, timeout)."}
          ^ScheduledExecutorService scheduling-executor
          (Executors/newSingleThreadScheduledExecutor
            (reify ThreadFactory
              (newThread
                [_ r]
                (doto (Thread. r "q-se")
                  (Thread/.setDaemon true)
                  (Thread/.setContextClassLoader base-classloader)))))))


#?(:clj  (defn delegate-scheduled
           "Schedule work for delayed execution.

            Use for sleep, timeout, and other time-based operations.

            Args:
              ms-or-duration - Delay as milliseconds (non-negative Long) or java.time.Duration
              f - Function to execute after delay

            Returns a ScheduledFuture for cancellation."
           [ms-or-duration f]
           (let [nanos (long (cond
                               (nat-int? ms-or-duration)
                               (* (long ms-or-duration) 1000000)

                               (and (instance? Duration ms-or-duration) (not (Duration/.isNegative ms-or-duration)))
                               (Duration/.toNanos ^Duration ms-or-duration)

                               :else
                               (throw (IllegalArgumentException. (format "Unsupported time unit '%s'. Use non-negative Long or Duration." (class ms-or-duration))))))]
             (ScheduledExecutorService/.schedule scheduling-executor ^Runnable f nanos TimeUnit/NANOSECONDS)))
   :cljs (defn delegate-scheduled
           "Schedule work for delayed execution.

            Use for sleep, timeout, and other time-based operations.

            Args:
              ms - Delay in milliseconds (non-negative integer)
              f - Function to execute after delay

            Returns a function that cancels the timeout via clearTimeout."
           [ms f]
           (if (nat-int? ms)
             (let [timeout-ref (js/setTimeout f ms)]
               (fn cancel-timeout [] (js/clearTimeout timeout-ref)))
             (throw (js/Error. "Unsupported time unit '" ms "'. Use a non-negative number.")))))


(defn delegate-sync
  "Execute work synchronously on the current thread.

   Use for lightweight operations that complete immediately,
   such as grounding already-resolved values or synchronous transforms.

   Returns nil (no cancellation handle - work completes before returning)."
  [f]
  (f)
  nil)
