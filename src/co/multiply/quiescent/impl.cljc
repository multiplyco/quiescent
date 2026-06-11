(ns ^:no-doc co.multiply.quiescent.impl
  "Task implementation: state machine, executors, and the Task type.

   Tasks progress through phases gated by CAS transitions on a MachineLatch.
   See `state-machine` for the lifecycle definition and `Task` for coordination details."
  (:refer-clojure :exclude [await promise])
  #?(:cljs (:require-macros co.multiply.quiescent.impl))
  (:require
    [co.multiply.machine-latch :as ml]
    [co.multiply.conc.deque :as deque]
    [co.multiply.quiescent.impl.executor :as executor :refer [delegate-virtual delegate-sync]]
    [co.multiply.quiescent.impl.ground :refer [ground]]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.quiescent.type :as type #?@(:cljs [:refer [ICancellable ITask IStateful TaskState]])]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [assoc-scope with-scope current-scope ask]])
  #?(:clj (:import
            [clojure.lang IBlockingDeref IDeref IFn IPending]
            [co.multiply.quiescent.impl ICancellable ITask IStateful TaskState]
            [java.util Iterator Set]
            [java.util.concurrent CancellationException CompletableFuture ConcurrentHashMap ConcurrentHashMap$KeySetView ExecutorService Future]
            [java.util.concurrent.atomic AtomicInteger]
            [java.util.function BiConsumer])))


;; # Globals
;; ################################################################################
(def ^{:dynamic true :doc "The currently executing task, used for parent-child registration."} *this*)

(declare -pending-task task?)


;; # Low-level helpers
;; ################################################################################
(defn- filter-by-index
  "Returns a vector of elements from coll where (pred index) is true.

   Example: (filter-by-index even? [:a :b :c :d]) => [:a :c]"
  [pred coll]
  (into [] (comp
             (map-indexed (fn [idx v] (if (pred idx) v ::remove)))
             (remove (partial #?(:clj identical? :cljs keyword-identical?) ::remove)))
    coll))


(defn cancelled?
  "True if the given task is cancelled."
  [t]
  (call/isCancelled t))


;; # Task definition
;; ################################################################################
(deftype Task [delegator latch ^:volatile-mutable state subscriptions compelled ^:volatile-mutable scope]
  #?(:clj IDeref)
  #?(:clj (deref [this]
            (Task/.awaitPhase this sm/phase-settling)
            (type/throw-boxed-error state)))

  #?(:clj IBlockingDeref)
  #?(:clj (deref [this timeout-ms timeout-val]
            (if (Task/.awaitPhaseMillis this sm/phase-settling ^long timeout-ms)
              (type/throw-boxed-error state)
              timeout-val)))


  IPending
  #?(:clj  (isRealized [_this]
             (ml/at-or-past? latch sm/phase-settling))
     :cljs (-realized? [_this]
             (ml/at-or-past? latch sm/phase-settling)))

  ICancellable
  ;; Cancellation
  (^boolean doCancel [this ^String msg]
    (call/doWrite this
      (TaskState. true true
        #?(:clj  (CancellationException. msg)
           :cljs (ex-info msg {:cancelled true})))))

  (doCancelDirect [this]
    (doto (-pending-task delegate-virtual)
      (call/doRun
        (fn []
          ;; Either drive cancellation from this executor, or…
          (or (call/doCancel this "Task cancelled directly.")
            ;; … await quiescence, and ground with `false`.
            (call/newQuiescenceProxy this false))))))


  ;; Cascade cancel from parent. Compelled tasks resist - they were marked important
  ;; at creation time and should complete even if parent is cancelled.
  (^boolean doCancelCascade [this]
    (and (not compelled) (call/doCancel this "Task cancelled via cascade.")))

  IStateful
  (^boolean isCancelled [_this] (true? (when state (.-cancelled ^TaskState state))))
  (^boolean isCompelled [_this] compelled)
  (^boolean isExceptional [_this] (true? (when state (.-exceptional ^TaskState state))))
  (^boolean atOrPast [_this ^clojure.lang.Keyword phase] (ml/at-or-past? latch phase))
  (getPhase [_this] (ml/get-state latch))
  (getResult [_this] (when state (.-result ^TaskState state)))
  (getScope [_this] scope)
  (getSubscriptions [_this] subscriptions)
  (setScope [_this new-scope] (set! scope new-scope))

  ITask
  (getNow [_this default]
    (if (instance? TaskState state)
      (type/throw-boxed-error state)
      default))

  (newQuiescenceProxy [this default]
    ;; Return a task which completes with `default` when `this`
    ;; reaches quiescent.
    (let [prx (-pending-task delegate-sync)]
      (subs/subscribe-callback this sm/phase-quiescent
        (fn [_task-state]
          (call/doApply prx default)))
      (subs/subscribe-teardown prx this)
      prx))

  ;; Waiting
  #?(:clj (^boolean awaitPhase [_this ^clojure.lang.Keyword phase]
                               (ml/await latch phase)))

  #?(:clj (^boolean awaitPhaseMillis [_this ^clojure.lang.Keyword phase ^long timeout-ms]
                                     (ml/await-millis latch phase timeout-ms)))

  #?(:clj (^boolean awaitPhaseDur [_this ^clojure.lang.Keyword phase ^java.time.Duration duration]
                                  (ml/await-dur latch phase duration)))

  ;; ## Entry points
  (^boolean doRun [this f tf]
    (doto (ml/transition! latch sm/action-run)
      (when
        #_(call/runSubscriptions this)
        (subs/subscribe-cancel-exec this sm/phase-settling
          (delegator
            (fn run
              []
              (with-scope scope
                (try (call/doGround this (f) tf)
                  (catch #?(:clj Throwable :cljs :default) e
                    (call/doWrite this (TaskState. true false e)))))))))))


  (^boolean doGround [this res tf]
    (doto (ml/transition! latch sm/action-ground)
      (when
        #_(call/runSubscriptions this)
        (ground this res
          (fn do-ground
            [grounded-value exceptional-task-state]
            (cond
              ;; Grounding failed: an inner task returned its (exceptional) task state.
              exceptional-task-state (call/doWrite this exceptional-task-state)
              ;; Grounding was successful, and we have a transformation function.
              tf (call/doTransform this grounded-value tf)
              ;; Grounding was successful, there's no transformation function given.
              :else (call/doWrite this (TaskState. false false grounded-value))))))))

  (^boolean doTransform [this res tf]
    (doto (ml/transition! latch sm/action-transform)
      (when
        #_(call/runSubscriptions this)
        (subs/subscribe-cancel-exec this sm/phase-settling
          (delegator
            (fn transform
              []
              (with-scope scope
                (try
                  (ground this (tf res)
                    (fn do-tf-ground
                      [grounded-value exceptional-task-state]
                      (if exceptional-task-state
                        (call/doWrite this exceptional-task-state)
                        (call/doWrite this (TaskState. false false grounded-value)))))
                  (catch #?(:clj Throwable :cljs :default) e
                    (call/doWrite this (TaskState. true false e)))))))))))

  (^boolean doWrite [this ^TaskState task-state]
    (doto (ml/transition! latch sm/action-write)
      (when
        #_(call/runSubscriptions this)
        (set! state task-state)
        (call/doSettle this))))

  (^boolean doSettle [this]
    (doto (ml/transition! latch sm/action-settle)
      (when
        (call/runSubscriptions this)
        (call/doQuiesce this))))

  (^boolean doQuiesce [this]
    (doto (ml/transition! latch sm/action-quiesce)
      (when
        (call/runSubscriptions this))))


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
    (or (deque/isEmpty subscriptions)
      (with-scope scope
        (deque/forEach [sub subscriptions]
          (when (and (ml/at-or-past? latch (subs/getPhase sub))
                  (deque/remove subscriptions sub))
            (try
              (subs/runSub sub state)
              (catch #?(:clj Throwable :cljs :default) e
                (call/doWrite this (TaskState. true false e)))))))))

  (doSubscribe [_this sub]
    (deque/add subscriptions sub)
    (when (and (ml/at-or-past? latch (subs/getPhase sub))
            (deque/remove subscriptions sub))
      (with-scope scope
        (subs/runSub sub state)))
    sub)

  (^boolean doUnsubscribe [_this sub]
    (deque/remove subscriptions sub))


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

  (doThen [this delegator f]
    (let [link (-pending-task delegator)]
      ;; If the chained `link` task is settling, attempt to cancel `this`.
      ;; Can only happen if `link` is cancelled, since otherwise it can't
      ;; reach the `settling` phase without `this` reaching `settling` first,
      ;; which is an uncancellable phase (action-write can't transition from settling).
      (subs/subscribe-teardown link this)
      ;; Start `then` on the chained `link` task when `this` has a result.
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-then
          [^TaskState state]
          (if (.-exceptional state)
            ;; If the state is unhandleable, don't do any additional work. Just
            ;; reference the existing state, and move on.
            (call/doWrite link state)
            ;; If the state is handleable, run the function.
            (call/doRun link (fn then [] (f (.-result state)))))))
      link))

  (doCatch [this delegator f]
    (let [link (-pending-task delegator)]
      (subs/subscribe-teardown link this)
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-catch
          [^TaskState state]
          (if (and (.-exceptional state) (not (.-cancelled state)))
            (call/doRun link (fn catch [] (f (.-result state))))
            (call/doWrite link state))))
      link))

  (doCatchTyped [this delegator type-handler-pairs]
    (assert (even? (count type-handler-pairs)) "Must have receive an even number of arguments.")
    #?(:clj (assert (every? class? (filter-by-index even? type-handler-pairs)) "Every function must have an associated class.")
       :cljs (assert (every? fn? (filter-by-index even? type-handler-pairs)) "Every function must have an associated predicate."))
    (assert (every? fn? (filter-by-index odd? type-handler-pairs)) "Every class must have an associated function.")
    (let [link (-pending-task delegator)]
      (subs/subscribe-teardown link this)
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-catch
          [^TaskState state]
          (let [result (.-result state)]
            (cond
              (not (.-exceptional state))
              (call/doWrite link state)

              (.-cancelled state)
              (call/doWrite link state)

              :else
              ;; Find the first matching pair given, then run the function.
              (let [c (type/vecCount type-handler-pairs)]
                (loop [idx (unchecked-int 0)]
                  (if (< idx c)
                    (let [err-class   (type/vecNth type-handler-pairs idx)
                          handler-idx (unchecked-inc-int idx)]
                      ;; In CLJ, we take Throwables. In CLJS, we take predicates.
                      (if #?(:clj (instance? err-class result) :cljs (err-class result))
                        (let [handler (type/vecNth type-handler-pairs handler-idx)]
                          (call/doRun link (fn catch [] (handler result))))
                        (recur (unchecked-inc-int handler-idx))))
                    ;; If there are no matches, reference the existing state and move on.
                    (call/doWrite link state))))))))
      link))


  (doHandle [this delegator f]
    (let [link (-pending-task delegator)]
      (subs/subscribe-teardown link this)
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-handle
          [^TaskState state]
          (if (.-cancelled state)
            (call/doWrite link state)
            (call/doRun link
              (fn handle
                []
                (if (.-exceptional state)
                  (f nil (.-result state))
                  (f (.-result state) nil)))))))
      link))


  (doOk [this delegator f]
    (let [link  (-pending-task delegate-sync)
          scope (call/getScope link)]
      (subs/subscribe-teardown link this)
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-ok
          [^TaskState state]
          (if (.-exceptional state)
            (call/doWrite link state)
            (subs/subscribe-cancel-exec link sm/phase-settling
              (delegator
                (fn ok
                  []
                  (with-scope scope
                    (try (f (.-result state))
                      (call/doWrite link state)
                      (catch #?(:clj Throwable :cljs :default) e
                        (call/doApply link nil e))))))))))
      link))


  (doErr [this delegator f]
    (let [link  (-pending-task delegate-sync)
          scope (call/getScope link)]
      (subs/subscribe-teardown link this)
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-err
          [^TaskState state]
          (if (and (.-exceptional state) (not (.-cancelled state)))
            (subs/subscribe-cancel-exec link sm/phase-settling
              (delegator
                (fn err
                  []
                  (with-scope scope
                    (try (f (.-result state))
                      (call/doWrite link state)
                      (catch #?(:clj Throwable :cljs :default) e
                        (call/doApply link nil e)))))))
            (call/doWrite link state))))
      link))


  (doDone [this delegator f]
    (let [link  (-pending-task delegate-sync)
          scope (call/getScope link)]
      (subs/subscribe-teardown link this)
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-done
          [^TaskState state]
          (if (.-cancelled state)
            (call/doWrite link state)
            (subs/subscribe-cancel-exec link sm/phase-settling
              (delegator
                (fn done
                  []
                  (with-scope scope
                    (try
                      (if (.-exceptional state)
                        (f nil (.-result state))
                        (f (.-result state) nil))
                      (call/doWrite link state)
                      (catch #?(:clj Throwable :cljs :default) e
                        (call/doApply link nil e))))))))))
      link))


  (doFinally [this delegator f]
    (let [link  (-pending-task delegate-sync)
          scope (call/getScope link)]
      (subs/subscribe-teardown link this)
      (subs/subscribe-callback this sm/phase-settling
        (fn setup-finally
          [^TaskState state]
          (delegator
            (fn finally
              []
              (with-scope scope
                (let [result    (.-result state)
                      cancelled (.-cancelled state)]
                  (try
                    (if (.-exceptional state)
                      (f nil result cancelled)
                      (f result nil cancelled))
                    (call/doWrite link state)
                    (catch #?(:clj Throwable :cljs :default) e
                      (call/doApply link nil e)))))))))
      link)))


;; # Task construction
;; ################################################################################
(defn -pending-task
  "Create a new Task in pending state. The latch is used as the initial sentinel
   value in the state AtomicReference, allowing CAS to detect first write."
  (^Task [delegator]
   (-pending-task delegator false))
  (^Task [delegator compelled]
   (let [latch  (sm/create-task-latch)
         t      (Task. delegator
                  latch
                  nil
                  (deque/new-deque)
                  compelled
                  nil)
         parent (ask *this* nil)]
     (call/setScope t (assoc-scope (current-scope) *this* t))
     (when parent
       ;; Register with parent for cascade cancellation. When parent settles,
       ;; cascade cancel is best-effort: child may be compelled (resists),
       ;; or already past cancellable phases (no-op).
       (subs/subscribe-teardown parent t))
     t)))


(defn interrupted?
  []
  #?(:clj  (.isInterrupted (Thread/currentThread))
     :cljs false))


(defn -do-run
  [t f]
  (if (interrupted?)
    (doto t (call/doCancel "Task created on interrupted thread."))
    (doto t (call/doRun f))))


(defmacro do-runner
  [delegator & body]
  `(doto (-pending-task ~delegator)
     (-do-run (fn runner# [] ~@body))))


(defmacro do-applier
  "Create a task that grounds a value (and optionally transforms it).

   Unlike `-live-task` which runs a function, this applies a value directly.
   The value is grounded (nested tasks resolved in parallel), then optionally
   transformed by `tf`. Used by coordination functions like `then`, `qmerge`,
   and `qjoin` to await multiple values and apply a combining function.

   Arities:
     `[delegator v]`       - Ground v, no transform
     `[delegator v e]`     - If e, fail with e; otherwise ground v
     `[delegator v e tf]`  - If e, fail with e; otherwise ground v then apply tf"
  ([delegator v]
   `(doto (-pending-task ~delegator) (call/doApply ~v)))
  ([delegator v e]
   `(doto (-pending-task ~delegator) (call/doApply ~v ~e)))
  ([delegator v e tf]
   `(doto (-pending-task ~delegator) (call/doApply ~v ~e ~tf))))


;; # Promise definition
;; ################################################################################
(deftype Promise [^Task task]
  #?(:clj IDeref)
  #?(:clj (deref [_this] (deref task)))

  #?(:clj IBlockingDeref)
  #?(:clj (deref [_this timeout-ms timeout-val] (deref task timeout-ms timeout-val)))


  IPending
  #?(:clj  (isRealized [_this] (realized? task))
     :cljs (-realized? [_this] (realized? task)))

  IFn
  #?(:clj  (invoke [this v] (call/doApply task v) this)
     :cljs (-invoke [this v] (call/doApply task v) this))

  ICancellable
  (^boolean doCancel [_this ^String msg] (call/doCancel task msg))
  (doCancelDirect [_this] (call/doCancelDirect task))
  (^boolean doCancelCascade [_this] (call/doCancelCascade task))

  IStateful
  (^boolean isCancelled [_this] (call/isCancelled task))
  (^boolean isCompelled [_this] (call/isCompelled task))
  (^boolean isExceptional [_this] (call/isExceptional task))
  (^boolean atOrPast [_this ^clojure.lang.Keyword phase] (call/atOrPast task phase))
  (getPhase [_this] (call/getPhase task))
  (getResult [_this] (call/getResult task))
  (getScope [_this] (call/getScope task))
  (getSubscriptions [_this] (call/getSubscriptions task))
  (setScope [_this new-scope] (call/setScope task new-scope))

  ITask
  ;; Info
  (getNow [_this default] (call/getNow task default))
  (newQuiescenceProxy [_this default] (call/newQuiescenceProxy task default))

  ;; Waiting
  #?(:clj (^boolean awaitPhase [_this ^clojure.lang.Keyword phase] (.awaitPhase task phase)))
  #?(:clj (^boolean awaitPhaseMillis [_this ^clojure.lang.Keyword phase ^long timeout-ms] (.awaitPhaseMillis task phase timeout-ms)))
  #?(:clj (^boolean awaitPhaseDur [_this ^clojure.lang.Keyword phase ^java.time.Duration duration] (.awaitPhaseDur task phase duration)))

  ;; ## Entry points
  (^boolean doRun [_this _f _tf] (throw (type/unsupported-operation-exception "Promises can't run functions.")))
  (^boolean doGround [_this res tf] (call/doGround task res tf))
  (^boolean doTransform [_this res tf] (call/doTransform task res tf))
  (^boolean doWrite [_this ^TaskState task-state] (call/doWrite task task-state))
  (^boolean doSettle [_this] (call/doSettle task))
  (^boolean doQuiesce [_this] (call/doQuiesce task))
  (runSubscriptions [_this] (call/runSubscriptions task))
  (doSubscribe [_this sub] (call/doSubscribe task sub))
  (^boolean doUnsubscribe [_this sub] (call/doUnsubscribe task sub))
  (doThen [_this delegator f] (call/doThen task delegator f))
  (doCatch [_this delegator f] (call/doCatch task delegator f))
  (doCatchTyped [_this delegator type-handler-pairs]
    (call/doCatchTyped task delegator type-handler-pairs))
  (doHandle [_this delegator f] (call/doHandle task delegator f))
  (doOk [_this delegator f] (call/doOk task delegator f))
  (doErr [_this delegator f] (call/doErr task delegator f))
  (doDone [_this delegator f] (call/doDone task delegator f))
  (doFinally [_this delegator f] (call/doFinally task delegator f)))


;; # Task conversion
;; ################################################################################
(extend-protocol type/ITaskable
  Task
  (taskable? [_] true)
  (groundable? [_] true)
  (as-task [t] t)

  Promise
  (taskable? [_] true)
  (groundable? [_] true)
  (as-task [p] p)

  nil
  (taskable? [_] false)
  (groundable? [_] false)
  (as-task [_] (co.multiply.quiescent.impl/do-applier delegate-sync nil)))


#?(:clj  (extend-protocol type/ITaskable
           CompletableFuture
           (taskable? [_] true)
           (groundable? [_] true)
           (as-task [^CompletableFuture cf]
             ;; `sync` is used as the delegator since it never will be used (doApply path).
             (let [task (doto (-pending-task delegate-sync)
                          (subs/subscribe-cancel-exec sm/phase-settling cf))]
               (letfn [(propagate-cf-result
                         [v e]
                         (if (CompletableFuture/.isCancelled cf)
                           (call/doCancel task "CompletableFuture cancelled.")
                           (call/doApply task v e)))]
                 (CompletableFuture/.whenCompleteAsync cf ^BiConsumer propagate-cf-result executor/virtual-executor))
               task))

           Future
           (taskable? [_] true)
           (groundable? [_] true)
           (as-task [^Future future]
             (let [task (doto (-pending-task delegate-virtual)
                          (subs/subscribe-cancel-exec sm/phase-settling future))]
               (call/doRun task
                 (fn []
                   (try
                     (call/doApply task (Future/.get future) nil)
                     (catch CancellationException e
                       (if (Future/.isCancelled future)
                         (call/doCancel task "Future cancelled.")
                         (call/doApply task nil e)))
                     (catch Throwable e
                       (call/doApply task nil e)))))
               task))

           Object
           (taskable? [_] false)
           (groundable? [_] false)
           (as-task [obj] (co.multiply.quiescent.impl/do-applier delegate-sync obj)))
   :cljs (extend-protocol type/ITaskable
           js/Promise
           (taskable? [_] true)
           (groundable? [_] true)
           (as-task [p]
             ;; `nil` is used as the executor since it never will be used (doApply path).
             (let [task (-pending-task delegate-virtual)]
               (letfn [(propagate-success
                         [v]
                         (call/doApply task v nil))
                       (propagate-failure
                         [e]
                         (call/doApply task nil e))]
                 (.then p propagate-success propagate-failure))
               task))

           default
           (taskable? [_] false)
           (groundable? [_] false)
           (as-task [obj] (co.multiply.quiescent.impl/do-applier delegate-sync obj))))
