(ns co.multiply.quiescent.timeout-test
  (:require
    [clojure.test :refer [is]]
    [co.multiply.quiescent :as q :refer [q qjoin]]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.test-support :refer [clause def-results make-exception result with-task]]
    [co.multiply.quiescent.type.call :as call])
  #?(:clj (:import
            [java.time Duration])))


(def-results timeout-test
  (clause :slow "Timeout on slow task"
    (let [task (q/sleep 1000 :result)]
      (with-task (q/timeout task 1)
        (result [_v e c]
          (is (some? e))
          (is (false? c))
          (is (q/cancelled? task))))))

  (clause :default-value "Timeout with default value"
    (let [task (q/sleep 1000 :result)]
      (with-task (q/timeout task 1 :timed-out)
        (result [v e c]
          (is (nil? e))
          (is (false? c))
          (is (= :timed-out v))
          (is (q/cancelled? task))))))

  (clause :fast "Fast task completes before timeout"
    (let [task (q/sleep 1 :result)]
      (with-task (q/timeout task 15 :timed-out)
        (result [v e c]
          (is (nil? e))
          (is (false? c))
          (is (= :result v)))))))


(def-results timeout-settlement-test
  ;; `timeout` races the task against a timer, and the timer is guaranteed to
  ;; eventually succeed. So if the race is decided on the first NON-EXCEPTIONAL
  ;; result, the timer wins by walkover whenever the task fails or is cancelled
  ;; — reporting a timeout for something that settled long before the deadline,
  ;; with the real outcome discarded. The task must win on ANY settlement.
  ;;
  ;; The timeouts below are set far longer than the task takes, so a regression
  ;; does not merely slow these tests down: the timer's result is a different
  ;; value from the task's, and the assertions read the difference.

  (clause :exception "A failing task wins the race with its own exception"
    (let [e1   (make-exception "Boom")
          task (q/sleep 1 e1)]
      (with-task (q/timeout task 1000)
        (result [_v e c]
          (is (false? c))
          (is (identical? e1 e)
            "the task's own exception, not a TimeoutException at the deadline")))))

  (clause :exception-with-default "A default covers the timer firing, not the task failing"
    ;; The distinction matters: swallowing failures is `catch`'s job. Conflating
    ;; the two makes "bound the wait, but let me see errors" inexpressible, and
    ;; costs the full timeout to report a failure that already happened.
    (let [e1   (make-exception "Boom")
          task (q/sleep 1 e1)]
      (with-task (q/timeout task 1000 :timed-out)
        (result [v e c]
          (is (false? c))
          (is (identical? e1 e))
          (is (nil? v) "must not resolve to the default"))))))


(def-results timeout-cancellation-test
  (clause :awaited-task "Cancelling the awaited task settles the timeout as cancelled"
    ;; Cancellation cascades from parent to child, not between siblings, so the
    ;; timer does not stop merely because the task it races has been cancelled.
    ;; `timeout` links the two, tearing the timer down when the task settles for
    ;; any reason — otherwise this reports a timeout at the deadline instead.
    (let [task  (q/sleep 1000 :never)
          timed (q/timeout task 100 :timed-out)]
      (q/cancel task)
      (with-task timed
        (result [v _e c]
          (is (true? c) "cancellation must stay cancellation, not become a timeout")
          (is (not= :timed-out v))))))

  (clause :timeout-task "Cancelling the timeout cancels the awaited task"
    (let [task  (q/sleep 1000 :never)
          timed (q/timeout task 1000 :timed-out)]
      (with-task (q/cancel timed)
        (result []
          (is (q/cancelled? timed))
          (is (q/cancelled? task)))))))


(def-results timeout-passthrough-test
  ;; `timeout` adopts the awaited task's settled state wholesale rather than
  ;; re-applying its value, which is sound only because a settled task's result
  ;; is already grounded — the task boundary guarantees no inner task survives
  ;; it. Re-applying would walk the entire value a second time looking for
  ;; tasks that cannot be there, at a cost that scales with the value's size.
  ;; This pins the assumption from `timeout`'s side: were it ever to stop
  ;; holding, the passthrough would hand callers an unresolved task.

  (clause :grounded "Inner tasks in the awaited value are resolved, not passed through"
    (let [task (q/task {:a (q/sleep 1 :inner) :b [(q/sleep 1 :deep)]})]
      (with-task (q/timeout task 1000)
        (result [v e c]
          (is (nil? e))
          (is (false? c))
          (is (= {:a :inner :b [:deep]} v)))))))


(def-results timeout-precedence-test
  ;; A settled task and a zero deadline is a nonsense proposition, and precisely
  ;; because of that it is the sharpest test of precedence: there is no interval
  ;; in which the task can be said to have got there first. It must still win.
  ;;
  ;; What makes that hold is construction order — `t` is subscribed before the
  ;; timer exists, so an already-settled task claims the result synchronously,
  ;; during the call, with nothing to contest it. Build the timer first and this
  ;; degrades to a genuine race that the timer sometimes wins.

  (clause :settled "A settled task beats a zero deadline"
    (let [task (q :immediate)]
      (with-task (q/timeout task 0 :timed-out)
        (result [v e c]
          (is (nil? e))
          (is (false? c))
          (is (= :immediate v))
          (is (not (q/cancelled? task)))))))

  (clause :settled-repeatedly "A settled task beats a zero deadline every single time"
    ;; Run inline rather than through `with-task`: the point is that each timeout
    ;; settles synchronously within its own construction, so `getNow` can read the
    ;; outcome without waiting. A regression shows up as `:timed-out` in the tally
    ;; on some fraction of iterations, which one assertion could easily miss.
    (let [outcomes (frequencies
                     (for [_ (range 500)]
                       (call/getNow (q/timeout (q :immediate) 0 :timed-out) ::pending)))]
      (is (= {:immediate 500} outcomes))))

  (clause :fast "A quick task beats a realistic deadline"
    ;; Below some duration, scheduler and JIT jitter make any expectation
    ;; meaningless. 100ms is comfortably above that on CI.
    (let [task (q/sleep 1 :result)]
      (with-task (q/timeout task 100 :timed-out)
        (result [v e c]
          (is (nil? e))
          (is (false? c))
          (is (= :result v)))))))


(def-results with-promise-test
  (clause :success "Timeout with promise - completes before timeout"
    (let [p (q/promise)]
      (q/sleep 1 #(p :value))
      (with-task (q/timeout p 1000)
        (result [v]
          (is (= :value v))))))

  (clause :value "Timeout with promise - times out"
    (let [p (q/promise)]
      (with-task (q/timeout p 1 :timed-out)
        (result [v]
          (is (= :timed-out v))))))

  (clause :function "Timeout with promise - default is function"
    (let [p (q/promise)]
      (with-task (q/timeout p 1 (constantly :computed))
        (result [v]
          (is (= :computed v))))))

  (clause :error "Timeout with promise - default is exception"
    (let [e1 (make-exception "Oh no!")
          p  (q/promise)]
      (with-task (q/timeout p 1 e1)
        (result [_v e c]
          (is (false? c))
          (is (identical? e1 e))))))

  #?(:clj (testing :duration "Using duration"
            (with-task (q/timeout (q/sleep 1 :result) (Duration/ofSeconds 1))
              (result [v]
                (is (= :result v)))))))


(def-results sleep-test
  (clause :value "sleep with value"
    (with-task (q/sleep 1 :done)
      (result [v]
        (is (= :done v)))))

  (clause :function "sleep with function"
    (with-task (q/sleep 1 (constantly :computed))
      (result [v]
        (is (= :computed v)))))

  (clause :error "sleep with exception"
    (let [e1 (make-exception "Oh no!")]
      (with-task (q/sleep 1 e1)
        (result [_v e c]
          (is (false? c))
          (is (identical? e1 e))))))

  (clause :nil "sleep default is nil"
    (with-task (q/sleep 1)
      (result [v]
        (is (nil? v)))))

  #?(:clj (clause :duration "sleep with duration"
            (with-task (q/sleep (Duration/ofNanos 1) :quick-result)
              (result [v]
                (is (= :quick-result v)))))))


(def-results retry-test
  (clause :success "Retry eventually succeeds"
    (let [attempts (atom 0)
          e1       (make-exception "Not yet")
          make-try (fn [_]
                     (q/task
                       (if (< (swap! attempts inc) 3)
                         (throw e1)
                         :success)))]
      (with-task (q/retry make-try
                   {:retries        5
                    :backoff-ms     1
                    :backoff-factor 1})
        (result [v]
          (is (= :success v))))))

  (clause :error "Retry exhausts attempts"
    (let [e1 (make-exception "Always fails")]
      (with-task (q/retry
                   (fn [_] (q/task (throw e1)))
                   {:retries        2
                    :backoff-ms     1
                    :backoff-factor 1})
        (result [_v e c]
          (is (false? c))
          (is (identical? e1 e)))))))


(def-results monitor-test
  (clause :basic "monitor does not cancel slow task"
    (let [task-completed (atom false)
          monitor-fired  (atom false)
          slow-task      (q/sleep 5
                           (fn []
                             (reset! task-completed true)
                             :result))]
      (with-task (q/monitor slow-task 1
                   (fn []
                     (reset! monitor-fired true)
                     :timed-out))
        (result [v]
          (is (= :result v))
          (is (true? @task-completed))
          (is (true? @monitor-fired))))))

  (clause :timeout "monitor side effect doesn't fire on fast task"
    (let [side-effect-ran (atom false)
          fast-task       (q/sleep 1 :result)]
      (with-task (qjoin (q/sleep 10) (q/monitor fast-task 5 (fn [] (reset! side-effect-ran true))))
        (result [v]
          (is (= :result v))
          (is (false? @side-effect-ran))))))

  #?(:clj (clause :duration "monitor with Duration"
            (let [side-effect-ran (atom false)
                  slow-task       (q/sleep 5 :result)]
              (with-task (q/monitor slow-task (Duration/ofMillis 1) (fn [] (reset! side-effect-ran true)))
                (result [v]
                  (is (= :result v))
                  (is (true? @side-effect-ran))))))))


(def-results await-test
  ;; await blocks until a task reaches a specified phase.

  (clause :basic "await returns true when task is quiescent"
    (let [task (q :immediate)]
      (with-task (q/await task)
        (result [v]
          (is (true? v))
          (is (= sm/phase-quiescent (call/getPhase task)))))))

  (clause :ms-success "await with millisecond timeout returns true when settled in time"
    (let [task (q/sleep 5 :result)]
      (with-task (q/await task 500)
        (result [v]
          (is (true? v) "Should return true when settled before timeout")))))

  (clause :ms-fail "await with millisecond timeout returns false when timed out"
    (let [task (q/sleep 1000 :result)]
      (with-task (q/await task 1)
        (result [v]
          (is (false? v) "Should return false when timeout expires")
          (q/cancel task)))))

  #?(:clj (clause :duration "await can have Duration timeout"
            (let [task (q/sleep 5 :result)]
              (with-task (q/await task (Duration/ofMillis 500))
                (result [v]
                  (is (true? v) "Should return true when settled before Duration timeout")
                  (q/cancel task))))))

  (clause :cancelling "cancelling await cancels task"
    (let [task   (q/sleep 1000 :success)
          waiter (q/await task 1000)]
      (with-task (q/cancel waiter)
        (result []
          (is (q/cancelled? task))
          (is (q/cancelled? waiter)))))))


(def-results time-test
  (clause :basic "Milliseconds are measured and passed to side-effect"
    (let [res (q/promise)
          dur (q/promise)]
      (-> (q/sleep 5 :done)
        (q/time
          (fn [v e c t]
            (res [v e c])
            (dur t))))
      (with-task [res dur]
        (result [[r d]]
          (is (= [:done nil false] r))
          (is (number? d))
          (is (<= 0 d))))))

  (clause :start-ms "Custom start-ms is respected"
    (let [dur                 (q/promise)
          early-start #?(:clj (System/currentTimeMillis) :cljs (js/performance.now))]
      (-> (q/task :done)
        (q/time early-start
          (fn [_ _ _ t]
            (dur t))))
      (with-task dur
        (result [v]
          (is (number? v))
          (is (<= 0 v)))))))
