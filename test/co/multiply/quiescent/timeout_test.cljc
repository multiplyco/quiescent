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
