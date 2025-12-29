(ns co.multiply.quiescent.timeout-test
  "Tests for timeout, monitor, sleep, await, and retry operations."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]])
  (:import
    [java.time Duration]
    [java.util.concurrent CancellationException TimeoutException]))


(use-fixtures :once platform-thread-fixture)


(deftest timeout-test
  (testing "Timeout on slow task"
    (is (thrown? TimeoutException
          @(q/timeout (q/task (Thread/sleep 1000) :result) 50))))

  (testing "Timeout with default value"
    (is (= :timed-out
          @(q/timeout (q/task (Thread/sleep 1000) :result) 50 :timed-out))))

  (testing "Fast task completes before timeout"
    (is (= :result
          @(q/timeout (q/task (Thread/sleep 10) :result) 1000)))))


(deftest timeout-with-promise-test
  (testing "Timeout with promise - completes before timeout"
    (let [p (q/promise)]
      (future (Thread/sleep 10) (p :value))
      (is (= :value @(q/timeout p 1000)))))

  (testing "Timeout with promise - times out"
    (let [p (q/promise)]
      (is (= :timed-out @(q/timeout p 50 :timed-out)))))

  (testing "Timeout with promise - default is function"
    (let [p (q/promise)]
      (is (= :computed @(q/timeout p 50 (fn [] :computed))))))

  (testing "Timeout with promise - default is exception"
    (let [p (q/promise)]
      (is (thrown-with-msg? IllegalStateException #"Custom timeout"
            @(q/timeout p 50 (IllegalStateException. "Custom timeout")))))))


(deftest timeout-duration-test
  (testing "Timeout with Duration on slow task"
    (is (thrown? TimeoutException
          @(q/timeout (q/task (Thread/sleep 1000) :result) (Duration/ofMillis 50)))))

  (testing "Timeout with Duration and default value"
    (is (= :timed-out
          @(q/timeout (q/task (Thread/sleep 1000) :result) (Duration/ofMillis 50) :timed-out))))

  (testing "Fast task completes before Duration timeout"
    (is (= :result
          @(q/timeout (q/task (Thread/sleep 10) :result) (Duration/ofSeconds 1)))))

  (testing "Timeout with Duration and function default"
    (let [executed (atom false)]
      (is (= :result
            @(q/timeout (q/task (Thread/sleep 1000) :too-slow)
               (Duration/ofMillis 50)
               (fn []
                 (reset! executed true)
                 :result))))
      (is @executed)))

  (testing "Timeout with Duration and exception default"
    (is (thrown-with-msg? IllegalStateException #"Too slow"
          @(q/timeout (q/task (Thread/sleep 1000) :result)
             (Duration/ofMillis 50)
             (IllegalStateException. "Too slow"))))))


(deftest timeout-advanced-test
  (testing "Timeout with function default executes function"
    (let [executed (atom false)]
      (is (= :result
            @(q/timeout (q/task (Thread/sleep 1000) :too-slow)
               50
               (fn []
                 (reset! executed true)
                 :result))))
      (is @executed)))

  (testing "Timeout with exception default throws"
    (is (thrown-with-msg? IllegalStateException #"Too slow"
          @(q/timeout (q/task (Thread/sleep 1000) :result)
             50
             (IllegalStateException. "Too slow"))))))


(deftest sleep-test
  (testing "sleep with value"
    (is (= :done @(q/sleep 10 :done))))

  (testing "sleep with function"
    (is (= :computed @(q/sleep 10 (fn [] :computed)))))

  (testing "sleep with exception"
    (is (thrown-with-msg? IllegalStateException #"Sleep failed"
          @(q/sleep 10 (IllegalStateException. "Sleep failed")))))

  (testing "sleep default is nil"
    (is (nil? @(q/sleep 10)))))


(deftest sleep-duration-test
  (testing "sleep with Duration object"
    (is (= :done @(q/sleep (Duration/ofMillis 10) :done))))

  (testing "sleep with Duration and function"
    (is (= :computed @(q/sleep (Duration/ofMillis 10) (fn [] :computed)))))

  (testing "sleep with Duration default is nil"
    (is (nil? @(q/sleep (Duration/ofMillis 10)))))

  (testing "sleep with Duration actually waits"
    (let [start-time (System/currentTimeMillis)]
      @(q/sleep (Duration/ofMillis 20) :done)
      (let [elapsed (- (System/currentTimeMillis) start-time)]
        (is (>= elapsed 15) "Should wait at least ~20ms")))))


(deftest retry-test
  (testing "Retry eventually succeeds"
    (let [attempts (atom 0)]
      (is (= :success
            @(q/retry
               (fn [_]
                 (q/task
                   (if (< (swap! attempts inc) 3)
                     (throw (Exception. "Not yet"))
                     :success)))
               {:retries        5
                :backoff-ms     10
                :backoff-factor 1})))))

  (testing "Retry exhausts attempts"
    (is (thrown? Exception
          @(q/retry
             (fn [_] (q/task (throw (Exception. "Always fails"))))
             {:retries        2
              :backoff-ms     10
              :backoff-factor 1})))))


(deftest monitor-test
  (testing "monitor does not cancel slow task"
    (let [task-completed (atom false)
          slow-task      (q/task
                           (Thread/sleep 100)
                           (reset! task-completed true)
                           :result)
          monitored      (q/monitor slow-task 20 (fn [] :timed-out))]
      ;; Original task should complete despite monitor timeout
      (is (= :result @monitored))
      (is @task-completed)))

  (testing "monitor side effect fires on slow task"
    (let [side-effect-ran (atom false)
          slow-task       (q/task (Thread/sleep 100) :result)]
      (q/monitor slow-task 20 (fn [] (reset! side-effect-ran true)))
      @slow-task
      (Thread/sleep 50)                                     ; Give time for side effect
      (is @side-effect-ran)))

  (testing "monitor side effect doesn't fire on fast task"
    (let [side-effect-ran (atom false)
          fast-task       (q/task (Thread/sleep 10) :result)]
      (q/monitor fast-task 50 (fn [] (reset! side-effect-ran true)))
      @fast-task
      (Thread/sleep 20)
      (is (false? @side-effect-ran))))

  (testing "monitor with Duration"
    (let [side-effect-ran (atom false)
          slow-task       (q/task (Thread/sleep 100) :result)]
      (q/monitor slow-task (Duration/ofMillis 20) (fn [] (reset! side-effect-ran true)))
      @slow-task
      (Thread/sleep 50)
      (is @side-effect-ran)))

  (testing "monitor returns original task unchanged"
    (let [task      (q/task 42)
          monitored (q/monitor task 1000 (fn [] :ignored))]
      (is (= 42 @monitored))))

  (testing "monitor allows external cancellation of original task"
    (let [task      (q/task (Thread/sleep 10000) :result)
          monitored (q/monitor task 50 (fn [] :timed-out))]
      ;; Cancel the original task
      (q/cancel task)
      ;; Both should see cancellation
      (is (thrown? CancellationException @task))
      (is (thrown? CancellationException @monitored)))))


(deftest await-test
  ;; await blocks until a task reaches a specified phase.

  (testing "await returns true when phase is already reached"
    (let [task (q/task :immediate)]
      @task                                                 ; Complete the task
      (is (true? (q/await task :quiescent)))))

  (testing "await blocks until phase is reached"
    (let [task          (q/task (Thread/sleep 50) :result)
          phase-reached (promise)
          _             (future
                          (Thread/sleep 10)
                          (q/await task :settling)
                          (deliver phase-reached true))]
      ;; Phase should be reached after task completes
      (is (true? (deref phase-reached 200 false)) "await should block until settling phase")))

  (testing "await with millisecond timeout returns true when phase reached"
    (let [task (q/task (Thread/sleep 20) :result)]
      (is (true? (q/await task :settling 500)) "Should return true when phase reached before timeout")))

  (testing "await with millisecond timeout returns false when timed out"
    (let [task (q/task (Thread/sleep 5000) :result)]
      (is (false? (q/await task :settling 50)) "Should return false when timeout expires")))

  (testing "await with Duration timeout returns true when phase reached"
    (let [task (q/task (Thread/sleep 20) :result)]
      (is (true? (q/await task :settling (Duration/ofMillis 500)))
        "Should return true when phase reached before Duration timeout")))

  (testing "await with Duration timeout returns false when timed out"
    (let [task (q/task (Thread/sleep 5000) :result)]
      (is (false? (q/await task :settling (Duration/ofMillis 50)))
        "Should return false when Duration timeout expires")))

  (testing "await for :quiescent waits for complete teardown"
    (let [cleanup-done   (promise)
          await-returned (promise)
          task           (-> (q/task (Thread/sleep 10000) :result)
                           (q/finally
                             (fn [& _]
                               (Thread/sleep 50)
                               (deliver cleanup-done true))))]
      (Thread/sleep 10)
      (q/cancel task)                                       ; Can cancel the link directly now
      ;; Run await in a future
      (future
        (q/await task :quiescent)
        (deliver await-returned true))
      ;; Wait for await to return
      (is (true? (deref await-returned 500 false)) "await should return when quiescent")
      ;; By the time await returns, cleanup should be complete
      (is (true? (deref cleanup-done 100 false)) "Cleanup should be complete after await :quiescent")))

  (testing "await for earlier phase returns immediately if already past"
    (let [task (q/task :immediate)]
      @task
      ;; Task is quiescent, so :settling should return immediately
      (let [start (System/currentTimeMillis)]
        (q/await task :settling)
        (is (< (- (System/currentTimeMillis) start) 50) "Should return immediately for past phase")))))
