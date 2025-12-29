(ns co.multiply.quiescent.promise-test
  "Tests for promise operations: creation, delivery, failure, cancellation,
   and related functionality including failed-task."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]])
  (:import
    [java.util.concurrent CancellationException]))


(use-fixtures :once platform-thread-fixture)


(deftest promise-test
  (testing "Promise can be delivered"
    (let [p (q/promise)]
      (future (Thread/sleep 10) (p 42))
      (is (= 42 @p))))

  (testing "Promise used in qlet"
    (let [p (q/promise)]
      (future (Thread/sleep 50) (p 100))
      (is (= 110
            @(q/qlet [v      p
                      result (+ v 10)]
               result))))))


(deftest promise-fail-test
  (testing "Promise can be failed with exception"
    (let [p (q/promise)
          e (Exception. "Failed!")]
      (future (Thread/sleep 10) (q/fail p e))
      (is (thrown? Exception @p))
      (try
        @p
        (is false "Should have thrown")
        (catch Exception ex
          (is (identical? e ex))))))

  (testing "fail can be used to bridge callback APIs"
    (let [p              (q/promise)
          callback-error (Exception. "API error")
          simulated-api  (fn [callbacks]
                           (future
                             (Thread/sleep 10)
                             ((:on-error callbacks) callback-error)))]
      (simulated-api {:on-success (fn [result] (p result))
                      :on-error   (fn [error] (q/fail p error))})
      (is (thrown? Exception @p))
      (try
        @p
        (is false "Should have thrown")
        (catch Exception e
          (is (identical? callback-error e))))))

  (testing "fail on already completed promise is no-op"
    (let [p (q/promise)]
      (p :success)
      (q/fail p (Exception. "Too late"))
      ;; First completion wins
      (is (= :success @p))))

  (testing "Promise can be caught after fail"
    (let [p (q/promise)]
      (q/fail p (Exception. "Error"))
      (is (= :recovered
            @(q/catch p (fn [_] :recovered)))))))


(deftest promise-as-function-test
  (testing "Promise can be invoked as function to deliver value"
    (let [p (q/promise)]
      (p 42)
      (is (= 42 @p))))

  (testing "Promise invoke in another thread"
    (let [p (q/promise)]
      (future (Thread/sleep 10) (p :delivered))
      (is (= :delivered @p))))

  (testing "Promise invoke returns the promise"
    (let [p (q/promise)]
      (is (identical? p (p :value)))))

  (testing "Promise can only be delivered once"
    (let [p (q/promise)]
      (p :first)
      (p :second)
      ;; First value wins
      (is (= :first @p)))))


(deftest promise-cancel-throws-test
  ;; Direct cancellation of promises is not supported - they are externally controlled.

  (testing "cancel on Promise throws UnsupportedOperationException"
    (let [p (q/promise)]
      (is (thrown? UnsupportedOperationException (q/cancel p))
        "Cancelling a Promise should throw UnsupportedOperationException")))

  (testing "Promise can still be failed with fail"
    (let [p (q/promise)
          e (Exception. "Expected error")]
      (q/fail p e)
      (is (thrown? Exception @p))
      (try
        @p
        (is false "Should have thrown")
        (catch Exception ex
          (is (identical? e ex))))))

  (testing "Cancelling outer task tears down Promise waiting inside"
    ;; When a Promise is awaited inside a task and the outer task is cancelled,
    ;; the Promise should see the cancellation via its underlying task being cancelled.
    (let [p     (q/promise)
          outer (q/task @p)]
      (Thread/sleep 20)
      @(q/cancel outer)
      ;; Outer should be cancelled
      (is (thrown? CancellationException @outer)))))


(deftest failed-task-test
  (testing "failed-task creates pre-failed task"
    (let [e    (Exception. "Already failed")
          task (q/failed-task e)]
      (is (realized? task))
      (is (thrown? Exception @task))
      (try
        @task
        (is false "Should have thrown")
        (catch Exception ex
          (is (identical? e ex))))))

  (testing "failed-task can be caught"
    (is (= :recovered
          @(q/catch (q/failed-task (Exception. "Error"))
                   (fn [_] :recovered)))))

  (testing "failed-task useful for conditional logic"
    (let [fetch-data (fn [valid?]
                       (if valid?
                         (q/task :data)
                         (q/failed-task (ex-info "Invalid" {:code 400}))))]
      (is (= :data @(fetch-data true)))
      (is (thrown-with-msg? Exception #"Invalid"
            @(fetch-data false))))))
