(ns co.multiply.quiescent.subscribe-teardown-test
  "Tests for the subscribe-teardown mutual cleanup pattern."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]]))


(use-fixtures :once platform-thread-fixture)


(deftest subscribe-teardown-task-already-settling-test
  (testing "Case 1: task is already settling - target is torn down immediately"
    (let [task   (q/task :done)
          target (q/task (Thread/sleep 5000) :target-result)]
      ;; Wait for task to settle
      @task
      ;; Target should still be running
      (is (not (realized? target)))

      ;; Now subscribe-teardown: task is settling, so teardown runs immediately
      (impl/subscribe-teardown task target)

      ;; Target should now be cancelled
      (is (q/cancelled? target)))))


(deftest subscribe-teardown-target-already-settling-test
  (testing "Case 2: target is already settling - no-op"
    (let [task   (q/task (Thread/sleep 5000) :task-result)
          target (q/task :done)]
      ;; Wait for target to settle
      @target
      ;; Task should still be running
      (is (not (realized? task)))

      ;; subscribe-teardown should be a no-op (returns nil)
      (is (nil? (impl/subscribe-teardown task target)))

      ;; Target completed normally, not cancelled
      (is (not (q/cancelled? target)))
      (is (= :done @target))

      ;; Clean up
      @(q/cancel task))))


(deftest subscribe-teardown-neither-settling-test
  (testing "Case 3a: task settles first - target is torn down"
    (let [task   (q/task (Thread/sleep 50) :task-done)
          target (q/task (Thread/sleep 5000) :target-result)]
      ;; Neither should be settled yet
      (is (not (realized? task)))
      (is (not (realized? target)))

      ;; Set up mutual subscriptions
      (impl/subscribe-teardown task target)

      ;; Wait for task to complete - this should tear down target
      @task

      ;; Target should be cancelled
      (is (q/cancelled? target))))

  (testing "Case 3b: target settles first - target completes normally, not cancelled"
    (let [task   (q/task (Thread/sleep 5000) :task-result)
          target (q/task (Thread/sleep 50) :target-done)]
      ;; Neither should be settled yet
      (is (not (realized? task)))
      (is (not (realized? target)))

      ;; Set up mutual subscriptions
      (impl/subscribe-teardown task target)

      ;; Wait for target to complete normally
      (is (= :target-done @target))

      ;; Target should NOT be cancelled - it settled first
      (is (not (q/cancelled? target)))

      ;; Clean up
      @(q/cancel task))))
