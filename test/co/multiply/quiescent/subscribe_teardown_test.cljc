(ns co.multiply.quiescent.subscribe-teardown-test
  (:require
    [clojure.test :refer [deftest is testing #?(:cljs async)]]
    [co.multiply.quiescent :as q :refer [q]]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.test-support :refer [allow-platform-park clause def-results result with-task make-exception]]
    [co.multiply.scoped :refer [ask]]))


(def-results subscribe-teardown-task-already-settling-test
  (clause :task-settling "Case 1: task is already settling - target is torn down immediately"
    (let [task   (q :done)
          target (q/sleep 5000 :target-result)]
      (is (not (realized? target)))
      (subs/subscribe-teardown task target)
      (with-task target
        (result [_v e c]
          (is (true? c))
          (is (some? e))))))

  (clause :target-settling "Case 2: target is already settling - no-op"
    (let [task   (q/sleep 5000 :task-result)
          target (q :done)]
      (is (not (realized? task)))
      ;; subscribe-teardown should be a no-op (returns nil)
      (is (nil? (subs/subscribe-teardown task target)))
      (with-task target
        (result [v _e c]
          (is (false? c))
          (is (= :done v))
          ;; Clean up
          (q/cancel task)))))

  (clause :task-first "Case 3a: task settles first - target is torn down"
    (let [task   (q/sleep 10 :task-done)
          target (q/sleep 5000 :target-result)]
      (subs/subscribe-teardown task target)
      ;; Neither should be settled yet
      (is (not (realized? task)))
      (is (not (realized? target)))
      (with-task (q/await task)
        (result []
          (is (q/cancelled? target))))))

  (clause :target "Case 3b: target settles first - target completes normally, not cancelled"
    (let [task   (q/sleep 5000 :task-result)
          target (q/sleep 10 :target-done)]
      (subs/subscribe-teardown task target)
      ;; Neither should be settled yet
      (is (not (realized? task)))
      (is (not (realized? target)))
      (with-task (q/await target)
        (result []
          (is (realized? target))
          (is (not (q/cancelled? target)))
          (q/cancel task))))))
