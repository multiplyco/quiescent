(ns co.multiply.quiescent.gate-test
  (:require
    [clojure.test :refer [is]]
    [co.multiply.quiescent :as q :refer [q qdo]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park clause def-results make-exception result with-task]]))


(allow-platform-park)


;; # Basic Gate Behavior
;; ################################################################################

(def-results gate-basic
  (clause :single-task "Single task through gate"
    (let [g (q/gate 2)]
      (with-task (q/gate-task g 42)
        (result [v]
          (is (= 42 v))))))

  (clause :multiple-tasks "Multiple tasks complete through gate"
    (let [g (q/gate 2)]
      (with-task (q [(q/gate-task g 1)
                     (q/gate-task g 2)
                     (q/gate-task g 3)])
        (result [v]
          (is (= [1 2 3] v))))))

  (clause :more-permits-than-tasks "More permits than tasks (no waiting)"
    (let [g (q/gate 10)]
      (with-task (q [(q/gate-task g :a)
                     (q/gate-task g :b)
                     (q/gate-task g :c)])
        (result [v]
          (is (= [:a :b :c] v)))))))


;; # Concurrency Limiting
;; ################################################################################

(def-results gate-concurrency
  (clause :limits-concurrent "Gate limits concurrent execution"
    (let [g          (q/gate 2)
          concurrent (atom 0)
          max-seen   (atom 0)
          make-task  (fn [_]
                       (q/gate-task g
                         (let [n (swap! concurrent inc)]
                           (swap! max-seen max n)
                           (q/sleep 1
                             (fn []
                               (swap! concurrent dec)
                               :done)))))]
      (with-task (q (mapv make-task (range 6)))
        (result [v]
          (is (= (repeat 6 :done) v))
          (is (<= @max-seen 2) "Should never exceed 2 concurrent")))))

  (clause :serial-with-one-permit "Gate with 1 permit runs tasks serially"
    (let [g          (q/gate 1)
          concurrent (atom 0)
          max-seen   (atom 0)
          make-task  (fn [i]
                       (q/gate-task g
                         (let [n (swap! concurrent inc)]
                           (swap! max-seen max n)
                           (q/sleep 1
                             (fn []
                               (swap! concurrent dec)
                               i)))))]
      (with-task (q (mapv make-task (range 4)))
        (result [v]
          (is (= [0 1 2 3] v))
          (is (= 1 @max-seen) "Should never exceed 1 concurrent"))))))


;; # Error Handling
;; ################################################################################

(def-results gate-errors
  (clause :error-releases-permit "Task error releases permit for next task"
    (let [g         (q/gate 1)
          completed (atom [])
          _task1    (q/gate-task g (throw (make-exception "fail")))
          task2     (q/gate-task g
                      (swap! completed conj :task2)
                      :success)]
      (with-task task2
        (result [v]
          (is (= :success v))
          (is (= [:task2] @completed) "Task2 should run after task1 fails")))))

  (clause :errors-propagate "Errors propagate from gated tasks"
    (let [g (q/gate 2)]
      (with-task (q/gate-task g (throw (make-exception "test error")))
        (result [_v e]
          (is (some? e)))))))


;; # Cancellation
;; ################################################################################

(def-results gate-cancellation
  (clause :gate-cancel-cancels-tasks "Cancelling gate cancels gated tasks"
    (let [g (q/gate 2)]
      (with-task (qdo (q/sleep 20 #(q/cancel g))
                   (q/gate-task g (q/sleep 10000 :should-not-complete)))
        (result [_v _e c]
          (is (true? c) "Gated task should be cancelled")))))

  (clause :task-cancel-releases-permit "Cancelled task releases permit"
    (let [g         (q/gate 1)
          task1     (q/gate-task g
                      (q/sleep 10000 :should-not-complete))
          completed (atom false)
          task2     (q/gate-task g
                      (reset! completed true)
                      :task2-done)]
      (with-task (q/then (q/sleep 20)
                   (fn [_]
                     (q/then (q/cancel task1)
                       (fn [_] task2))))
        (result [v]
          (is (= :task2-done v))
          (is (true? @completed) "Task2 should run after task1 cancelled")))))

  (clause :enqueue-after-cancel "Enqueueing after gate cancelled returns cancelled task"
    (let [g (doto (q/gate 2) (q/cancel))]
      (with-task (q/gate-task g :should-not-run)
        (result [_v _e c]
          (is (true? c) "Task should be cancelled"))))))


;; # Chaining
;; ################################################################################

(def-results gate-chaining
  (clause :then-chain "Can chain on gated tasks"
    (let [g (q/gate 2)]
      (with-task (-> (q/gate-task g 10)
                   (q/then (fn [v] (* v 2))))
        (result [v]
          (is (= 20 v))))))

  (clause :nested-gated-tasks "Nested gated tasks work"
    (let [g (q/gate 1)]
      (with-task (q/gate-task g (q/gate-task g 42))
        (result [v]
          (is (= 42 v)))))))
