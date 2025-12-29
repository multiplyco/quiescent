(ns co.multiply.quiescent.basic-test
  "Tests for core task functionality: ground, flattening, cpu-task,
   reusability, and subscription management."
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]]
    [co.multiply.scoped :refer [scoping]])
  (:import
    [co.multiply.quiescent.impl TeardownSubscription]))


(use-fixtures :once platform-thread-fixture)


;; ## Ground function tests (core orchestration)
(deftest ground-nested-structures-test
  (testing "Tasks in maps"
    (is (= {:a 1 :b 2}
          @(q/q {:a (q/task 1) :b (q/task 2)}))))

  (testing "Tasks in vectors"
    (is (= [1 2 3]
          @(q/q [(q/task 1) (q/task 2) (q/task 3)]))))

  (testing "Tasks in sets"
    (is (= #{1 2 3}
          @(q/q #{(q/task 1) (q/task 2) (q/task 3)}))))

  (testing "Deeply nested tasks"
    (is (= {:users [{:id 1 :posts [10 20]}
                    {:id 2 :posts [30]}]}
          @(q/q {:users [(q/task {:id 1 :posts [(q/task 10) (q/task 20)]})
                         (q/task {:id 2 :posts [(q/task 30)]})]}))))

  (testing "Single task in structure (optimization path)"
    (is (= {:only 42}
          @(q/q {:only (q/task 42)}))))

  (testing "Empty structure with no tasks"
    (is (= {:a 1 :b 2}
          @(q/q {:a 1 :b 2})))))


(deftest task-flattening-test
  (testing "Nested tasks are flattened"
    (is (= 42 @(q/task (q/task (q/task 42))))))

  (testing "Task returning task from then chain"
    (is (= 42 @(q/then (q/task 40)
                 (fn [v] (q/task (+ v 2)))))))

  (testing "Deeply nested task in structure"
    (is (= {:value 42}
          @(q/task {:value (q/task (q/task 42))})))))


(deftest task-reusability-test
  (testing "Task can be dereferenced multiple times"
    (let [task (q/task 42)]
      (is (= 42 @task))
      (is (= 42 @task))
      (is (= 42 @task))))

  (testing "Task can be chained from multiple times"
    (let [task (q/task 10)
          t1   (q/then task inc)
          t2   (q/then task (partial * 2))
          t3   (q/then task str)]
      (is (= 11 @t1))
      (is (= 20 @t2))
      (is (= "10" @t3))))

  (testing "Multiple chains can depend on same upstream task"
    (let [upstream (q/task (Thread/sleep 50) 10)
          branch1  (q/then upstream inc)
          branch2  (q/then upstream (partial * 2))
          combined (q/then branch1 branch2 +)]
      ;; Should only run upstream once, not twice
      (is (= 31 @combined)))))                              ; (10+1) + (10*2) = 11 + 20 = 31


(deftest cpu-task-test
  ;; These tests verify that cpu-task and task work correctly without making
  ;; assumptions about specific thread naming (since cpu-executor may be
  ;; aliased to virtual-executor to avoid blocking issues).

  (testing "cpu-task executes and returns value"
    (is (= 42 @(q/cpu-task 42))))

  (testing "cpu-task executes expression"
    (let [result (atom nil)]
      @(q/cpu-task (reset! result :executed))
      (is (= :executed @result))))

  (testing "task executes and returns value"
    (is (= 42 @(q/task 42))))

  (testing "task executes expression"
    (let [result (atom nil)]
      @(q/task (reset! result :executed))
      (is (= :executed @result))))

  (testing "task runs on virtual thread executor"
    (let [thread-name (atom nil)]
      @(q/task
         (reset! thread-name (.getName (Thread/currentThread))))
      (is (str/includes? @thread-name "q-io")))))


(defn- teardown-subscriptions
  "Filter subscriptions to only TeardownSubscription (parent-child cascade).
   Internal subscriptions like CallbackSubscription are excluded."
  [subs]
  (filter #(instance? TeardownSubscription %) subs))


(deftest child-unsubscribes-from-parent-test
  ;; These tests verify that children unsubscribe from parents when they settle,
  ;; preventing memory leaks in long-running loops. We keep the parent alive
  ;; (blocked on a promise) to ensure we're testing child-initiated cleanup,
  ;; not parent-settling cleanup.
  ;;
  ;; We filter for TeardownSubscription specifically, as tasks may have internal
  ;; CallbackSubscriptions (e.g., thread runner teardown) that are unrelated to
  ;; parent-child relationships.

  (testing "Child unsubscribes from parent when it settles (while parent still running)"
    (scoping [impl/*this* nil]
      (let [p      (promise)
            parent (q/task
                     @(q/task :child-result)
                     (deliver p true)
                     @(promise))                            ; Keep parent alive
            subs   (do @p (teardown-subscriptions (:subscriptions (q/get-state parent))))]
        (q/cancel parent)
        (is (empty? subs)
          "Parent should have no TeardownSubscriptions after child settles"))))

  (testing "Multiple children all unsubscribe when they settle (while parent still running)"
    (scoping [impl/*this* nil]
      (let [p      (promise)
            parent (q/task
                     @(q/task :one)
                     @(q/task :two)
                     @(q/task :three)
                     (deliver p true)
                     @(promise))                            ; Keep parent alive
            subs   (do @p (teardown-subscriptions (:subscriptions (q/get-state parent))))]
        (q/cancel parent)
        (is (empty? subs)
          "Parent should have no TeardownSubscriptions after all children settle"))))

  (testing "Children in long-running loop don't accumulate subscriptions"
    (scoping [impl/*this* nil]
      (let [p      (promise)
            parent (q/task
                     (dotimes [_ 100]
                       @(q/task :iteration))
                     (deliver p true)
                     @(promise))                            ; Keep parent alive
            subs   (do @p (teardown-subscriptions (:subscriptions (q/get-state parent))))]
        (q/cancel parent)
        (is (empty? subs)
          "Parent should have no TeardownSubscriptions after loop completes")))))
