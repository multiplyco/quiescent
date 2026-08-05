(ns co.multiply.quiescent.basic-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing]]
    [co.multiply.quiescent :as q :refer [q]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park clause def-results result with-task make-exception]]
    [co.multiply.quiescent.type.call :as call])
  #?(:clj (:import
            [co.multiply.quiescent.impl.subscription TeardownSubscription])))


(allow-platform-park)


;; # Tasks in Collections
;; ################################################################################

(def-results tasks-in-collections
  (clause :maps "Tasks in maps"
    (with-task (q {:a (q/task 1) :b (q/task 2)})
      (result [v]
        (is (= {:a 1 :b 2} v)))))

  (clause :vectors "Tasks in vectors"
    (with-task (q [(q/task 1) (q/task 2) (q/task 3)])
      (result [v]
        (is (= [1 2 3] v)))))

  (clause :sets "Tasks in sets"
    (with-task (q #{(q/task 1) (q/task 2) (q/task 3)})
      (result [v]
        (is (= #{1 2 3} v)))))

  (clause :deeply-nested "Deeply nested tasks"
    (with-task (q {:users [(q/task {:id 1 :posts [(q/task 10) (q/task 20)]})
                           (q/task {:id 2 :posts [(q/task 30)]})]})
      (result [v]
        (is (= {:users [{:id 1 :posts [10 20]}
                        {:id 2 :posts [30]}]} v)))))

  (clause :single-task "Single task in structure (optimization path)"
    (with-task (q {:only (q/task 42)})
      (result [v]
        (is (= {:only 42} v)))))

  (clause :plain-structure "Plain structure with no tasks"
    (with-task (q {:a 1 :b 2})
      (result [v]
        (is (= {:a 1 :b 2} v))))))


;; # Task Flattening
;; ################################################################################

(def-results task-flattening
  (clause :nested-tasks "Nested tasks are flattened"
    (with-task (q/task (q/task (q/task 42)))
      (result [v]
        (is (= 42 v)))))

  (clause :then-chain "Task returning task from then chain"
    (with-task (q/then (q/task 40)
                 (fn [v] (q/task (+ v 2))))
      (result [v]
        (is (= 42 v)))))

  (clause :nested-in-structure "Deeply nested task in structure"
    (with-task (q/task {:value (q/task (q/task 42))})
      (result [v]
        (is (= {:value 42} v))))))


;; # Task Reuse
;; ################################################################################

(def-results task-reuse
  (clause :multiple-uses "Task output can be used multiple times"
    (with-task (let [t (q/task 42)]
                 (q [t t t]))
      (result [v]
        (is (= [42 42 42] v)))))

  (clause :multiple-chains "Task can be chained from multiple times"
    (with-task (let [task (q/task 10)]
                 (q [(q/then task inc)
                     (q/then task (partial * 2))
                     (q/then task str)]))
      (result [v]
        (is (= [11 20 "10"] v)))))

  (clause :same-upstream "Multiple chains can depend on same upstream task"
    (with-task (let [upstream (q/sleep 10 10)
                     branch1  (q/then upstream inc)
                     branch2  (q/then upstream (partial * 2))]
                 (q/then branch1 branch2 +))
      (result [v]
        (is (= 31 v))))))


;; # CLJ-Only Tests
;; ################################################################################

#?(:clj (deftest task-deref-reuse-test
          (testing "Task can be dereferenced multiple times"
            (let [task (q/task 42)]
              (is (= 42 @task))
              (is (= 42 @task))
              (is (= 42 @task))))))


#?(:clj (deftest cpu-virtual-test
          ;; These tests verify that cpu-task and task work correctly without making
          ;; assumptions about specific thread naming (since cpu-executor may be
          ;; aliased to virtual-executor to avoid blocking issues).

          (testing "cpu-task executes and returns value"
            (is (= 42 @(q/cpu-task 42)))
            (is (= 42 @(q/task 42))))

          (testing "cpu-task executes expression"
            (let [result (promise)]
              (q/cpu-task (deliver result :executed))
              (is (= :executed @result))))

          (testing "task executes expression"
            (let [result (promise)]
              (q/task (deliver result :executed))
              (is (= :executed @result))))

          (testing "task runs on virtual thread executor"
            (let [thread-name (promise)]
              (q/task (deliver thread-name (.getName (Thread/currentThread))))
              (is (str/includes? @thread-name "q-io"))))))


#?(:clj (defn- teardown-subscriptions
          "Filter subscriptions to only TeardownSubscription (parent-child cascade).
           Internal subscriptions like CallbackSubscription are excluded."
          [subs]
          (filter #(instance? TeardownSubscription %) subs)))


#?(:clj (deftest child-unsubscribes-from-parent-test
          ;; These tests verify that children unsubscribe from parents when they settle,
          ;; preventing memory leaks in long-running loops. We keep the parent alive
          ;; (blocked on a promise) to ensure we're testing child-initiated cleanup,
          ;; not parent-settling cleanup.
          ;;
          ;; We filter for TeardownSubscription specifically, as tasks may have internal
          ;; CallbackSubscriptions (e.g., thread runner teardown) that are unrelated to
          ;; parent-child relationships.

          (testing "Child unsubscribes from parent when it settles (while parent still running)"
            (let [p      (promise)
                  parent (q/task
                           @(q/task :child-result)
                           (deliver p true)
                           @(promise))                      ; Keep parent alive
                  subs   (do @p (teardown-subscriptions (call/getSubscriptions parent)))]
              @(q/cancel parent)
              (is (empty? subs)
                "Parent should have no TeardownSubscriptions after child settles")))

          (testing "Multiple children all unsubscribe when they settle (while parent still running)"
            (let [p      (promise)
                  parent (q/task
                           @(q/task :one)
                           @(q/task :two)
                           @(q/task :three)
                           (deliver p true)
                           @(promise))                      ; Keep parent alive
                  subs   (do @p (teardown-subscriptions (call/getSubscriptions parent)))]
              @(q/cancel parent)
              (is (empty? subs)
                "Parent should have no TeardownSubscriptions after all children settle")))

          (testing "Children in long-running loop don't accumulate subscriptions"
            (let [p      (promise)
                  parent (q/task
                           (dotimes [_ 100]
                             @(q/task :iteration))
                           (deliver p true)
                           @(promise))                      ; Keep parent alive
                  subs   (do @p (teardown-subscriptions (call/getSubscriptions parent)))]
              @(q/cancel parent)
              (is (empty? subs)
                "Parent should have no TeardownSubscriptions after loop completes")))))


#?(:clj (deftest blocking-deref-timeout-test
          (testing "Blocking deref with timeout returns default"
            (let [task (q/task (Thread/sleep 1000) :result)]
              ;; Should timeout and return default
              (is (= :timed-out (deref task 50 :timed-out)))))

          (testing "Blocking deref completes before timeout"
            (let [task (q/task (Thread/sleep 10) :result)]
              ;; Should complete successfully
              (is (= :result (deref task 1000 :timed-out)))))

          (testing "Blocking deref respects thread interruption"
            (let [task         (q/task (Thread/sleep 10000) :result)
                  worker-error (atom nil)
                  worker       (Thread.
                                 (fn []
                                   (try
                                     (deref task 20000 :default)
                                     (catch Throwable e
                                       (reset! worker-error e)))))]
              (.start worker)
              (Thread/sleep 10)
              (.interrupt worker)
              (.join worker 1000)
              (is (instance? InterruptedException @worker-error))))))


(defn get-time-now
  []
  #?(:clj  (System/currentTimeMillis)
     :cljs (js/performance.now)))


(def-results get-now-test
  (clause :returns-default "get-now returns default for incomplete task"
    (is (= :not-ready (q/get-now (q/sleep 1000 :result) :not-ready))))

  (clause :returns-completed "get-now returns value for completed task"
    (let [task (q 42)]
      (with-task task
        (result []
          (is (= 42 (q/get-now task :default)))))))

  (clause :throws "get-now throws for exceptionally completed task"
    (let [task (q/task (throw (make-exception "Error")))]
      (with-task task
        (result [_v e c]
          (is (some? (try
                       (q/get-now task :default)
                       nil
                       (catch #?(:clj Throwable :cljs :default) e e))))
          (is (false? c))
          (is (some? e))))))


  (clause :no-blocking "get-now is non-blocking"
    (let [task       (q/sleep 100 :result)
          start-time (get-time-now)
          result     (q/get-now task :not-ready)
          elapsed    (- (get-time-now) start-time)]
      (is (= :not-ready result))
      (is (< elapsed 50) "Should return immediately, not block"))))


(def-results get-ex-test
  (clause :unsettled "get-ex returns nil for unsettled task"
    (is (nil? (q/get-ex (q/sleep 1000 :result)))))

  (clause :settled-with-value "get-ex returns nil for task settled with a value"
    (let [task (q 42)]
      (with-task task
        (result []
          (is (nil? (q/get-ex task)))))))

  (clause :failed "get-ex returns the exception from a failed task"
    (let [ex   (make-exception "Boom")
          task (q/task (throw ex))]
      (with-task task
        (result [_v e c]
          (is (identical? ex (q/get-ex task)))
          (is (identical? e (q/get-ex task)))
          (is (false? c))))))

  (clause :cancelled "get-ex returns the cancellation exception from a cancelled task"
    (let [task (q/sleep 1000 :result)]
      (with-task (q/cancel task)
        (result []
          (is (q/cancelled? task))
          (let [e (q/get-ex task)]
            #?(:clj  (is (instance? java.util.concurrent.CancellationException e))
               :cljs (is (true? (:cancelled (ex-data e))))))))))

  (clause :plain-value "get-ex returns nil for a plain value"
    (is (nil? (q/get-ex 42)))))
