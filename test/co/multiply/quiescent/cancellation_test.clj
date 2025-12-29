(ns co.multiply.quiescent.cancellation-test
  "Tests for task cancellation behavior including parent-child relationships,
   waiter vs child semantics, interruption handling, and cancel returning Task[Boolean]."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]]
    [co.multiply.scoped :refer [ask]])
  (:import
    [java.util.concurrent CancellationException]))


(use-fixtures :once platform-thread-fixture)


(deftest basic-cancellation-test
  (testing "Basic task cancellation"
    (let [started (promise)
          task    (q/task
                    (deliver started true)
                    (Thread/sleep 10000)
                    :completed)]
      @started                                              ; Wait for task to start
      (q/cancel task)
      (is (thrown? CancellationException @task))))

  (testing "Parent cancellation cancels children"
    (let [child-started (promise)
          parent        (q/task
                          (let [child (q/task
                                        (deliver child-started true)
                                        (Thread/sleep 10000)
                                        :completed)]
                            (Thread/sleep 100)              ; Give time for registration
                            @child))]
      @child-started                                        ; Wait for child to start
      (q/cancel parent)
      (is (thrown? CancellationException @parent))))

  (testing "Cancelling one task in qlet doesn't affect independent tasks"
    (let [task-a-started (promise)
          task-b-started (promise)
          result         (q/qlet [a (q/task
                                      (deliver task-a-started true)
                                      (Thread/sleep 20)
                                      10)
                                  b (q/task
                                      (deliver task-b-started true)
                                      (Thread/sleep 20)
                                      20)
                                  c (+ a b)]
                           c)]
      @task-a-started
      @task-b-started
      ;; Both tasks should complete successfully
      (is (= 30 @result)))))


(deftest waiter-interruption-doesnt-cancel-task-test
  (testing "Interrupting a thread waiting on a task does NOT cancel the task"
    ;; This verifies that the task being waited on continues running
    ;; even when the waiter is interrupted. The waiter's interruption
    ;; is the waiter's problem, not the task's.
    (let [task-completed (atom false)
          task           (q/task
                           (Thread/sleep 100)
                           (reset! task-completed true)
                           :result)
          waiter-error   (atom nil)
          waiter         (Thread.
                           (fn []
                             (try
                               @task
                               (catch InterruptedException e
                                 (reset! waiter-error e)))))]
      (.start waiter)
      (Thread/sleep 20)                                     ; Let waiter start waiting
      (.interrupt waiter)                                   ; Interrupt the waiter
      (.join waiter 1000)
      ;; Waiter should have been interrupted
      (is (instance? InterruptedException @waiter-error))
      ;; But task should still complete!
      (Thread/sleep 150)
      (is @task-completed "Task should complete even though waiter was interrupted")
      (is (= :result @task))))

  (testing "Multiple waiters - one interrupted, task still completes for others"
    (let [task           (q/task
                           (Thread/sleep 100)
                           :result)
          waiter1-error  (atom nil)
          waiter2-result (atom nil)
          waiter1        (Thread.
                           (fn []
                             (try
                               @task
                               (catch InterruptedException e
                                 (reset! waiter1-error e)))))
          waiter2        (Thread.
                           (fn []
                             (reset! waiter2-result @task)))]
      (.start waiter1)
      (.start waiter2)
      (Thread/sleep 20)
      (.interrupt waiter1)                                  ; Interrupt only waiter1
      (.join waiter1 1000)
      (.join waiter2 1000)
      ;; Waiter1 was interrupted
      (is (instance? InterruptedException @waiter1-error))
      ;; But waiter2 got the result
      (is (= :result @waiter2-result)))))


(deftest task-creation-on-interrupted-thread-test
  (testing "Creating task on interrupted thread returns cancelled task without throwing"
    ;; When comply-interrupt-task detects interruption, it should cancel
    ;; the task but NOT throw an exception. This allows reactive frameworks
    ;; like Electric/Missionary to handle teardown gracefully.
    (let [task-result    (atom nil)
          exception-atom (atom nil)
          worker         (Thread.
                           (fn []
                             ;; Set interrupt flag before creating task
                             (.interrupt (Thread/currentThread))
                             (try
                               ;; Create task on interrupted thread
                               (let [task (q/task :should-not-run)]
                                 (reset! task-result task))
                               (catch Throwable e
                                 (reset! exception-atom e)))))]
      (.start worker)
      (.join worker 1000)
      ;; Should NOT have thrown
      (is (nil? @exception-atom) "Task creation should not throw on interrupted thread")
      ;; Task should exist and be cancelled
      (is (some? @task-result) "Task should be returned")
      (is (realized? @task-result) "Task should be realized (cancelled)")
      (is (thrown? CancellationException @@task-result))))

  (testing "Code after task creation continues when thread is interrupted"
    ;; This verifies that comply-interrupt-task doesn't throw, allowing
    ;; subsequent code to execute (e.g., cleanup handlers)
    (let [continued (atom false)
          worker    (Thread.
                      (fn []
                        (.interrupt (Thread/currentThread))
                        (q/task :ignored)
                        ;; This should execute because we don't throw
                        (reset! continued true)))]
      (.start worker)
      (.join worker 1000)
      (is @continued "Code after task creation should continue"))))


(deftest cancelling-waiter-doesnt-cancel-waited-task-test
  (testing "Cancelling task B that waits on task A does NOT cancel task A"
    ;; This is the key semantic: waiting on a task creates a "waiter" relationship,
    ;; NOT a parent-child relationship. Cancelling the waiter should not affect
    ;; the task being waited on.
    ;;
    ;; (let [a (task (Thread/sleep 100) true)
    ;;       b (task @a)]
    ;;   (cancel b))
    ;; => a should keep running!
    (let [task-a-completed (atom false)
          task-a           (q/task
                             (Thread/sleep 100)
                             (reset! task-a-completed true)
                             :a-result)
          task-b           (q/task @task-a)]
      ;; Cancel b immediately, before a completes
      (Thread/sleep 10)
      (q/cancel task-b)
      ;; b should be cancelled
      (is (thrown? CancellationException @task-b))
      ;; But a should continue and complete!
      (Thread/sleep 150)
      (is @task-a-completed "Task A should complete even though task B was cancelled")
      (is (= :a-result @task-a))))

  (testing "Cancelled waiter task doesn't prevent other tasks from getting result"
    ;; If multiple tasks wait on the same upstream task, cancelling one waiter
    ;; should not affect the upstream or other waiters
    (let [upstream (q/task
                     (Thread/sleep 100)
                     :upstream-result)
          waiter1  (q/task @upstream)
          waiter2  (q/task @upstream)]
      (Thread/sleep 10)
      (q/cancel waiter1)
      ;; waiter1 is cancelled
      (is (thrown? CancellationException @waiter1))
      ;; But waiter2 should get the result
      (is (= :upstream-result @waiter2))
      ;; And upstream completed normally
      (is (= :upstream-result @upstream)))))


(deftest parent-child-cancellation-test
  (testing "Cancelling parent cancels child task created inside"
    ;; This is the parent-child relationship: when a task is created INSIDE
    ;; another task's body, it becomes a child via add-child. Cancelling
    ;; the parent cascades to children.
    ;;
    ;; (let [a (task @(task (Thread/sleep 100) ...))]
    ;;   (cancel a))
    ;; => inner task should be cancelled because it's a child of a
    (let [inner-ran (atom false)
          outer     (q/task
                      @(q/task
                         (Thread/sleep 100)
                         (reset! inner-ran true)
                         :inner-result))]
      ;; Cancel outer immediately
      (Thread/sleep 10)
      (q/cancel outer)
      ;; Outer is cancelled
      (is (thrown? CancellationException @outer))
      ;; Inner should NOT have completed (it was cancelled via teardown)
      (Thread/sleep 150)
      (is (not @inner-ran) "Child task should be cancelled when parent is cancelled")))

  (testing "Deep nesting: cancelling root cancels all descendants"
    (let [level3-ran (atom false)
          root       (q/task
                       @(q/task
                          @(q/task
                             (Thread/sleep 100)
                             (reset! level3-ran true)
                             :level3)))]
      (Thread/sleep 10)
      (q/cancel root)
      (is (thrown? CancellationException @root))
      (Thread/sleep 150)
      (is (not @level3-ran) "Deeply nested child should be cancelled")))

  (testing "Cancelling parent cancels all sibling children"
    ;; (let [a (task
    ;;           (let [b (task (Thread/sleep 5000) true)
    ;;                 c (task @b)]
    ;;             @c))]
    ;;   (cancel a))
    ;; => Both b and c are cancelled because they're both children of a
    (let [b-ran (atom false)
          c-ran (atom false)
          a     (q/task
                  (let [b (q/task
                            (Thread/sleep 200)
                            (reset! b-ran true)
                            :b-result)
                        c (q/task
                            (reset! c-ran true)
                            @b)]
                    @c))]
      (Thread/sleep 20)
      (q/cancel a)
      (is (thrown? CancellationException @a))
      (Thread/sleep 250)
      ;; c might have started but b should not complete
      (is (not @b-ran) "Sibling child b should be cancelled when parent is cancelled")))

  (testing "Child exception terminates parent which tears down siblings"
    ;; (let [a (task
    ;;           (let [b (task (Thread/sleep 100) true)
    ;;                 c (task (throw (Exception. "Oh no!")))]
    ;;             @c))]
    ;;   @a)
    ;; => c throws -> a terminates with exception -> b is torn down
    ;; Note: c doesn't directly cancel b (siblings don't affect each other),
    ;; but a's termination triggers teardown of all children defined in a's closure.
    (let [b-ran (atom false)
          a     (q/task
                  (let [b (q/task
                            (Thread/sleep 100)
                            (reset! b-ran true)
                            :b-result)
                        c (q/task
                            (throw (Exception. "Oh no!")))]
                    @c))]
      ;; a should propagate c's exception
      (is (thrown-with-msg? Exception #"Oh no!" @a))
      ;; b should be torn down because a terminated (not because c threw)
      (Thread/sleep 150)
      (is (not @b-ran) "Sibling b is torn down when parent a terminates")))

  (testing "Borrowed task is NOT torn down when borrower terminates"
    ;; A task defined OUTSIDE and passed into another task is not a child.
    ;; It should continue running even if the borrower terminates.
    (let [external-ran (atom false)
          external     (q/task
                         (Thread/sleep 100)
                         (reset! external-ran true)
                         :external-result)
          borrower     (q/task
                         @external
                         (throw (Exception. "Borrower fails")))]
      ;; Borrower throws
      (is (thrown-with-msg? Exception #"Borrower fails" @borrower))
      ;; But external continues - it was not defined inside borrower
      (Thread/sleep 150)
      (is @external-ran "External task continues running (not a child)")
      (is (= :external-result @external))))

  (testing "In-flight child task is interrupted when parent throws"
    ;; When a child task is already running (sleeping) and the parent throws,
    ;; the child's thread should be interrupted, stopping execution.
    (let [child-completed (atom false)
          parent          (q/task
                            (let [a (q/task
                                      (Thread/sleep 100)
                                      (reset! child-completed true))]
                              (Thread/sleep 50)
                              (throw (Exception. "Parent fails"))))]
      (is (thrown-with-msg? Exception #"Parent fails" @parent))
      ;; Give plenty of time for child to complete if it wasn't interrupted
      (Thread/sleep 150)
      (is (not @child-completed) "Child should be interrupted before completing"))))


(deftest cancelled?-test
  (testing "cancelled? with 1-arity"
    (testing "returns true for cancelled task"
      (let [task (q/task (Thread/sleep 10000) :result)]
        (Thread/sleep 10)
        @(q/cancel task)
        (is (true? (q/cancelled? task)))))

    (testing "returns false for successful task"
      (let [task (q/task :success)]
        @task
        (is (false? (q/cancelled? task)))))

    (testing "returns false for failed task"
      (let [task (q/task (throw (Exception. "error")))]
        (try @task (catch Exception _))
        (is (false? (q/cancelled? task)))))

    (testing "returns false for nil argument"
      (is (false? (q/cancelled? nil)))))

  (testing "finally receives cancelled boolean as third argument"
    (testing "cancelled is true when upstream was cancelled"
      (let [was-cancelled (promise)
            upstream      (q/task (Thread/sleep 10000) :result)
            chained       (q/finally upstream
                            (fn [_ _ cancelled]
                              (deliver was-cancelled cancelled)))]
        (Thread/sleep 10)
        @(q/cancel upstream)
        (try @chained (catch CancellationException _))
        (is (true? @was-cancelled))))

    (testing "cancelled is false when upstream completed successfully"
      (let [was-cancelled (promise)
            upstream      (q/task :success)
            chained       (q/finally upstream
                            (fn [_ _ cancelled]
                              (deliver was-cancelled cancelled)))]
        @chained
        (is (false? @was-cancelled))))

    (testing "cancelled is false when upstream failed with exception"
      (let [was-cancelled (promise)
            upstream      (q/task (throw (Exception. "error")))
            chained       (q/finally upstream
                            (fn [_ _ cancelled]
                              (deliver was-cancelled cancelled)))]
        (try @chained (catch Exception _))
        (is (false? @was-cancelled))))))


(deftest cancel-returns-task-test
  ;; cancel now returns a Task[Boolean] that settles when the target reaches quiescent.
  ;; The boolean indicates whether this cancel call won the race.

  (testing "cancel returns a Task"
    (let [task   (q/task (Thread/sleep 1000) :result)
          result (q/cancel task)]
      (is (q/task? result) "cancel should return a Task")))

  (testing "cancel returns true when cancellation succeeds"
    (let [task   (q/task (Thread/sleep 1000) :result)
          _      (Thread/sleep 10)                          ; Let task start
          result @(q/cancel task)]
      (is (true? result) "cancel should return true when it wins the race")))

  (testing "cancel returns false when task already settled successfully"
    (let [task (q/task :immediate)]
      @task                                                 ; Wait for task to complete
      (let [result @(q/cancel task)]
        (is (false? result) "cancel should return false when task already completed"))))

  (testing "cancel returns false when task already failed"
    (let [task (q/task (throw (Exception. "Error")))]
      (try @task (catch Exception _))
      (let [result @(q/cancel task)]
        (is (false? result) "cancel should return false when task already failed"))))

  (testing "First cancel wins, subsequent cancels return false"
    (let [task   (q/task (Thread/sleep 1000) :result)
          _      (Thread/sleep 10)
          first  @(q/cancel task)
          second @(q/cancel task)
          third  @(q/cancel task)]
      (is (true? first) "First cancel should win")
      (is (false? second) "Second cancel should return false")
      (is (false? third) "Third cancel should return false")))

  (testing "cancel task settles when target reaches quiescent"
    ;; The returned task should settle only when the target is fully quiescent
    (let [cleanup-started  (promise)
          cleanup-finished (promise)
          task             (-> (q/task (Thread/sleep 10000) :result)
                             (q/finally
                               (fn [_ _ _]
                                 (deliver cleanup-started true)
                                 (Thread/sleep 50)
                                 (deliver cleanup-finished true))))
          cancel-task      (do (Thread/sleep 10)
                             (q/cancel task))]
      ;; Cancel task should not settle until finally completes
      (is (true? (deref cleanup-started 500 false)) "Cleanup should have started")
      ;; Deref the cancel task - it should wait for quiescence
      (is (true? @cancel-task) "Cancel should return true")
      ;; By the time deref returns, cleanup should be finished
      (is (true? (deref cleanup-finished 100 false)) "Cleanup should have completed")))

  (testing "cancel can be used fire-and-forget style"
    ;; Just calling (cancel task) without deref still triggers cancellation
    (let [task (q/task (Thread/sleep 1000) :result)]
      (Thread/sleep 10)
      (q/cancel task)                                       ; Fire and forget
      (Thread/sleep 50)
      (is (thrown? CancellationException @task)))))
