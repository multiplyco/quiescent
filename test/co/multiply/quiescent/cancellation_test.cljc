(ns co.multiply.quiescent.cancellation-test
  (:require
    [clojure.test :refer [deftest is testing #?(:cljs async)]]
    [co.multiply.quiescent :as q :refer [q qdo]]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.test-support :refer [allow-platform-park clause def-results result with-task make-exception]]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [ask]])
  #?(:clj (:import
            [java.util.concurrent CancellationException])))


(allow-platform-park)


;; # Structured concurrency
;; ################################################################################

(def-results structured-concurrency-test
  (clause :basic "Basic task cancellation"
    (let [task (q/sleep 100)]
      (with-task (q/cancel task)
        (result []
          (is (q/cancelled? task))))))

  (clause :parent-cancels-children "Parent cancellation cancels children"
    (let [inner (atom nil)
          outer (atom nil)]
      (with-task (q/task
                   (reset! outer (ask impl/*this*))
                   (q/task
                     (let [sleeper (q/sleep 100)]
                       (reset! inner sleeper)
                       (q/cancel @outer)
                       sleeper)))
        (result []
          (is (q/cancelled? @inner)))))))


#?(:clj (deftest deep-nesting-cancellation-test
          (testing "Deep nesting: cancelling root cancels all descendants"
            (let [level3-ran     (atom false)
                  level1-mounted (promise)
                  root           (q/task
                                   (deliver level1-mounted true)
                                   @(q/task
                                      @(q/sleep 100
                                         (fn []
                                           (reset! level3-ran true)
                                           :level3))))]
              @level1-mounted
              @(q/cancel root)
              (is (q/cancelled? root))
              (is (not @level3-ran) "Deeply nested child should be cancelled")))))


;; # Thread semantics (CLJ)
;; ################################################################################

#?(:clj (deftest waiter-interruption-doesnt-cancel-task-test
          (testing "Interrupting a thread waiting on a task does NOT cancel the task"
            ;; This verifies that the task being waited on continues running
            ;; even when the waiter is interrupted. I.e., task lifecycle isn't
            ;; dependent on listener.
            (let [task           (q/sleep 10 true)
                  waiter-started (promise)
                  waiter-error   (promise)
                  waiter         (Thread.
                                   (fn []
                                     (deref task 5 true)
                                     (deliver waiter-started true)
                                     (try @task
                                       (catch InterruptedException e
                                         (deliver waiter-error e)))))]
              (.start waiter)
              @waiter-started
              (.interrupt waiter)
              ;; Waiter should have been interrupted
              (is (instance? InterruptedException @waiter-error))
              (is @task "Task should complete even though waiter was interrupted")))))


#?(:clj (deftest task-creation-on-interrupted-thread-test
          (testing "Creating task on interrupted thread returns cancelled task without throwing"
            ;; When comply-interrupt-task detects interruption, it should cancel
            ;; the task but NOT throw an exception. This allows reactive frameworks
            ;; like Electric/Missionary to handle teardown gracefully.
            (let [captured-task  (promise)
                  exception-atom (atom nil)
                  worker         (Thread.
                                   (fn []
                                     ;; Set interrupt flag before creating task
                                     (.interrupt (Thread/currentThread))
                                     (try
                                       ;; Create task on interrupted thread
                                       (let [task (q/task :should-not-run)]
                                         (deliver captured-task task))
                                       (catch Throwable e
                                         (reset! exception-atom e)))))]
              (.start worker)
              (.join worker 1000)
              (is (some? @captured-task) "Task should be returned")
              (is (q/cancelled? @captured-task) "Task should be cancelled")
              (is (nil? @exception-atom) "Task creation should not throw on interrupted thread")))

          (testing "Code after task creation continues when thread is interrupted"
            ;; This verifies that comply-interrupt-task doesn't throw, allowing
            ;; subsequent code to execute (e.g., cleanup handlers)
            (let [continued (promise)
                  worker    (Thread.
                              (fn []
                                (.interrupt (Thread/currentThread))
                                (q/task :ignored)
                                ;; This should execute because we don't throw
                                (deliver continued true)))]
              (.start worker)
              (.join worker 1000)
              (is @continued "Code after task creation should continue")))))


#?(:clj (deftest cancelling-waiter-doesnt-cancel-waited-task-test
          (testing "Cancelling task B that waits on task A does NOT cancel task A"
            (let [task-a (q/sleep 10 :a-result)
                  task-b (q/task @task-a)]
              @(q/cancel task-b)
              (is (= :a-result @task-a) "Task A should complete")
              (is (q/cancelled? task-b) "Task B should be cancelled")))
          (testing "Cancelled waiter task doesn't prevent other tasks from getting result"
            (let [upstream (q/sleep 10 :upstream-result)
                  waiter1  (q/task @upstream)
                  waiter2  (q/task @upstream)]
              @(q/cancel waiter1)
              (is (q/cancelled? waiter1))
              (is (= :upstream-result @waiter2))
              (is (= :upstream-result @upstream))))))


;; # Chain propagation
;; ################################################################################

(def-results chain-propagation-test
  (clause :cancels-upstream-and-downstream "Cancelling task B cancels upstream and downstream tasks"
    (let [task-a  (q/sleep 10 :a-result)
          task-b  (q/then task-a str)
          results (atom [])
          record! (fn [& _] (swap! results conj :ran))
          task-c  (q/catch task-b record!)
          task-d  (q/handle task-b record!)
          tasks   [task-a task-b task-c task-d]]
      (with-task [(q/cancel task-b) tasks]
        (result []
          (is (every? q/cancelled? tasks) "All tasks should be cancelled")
          (is (empty? @results) "Handlers should not have run."))))))


;; # Handler behavior
;; ################################################################################

(def-results handler-behavior-test
  (clause :only-finally-runs "Only finally runs when upstream is cancelled"
    (let [task-a          (q/sleep 10 :a-result)
          results         (atom #{})
          first-completed (q/promise)
          record!         (fn [k] (fn [& _] (swap! results conj k) (first-completed k)))
          task-then       (q/then task-a (record! :then))
          task-catch      (q/catch task-a (record! :catch))
          task-handle     (q/handle task-a (record! :handle))
          task-ok         (q/ok task-a (record! :ok))
          task-err        (q/err task-a (record! :err))
          task-done       (q/done task-a (record! :done))]
      (q/finally [task-then task-catch task-handle task-ok task-err task-done]
                 (record! :finally))
      (with-task (qdo (q/cancel task-a) first-completed)
        (result [v]
          (is (= #{:finally} @results) "Only finally should run on cancellation")
          (is (= :finally v) "Only finally should run on cancellation"))))))


;; # Exception teardown
;; ################################################################################

#?(:clj (deftest in-flight-child-interrupted-test
          (testing "In-flight child task is interrupted when parent throws"
            ;; When a child task is already running (sleeping) and the parent throws,
            ;; the child's thread should be interrupted, stopping execution.
            (let [child-res  (atom nil)
                  child-done (promise)
                  parent     (q/task
                               (q/task
                                 (try (Thread/sleep 100)
                                   (reset! child-res true)
                                   (finally (child-done true))))
                               (Thread/sleep 10)
                               (throw (Exception. "Parent fails")))]
              (is (thrown-with-msg? Exception #"Parent fails" @parent))
              @child-done
              (is (not @child-res) "Child should be interrupted before completing")))))


(def-results exception-teardown-test
  (clause :sibling-teardown "Child exception terminates parent which tears down siblings"
    (let [b-ran  (atom false)
          b-ref  (atom nil)
          parent (q/task
                   (reset! b-ref
                     (q/sleep 100
                       (fn []
                         (reset! b-ran true)
                         :b-result)))
                   (q/task (throw (make-exception "Oh no!"))))]
      (with-task parent
        (result [_v e c]
          (is (q/cancelled? @b-ref) "Sibling was cancelled.")
          (is (false? c) "Parent didn't cancel.")
          (is (some? e) "Parent did throw.")
          (is (not @b-ran) "Sibling b is torn down when parent a terminates."))))))


;; # Borrowed vs child
;; ################################################################################

(def-results borrowed-task-test
  (clause :not-torn-down "Borrowed task is NOT torn down when borrower terminates"
    (let [external (q/sleep 10 :external-result)
          borrower (q/task
                     ;; Just referenced somewhere in the body
                     external
                     (throw (make-exception "Borrower fails")))]
      (with-task external
        (result [v e c]
          (is (= :external-result v) "Borrowed completes.")
          (is (not c) "Borrowed is not cancelled.")
          (is (nil? e) "Borrowed did not throw.")
          (is (q/exceptional? borrower) "Borrower is exceptional."))))))


#?(:clj (deftest borrowed-task-deref-test
          (testing "Borrowed task (dereferenced) is NOT torn down when borrower terminates"
            (let [external (q/sleep 10 :external-result)
                  borrower (q/task
                             ;; For CLJ, also test dereferencing.
                             @external
                             (throw (make-exception "Borrower fails")))]
              (with-task external
                (result [v e c]
                  (is (= :external-result v) "Borrowed completes.")
                  (is (not c) "Borrowed is not cancelled.")
                  (is (nil? e) "Borrowed did not throw.")
                  (is (q/exceptional? borrower) "Borrower is exceptional.")))))))


;; # cancelled? predicate
;; ################################################################################

(def-results cancelled-predicate-test
  (clause :true-for-cancelled "returns true for cancelled task"
    (let [task (q/sleep 10000 :result)]
      (with-task (q/cancel task)
        (result [v]
          (is (true? v))
          (is (q/cancelled? task))
          #?(:clj (is (thrown? CancellationException @task)))))))

  (clause :false-for-successful "returns false for successful task"
    (let [task (q/task :success)]
      (with-task (-> (q/await task) (q/then (fn [& _] (q/cancel task))))
        (result [v]
          (is (false? v))
          (is (false? (q/cancelled? task)))))))

  (clause :false-for-failed "returns false for failed task"
    (let [task (q/task (throw (make-exception "Oh no!")))]
      (with-task (-> (q/await task) (q/then (fn [& _] (q/cancel task))))
        (result [v]
          (is (false? v))
          (is (false? (q/cancelled? task)))))))

  (clause :throws-for-nil "throws for nil"
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs ExceptionInfo)
          (q/cancel nil)))))


;; # finally cancelled argument
;; ################################################################################

(def-results finally-cancelled-arg-test
  (clause :true-when-cancelled "cancelled is true when upstream was cancelled"
    (let [finally-args (q/promise)
          upstream     (q/sleep 10000 :result)
          chained      (q/finally upstream
                                  (fn [v e cancelled]
                                    (finally-args [v e cancelled])))]
      (with-task (qdo (q/cancel upstream) finally-args)
        (result [v]
          (let [[_v err cancelled] v]
            (is (true? cancelled))
            (is (q/cancelled? chained))
            #?(:clj (is (instance? CancellationException err)
                      "finally handler receives CancellationException"))
            #?(:cljs (is (true? (:cancelled (ex-data err)))
                       "finally handler receives cancellation ex-info")))))))

  (clause :false-when-successful "cancelled is false when upstream succeeds"
    (let [was-cancelled (q/promise)
          upstream      (q :success)
          chained       (q/finally upstream
                                   (fn [_ _ cancelled]
                                     (was-cancelled cancelled)))]
      (with-task (qdo (q/cancel upstream) was-cancelled)
        (result [v]
          (is (false? v))
          (is (not (q/cancelled? chained)))))))

  (clause :false-when-failed "cancelled is false when upstream fails"
    (let [was-cancelled (q/promise)
          upstream      (q/task (throw (make-exception "Oh no!")))
          chained       (q/finally upstream
                                   (fn [_ _ cancelled]
                                     (was-cancelled cancelled)))]
      (with-task (qdo (q/sleep 10 #(q/cancel upstream)) was-cancelled)
        (result [v]
          (is (false? v))
          (is (not (q/cancelled? chained))))))))


;; # cancel return value
;; ################################################################################

(def-results cancel-return-value-test
  (clause :true-when-successful "cancel returns true when cancellation succeeds"
    (with-task (q/cancel (q/sleep 1000 :result))
      (result [v]
        (is (true? v) "cancel should return true when it wins the race"))))

  (clause :false-when-unsuccessful "cancel returns false when cancellation fails"
    (let [task (q/task :success)]
      (with-task (q/then task (fn [& _] (q/cancel task)))
        (result [v]
          (is (false? v) "cancel should return false when it loses the race")))))

  (clause :exactly-one-wins "One cancel wins, subsequent cancels return false"
    (let [task (q/sleep 1000 :result)]
      (with-task [(q/cancel task) (q/cancel task) (q/cancel task)]
        (result [v]
          (is (= 1 (count (filterv true? v))) "Exactly one cancellation succeeded.")))))

  (clause :settles-at-quiescent "When the cancel task settles, the cancelled task should be quiescent"
    (let [task (q/sleep 10000 :result)]
      (with-task (-> (q/cancel task)
                   (q/then (fn [& _] (call/getPhase task))))
        (result [v]
          (is (= sm/phase-quiescent v)))))))


;; # Compel
;; ################################################################################
(def-results compel-test
  (clause :ignores-cascade-cancellation "Compelled task ignores cascade cancellation from parent"
    (let [inner     (q/sleep 20 :inner-result)
          compelled (q/compel inner)
          parent    (qdo compelled (q/sleep 100 :parent-result))]
      (with-task (qdo (q/sleep 10 #(q/cancel parent)) inner)
        (result [v]
          (is (= :inner-result v) "Compelled task completes despite parent cancellation")
          (is (q/cancelled? parent) "Parent was cancelled")
          (is (not (q/cancelled? inner)) "Inner task was not cancelled")))))

  (clause :allows-direct-cancellation "Direct cancellation on compelled task propagates to inner task"
    (let [inner-ran (atom false)
          inner     (q/sleep 100 #(reset! inner-ran true))
          compelled (q/compel inner)]
      (with-task (q/cancel compelled)
        (result [v]
          (is (true? v) "Cancel succeeded")
          (is (q/cancelled? compelled) "Compelled wrapper is cancelled")
          (is (q/cancelled? inner) "Inner task is also cancelled")
          (is (false? @inner-ran) "Inner task did not run")))))

  (clause :completes-normally "Compelled task completes normally when not cancelled"
    (let [inner     (q/sleep 10 :inner-result)
          compelled (q/compel inner)]
      (with-task compelled
        (result [v]
          (is (= :inner-result v) "Compelled task returns inner result")))))

  (clause :enables-cleanup "Compel allows critical cleanup tasks"
    (let [cleanup-ran (q/promise)
          task        (-> (q/task (throw (make-exception "Simulated failure")))
                        (q/finally
                          (fn [_v _e _c]
                            (q/compel (q/task (cleanup-ran true))))))]
      (with-task cleanup-ran
        (result [v]
          (is (q/exceptional? task) "Parent fails.")
          (is (true? v) "Cleanup ran.")))))

  (clause :qlet-compel "Compelled task in qlet completes despite sibling failure"
    (let [essential-ran (q/promise)
          task          (q/qlet [explosive (q/task (throw (make-exception "Boom!")))
                                 essential (q/compel (q/sleep 10 #(essential-ran true)))]
                          [explosive essential])]
      (with-task (q/timeout essential-ran 100 false)
        (result [v]
          (is (q/exceptional? task) "qlet threw.")
          (is (true? v) "Essential ran.")))))

  (clause :race-compel "Compelled task in race completes despite losing"
    (let [essential-ran (q/promise)
          task          (q/race (q/task :done) (q/compel (q/sleep 10 #(essential-ran true))))]
      (with-task (q/timeout essential-ran 100 false)
        (result [v]
          (is (false? (q/exceptional? task)) "Race completed.")
          (is (true? v) "Essential ran.")))))

  (clause :timeout-compel "Compelled task in timeout completes despite timing out"
    (let [essential-ran (q/promise)
          task          (q/timeout (q/compel (q/sleep 10 #(essential-ran true))) 0)]
      (with-task (q/timeout essential-ran 100 false)
        (result [v]
          (is (q/exceptional? task) "Timeout threw.")
          (is (true? v) "Essential ran."))))))


;; # Monitor
;; ################################################################################
(def-results monitor-cancellation-test
  (clause :propagates "Monitor should not break the cancellation chain."
    (let [task (q/sleep 10000)]
      (with-task (-> task
                   ;; Set up monitor
                   (q/monitor 5000 #(println "I should not print."))
                   ;; Cancel it
                   (q/cancel))
        (result []
          (is (q/cancelled? task)))))))


;; # Abort Signal (CLJS only)
;; ################################################################################

#?(:cljs (def-results abort-signal-test
           (clause :not-aborted-initially "abort-signal returns a signal that is not aborted initially"
             (with-task (q/task
                          (let [signal (q/abort-signal)]
                            (.-aborted signal)))
               (result [v]
                 (is (false? v) "Signal should not be aborted when created"))))

           (clause :aborted-on-cancel "Signal is aborted when task is cancelled"
             (let [signal-aborted (q/promise)
                   task           (q/task
                                    (let [signal (q/abort-signal)]
                                      (.addEventListener signal "abort" #(signal-aborted true))
                                      (q/sleep 10000)))]
               (with-task (qdo (q/cancel task) signal-aborted)
                 (result [v]
                   (is (true? v) "Signal should be aborted when task is cancelled")))))

           (clause :aborted-on-success "Signal is aborted when task succeeds (at settling)"
             (let [signal-aborted (q/promise)
                   task           (q/task
                                    (let [signal (q/abort-signal)]
                                      (.addEventListener signal "abort" #(signal-aborted true))
                                      :success))]
               (with-task (qdo task signal-aborted)
                 (result [v]
                   (is (true? v) "Signal should be aborted at settling")))))

           (clause :aborted-on-failure "Signal is aborted when task fails (at settling)"
             (let [signal-aborted (q/promise)
                   task           (q/task
                                    (let [signal (q/abort-signal)]
                                      (.addEventListener signal "abort" #(signal-aborted true))
                                      (throw (make-exception "Task fails"))))]
               ;; Catch the failure so qdo doesn't propagate it, then await signal-aborted
               (with-task (qdo (q/catch task (constantly nil)) signal-aborted)
                 (result [v]
                   (is (true? v) "Signal should be aborted at settling")))))

           (clause :independent-signals "Multiple abort-signal calls return independent signals"
             (with-task (q/task
                          (let [signal1 (q/abort-signal)
                                signal2 (q/abort-signal)]
                            (not (identical? signal1 signal2))))
               (result [v]
                 (is (true? v) "Each abort-signal call should return a different signal"))))

           (clause :works-in-q "abort-signal works within q (not just task)"
             (let [signal-aborted (q/promise)
                   task           (q
                                    (let [signal (q/abort-signal)]
                                      (.addEventListener signal "abort" #(signal-aborted true))
                                      (q/sleep 10000)))]
               (with-task (qdo (q/cancel task) signal-aborted)
                 (result [v]
                   (is (true? v) "Signal should be aborted when q-task is cancelled")))))))


#?(:cljs (deftest abort-signal-outside-scope-test
           (async done
             (is (thrown-with-msg? js/Error #"task scope"
                   (q/abort-signal)))
             (done))))


#?(:cljs (def-results aborted-predicate-test
           (clause :false-for-fresh-signal "aborted? returns false for a fresh signal"
             (with-task (q/task
                          (q/aborted? (q/abort-signal)))
               (result [v]
                 (is (false? v) "Fresh signal should not be aborted"))))

           (clause :true-for-aborted-signal "aborted? returns true for an aborted signal"
             (let [aborted-result (q/promise)
                   task           (q/task
                                    (let [signal (q/abort-signal)]
                                      (.addEventListener signal "abort"
                                        #(aborted-result (q/aborted? signal)))
                                      (q/sleep 10000)))]
               (with-task (qdo (q/cancel task) aborted-result)
                 (result [v]
                   (is (true? v) "Signal should report aborted after abort event")))))))


#?(:cljs (def-results comply-abort-test
           (clause :no-throw-for-fresh-signal "comply-abort does not throw for a fresh signal"
             (with-task (q/task
                          (q/comply-abort (q/abort-signal))
                          :completed)
               (result [v]
                 (is (= :completed v) "Should complete without throwing"))))

           (clause :throws-for-aborted-signal "comply-abort throws for an aborted signal"
             (let [threw (q/promise)
                   task  (q/task
                           (let [signal (q/abort-signal)]
                             (.addEventListener signal "abort"
                               (fn []
                                 (try
                                   (q/comply-abort signal)
                                   (threw false)
                                   (catch js/Error _e
                                     (threw true)))))
                             (q/sleep 10000)))]
               (with-task (qdo (q/cancel task) threw)
                 (result [v]
                   (is (true? v) "comply-abort should throw for aborted signal")))))))
