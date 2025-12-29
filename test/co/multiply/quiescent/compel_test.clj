(ns co.multiply.quiescent.compel-test
  "Tests for compel functionality: tasks that survive parent cancellation."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]])
  (:import
    [java.util.concurrent CancellationException]))


(use-fixtures :once platform-thread-fixture)


(deftest compel-test
  (testing "Compelled task survives parent cancellation"
    (let [compelled-started  (promise)
          compelled-finished (promise)
          parent             (q/task
                               (let [compelled (q/compel
                                                 (q/task
                                                   (deliver compelled-started true)
                                                   (Thread/sleep 100)
                                                   (deliver compelled-finished :completed)
                                                   :completed))]
                                 (Thread/sleep 20)          ; Give time for registration
                                 @compelled))]
      @compelled-started                                    ; Wait for compelled task to start
      (q/cancel parent)
      ;; Parent should be cancelled
      (is (thrown? CancellationException @parent))
      ;; But compelled task should complete successfully
      (is (= :completed @compelled-finished))))

  (testing "Normal child task is cancelled with parent"
    (let [child-started  (promise)
          child-finished (promise)
          parent         (q/task
                           (let [child (q/task
                                         (deliver child-started true)
                                         (Thread/sleep 100)
                                         (deliver child-finished :completed)
                                         :completed)]
                             (Thread/sleep 20)
                             @child))]
      @child-started                                        ; Wait for child to start
      (q/cancel parent)
      ;; Parent cancelled
      (is (thrown? CancellationException @parent))
      ;; Child should NOT finish (gets cancelled)
      (Thread/sleep 150)                                    ; Wait longer than child would take
      (is (not (realized? child-finished)))))

  (testing "Compel allows critical cleanup tasks"
    (let [cleanup-ran (atom false)
          parent      (q/task
                        (try
                          (throw (Exception. "Simulated failure"))
                          (finally
                            ;; Critical cleanup that MUST complete
                            @(q/compel
                               (q/task
                                 (Thread/sleep 50)
                                 (reset! cleanup-ran true))))))]
      ;; Parent fails
      (is (thrown? Exception @parent))
      ;; But cleanup completed
      (is @cleanup-ran)))

  (testing "Compelled task in qlet completes despite sibling failure"
    (let [p (q/promise)]
      (is (thrown? Exception
            @(q/qlet [explosive (q/task (throw (Exception. "Boom!")))
                      essential (q/compel (q/task (Thread/sleep 10) (deliver p true)))]
               [explosive essential])))                     ; Body depends on both - they're coordinated in same collection
      ;; Essential task should complete despite explosive failing and ground trying to cancel it
      (is (true? @(q/timeout p 100 false)))))

  (testing "Race with compelled task - winner completes, compelled loser keeps running"
    (let [compelled-ran (atom false)]
      ;; Quick task wins immediately
      (is (= :quick
            @(q/race
               (q/task :quick)
               (q/compel (q/task (Thread/sleep 50) (reset! compelled-ran true) :slow)))))
      ;; But compelled task should still complete in background
      (Thread/sleep 100)
      (is @compelled-ran)))

  (testing "Race with compelled task - cancelled race still allows compelled task to complete"
    (let [compelled-ran (atom false)
          r             (q/race
                          (q/task (Thread/sleep 1000) :slow-winner)
                          (q/compel (q/task (Thread/sleep 50) (reset! compelled-ran true) :compelled)))]
      ;; Cancel the race before either completes
      (q/cancel r)
      (is (thrown? java.util.concurrent.CancellationException @r))
      ;; Compelled task should still complete despite race being cancelled
      (Thread/sleep 100)
      (is @compelled-ran)))

  (testing "Timeout with compelled task - timeout fires but compelled task keeps running"
    (let [compelled-ran (atom false)]
      ;; Timeout fires before compelled task completes
      (is (= :timed-out
            @(q/timeout (q/compel (q/task (Thread/sleep 50) (reset! compelled-ran true) :result))
               10
               :timed-out)))
      ;; But compelled task should still complete in background
      (Thread/sleep 100)
      (is @compelled-ran)))

  (testing "Timeout with compelled task - cancelled timeout still allows compelled task to complete"
    (let [compelled-ran (atom false)
          timeout-task  (q/timeout (q/compel (q/task (Thread/sleep 50) (reset! compelled-ran true) :result))
                          1000
                          :timed-out)]
      ;; Cancel the timeout before it fires or task completes
      (q/cancel timeout-task)
      (is (thrown? java.util.concurrent.CancellationException @timeout-task))
      ;; Compelled task should still complete despite timeout being cancelled
      (Thread/sleep 100)
      (is @compelled-ran)))

  (testing "Compel with direct cancel - direct API calls still cancel compelled tasks"
    ;; When you cancel a task, the work should NOT complete (side effects shouldn't happen)
    (let [p (q/promise)]
      (q/cancel (q/compel (q/task (Thread/sleep 50) (deliver p false))))
      ;; Promise should not be delivered because task was cancelled before sleep completed
      ;; Timeout should return the default value 'true'
      (is (= true @(q/timeout p 100 true))))))


(deftest compel-rejects-promise-test
  ;; compel only accepts Tasks, not Promises.

  (testing "compel throws IllegalArgumentException for Promise"
    (let [p (q/promise)]
      (p :value)
      (is (thrown? IllegalArgumentException (q/compel p))
        "compel should throw IllegalArgumentException for Promise")))

  (testing "compel accepts Task"
    (let [task (q/task :value)]
      (is (= :value @(q/compel task)) "compel should work with Task")))

  (testing "compel throws for plain values"
    ;; Plain values are not auto-groundable, so they fail the task? check
    (is (thrown? IllegalArgumentException (q/compel 42))
      "compel should throw for plain values")
    (is (thrown? IllegalArgumentException (q/compel {:a 1}))
      "compel should throw for maps")
    (is (thrown? IllegalArgumentException (q/compel [1 2 3]))
      "compel should throw for vectors"))

  (testing "compel accepts CompletableFuture"
    ;; CompletableFutures are auto-groundable, so they get converted to Tasks
    (let [cf (java.util.concurrent.CompletableFuture/completedFuture :cf-value)]
      (is (= :cf-value @(q/compel cf)) "CompletableFuture should be converted and compelled"))))
