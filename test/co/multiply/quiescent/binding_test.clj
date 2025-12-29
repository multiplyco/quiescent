(ns co.multiply.quiescent.binding-test
  "Tests for scoped value propagation through all task phases.
   Scoped values should be correctly propagated through f (body function),
   tf (transform function), and subscriptions, in both normal flow
   and cancellation scenarios."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]]
    [co.multiply.scoped :refer [ask scoping]])
  (:import
    [java.util.concurrent CancellationException]))


(use-fixtures :once platform-thread-fixture)


(def ^:dynamic *my-binding*)
(def ^:dynamic *value*)
(def ^:dynamic *cpu-binding*)
(def ^:dynamic *nested*)
(def ^:dynamic *test-binding*)
(def ^:dynamic *outer-binding*)
(def ^:dynamic *inner-binding*)


(deftest scope-propagation-test
  (testing "Scoped values propagate to virtual threads"
    (scoping [*my-binding* :outer-value]
      (is (= :outer-value
            @(q/task (ask *my-binding*))))))

  (testing "Scoped values propagate through then chains"
    (scoping [*value* 100]
      (is (= 200
            @(-> (q/task (ask *value*))
               (q/then (fn [v] (+ v (ask *value*)))))))))

  (testing "Scoped values propagate to CPU tasks"
    (scoping [*cpu-binding* :cpu-value]
      (is (= :cpu-value
            @(q/cpu-task (ask *cpu-binding*))))))

  (testing "Nested tasks inherit scoped values from parent"
    (scoping [*nested* :parent]
      (is (= :parent
            @(q/task
               (q/task
                 (ask *nested*))))))))


(deftest f-body-scope-test
  (testing "f (task body) sees scoped values from creation context"
    (scoping [*test-binding* :from-outer]
      (is (= :from-outer @(q/task (ask *test-binding*))))))

  (testing "f sees scoped values on virtual executor"
    (scoping [*test-binding* :virtual]
      (let [observed (atom nil)]
        @(q/task (reset! observed (ask *test-binding*)))
        (is (= :virtual @observed)))))

  (testing "f sees scoped values on CPU executor"
    (scoping [*test-binding* :cpu]
      (let [observed (atom nil)]
        @(q/cpu-task (reset! observed (ask *test-binding*)))
        (is (= :cpu @observed)))))

  (testing "Nested tasks inherit scoped values through parent chain"
    (scoping [*outer-binding* :outer]
      (is (= [:outer :outer]
            @(q/task
               (scoping [*inner-binding* :inner]
                 [(q/task (ask *outer-binding*))
                  (q/task (ask *outer-binding*))])))))))


(deftest tf-transform-scope-test
  (testing "tf (transform function) sees scoped values from creation context"
    (scoping [*test-binding* :transform-context]
      (is (= :transform-context
            @(q/then (q/task 42)
               (fn [_] (ask *test-binding*)))))))

  (testing "tf sees scoped values when called on virtual executor"
    (scoping [*test-binding* :tf-virtual]
      (let [observed (atom nil)]
        @(q/then (q/task 42)
           (fn [_] (reset! observed (ask *test-binding*))))
        (is (= :tf-virtual @observed)))))

  (testing "tf sees scoped values when called on CPU executor"
    (scoping [*test-binding* :tf-cpu]
      (let [observed (atom nil)]
        @(q/then-cpu (q/task 42)
           (fn [_] (reset! observed (ask *test-binding*))))
        (is (= :tf-cpu @observed)))))

  (testing "Chained transforms all see correct scoped values"
    (scoping [*test-binding* :chained]
      (is (= [:chained :chained :chained]
            @(-> (q/task 1)
               (q/then (fn [_] [(ask *test-binding*)]))
               (q/then (fn [v] (conj v (ask *test-binding*))))
               (q/then (fn [v] (conj v (ask *test-binding*)))))))))

  (testing "handle function sees scoped values"
    (scoping [*test-binding* :handle-context]
      (is (= :handle-context
            @(q/handle (q/task 42)
               (fn [_ _] (ask *test-binding*)))))))

  (testing "catch function sees scoped values"
    (scoping [*test-binding* :catch-context]
      (is (= :catch-context
            @(q/catch (q/task (throw (Exception. "test")))
                      (fn [_] (ask *test-binding*)))))))

  (testing "ok function sees scoped values"
    (scoping [*test-binding* :ok-context]
      (let [observed (atom nil)]
        @(q/ok (q/task 42)
           (fn [_] (reset! observed (ask *test-binding*))))
        (is (= :ok-context @observed)))))

  (testing "finally function sees scoped values"
    (scoping [*test-binding* :finally-context]
      (let [observed (atom nil)]
        @(q/finally (q/task 42)
                    (fn [& _] (reset! observed (ask *test-binding*))))
        (is (= :finally-context @observed))))))


(deftest subscription-scope-test
  ;; Subscriptions are internal callbacks that fire at phase transitions.
  ;; They should see the scoped values from when the subscription was registered.

  (testing "Settling subscription sees scoped values from then-chain creation"
    ;; q/then creates a subscription that fires when the source settles
    (scoping [*test-binding* :sub-context]
      (let [observed (atom nil)
            task     (q/task (Thread/sleep 20) :result)]
        ;; The then subscription is registered with current scoped values
        (q/ok task (fn [_] (reset! observed (ask *test-binding*))))
        @task
        (Thread/sleep 50)
        (is (= :sub-context @observed)))))

  (testing "Multiple subscriptions each see their own scoped values"
    (let [obs1 (atom nil)
          obs2 (atom nil)
          task (q/task (Thread/sleep 20) :result)]
      (scoping [*test-binding* :first]
        (q/ok task (fn [_] (reset! obs1 (ask *test-binding*)))))
      (scoping [*test-binding* :second]
        (q/ok task (fn [_] (reset! obs2 (ask *test-binding*)))))
      @task
      (Thread/sleep 50)
      (is (= :first @obs1))
      (is (= :second @obs2))))

  (testing "Ground callback sees parent scoped values"
    ;; When a task grounds (resolves nested tasks), the ground callback
    ;; should see the scoped values from the outer task's context
    (scoping [*test-binding* :ground-context]
      (let [observed (atom nil)]
        @(q/task
           (let [inner (q/task (reset! observed (ask *test-binding*)))]
             @inner))
        (is (= :ground-context @observed)))))

  (testing "Child task subscription sees parent scoped values"
    ;; When parent creates child, child should inherit parent's scoped values
    (scoping [*test-binding* :parent-context]
      (let [observed (atom nil)]
        @(q/task
           @(q/task (reset! observed (ask *test-binding*))))
        (is (= :parent-context @observed))))))


(deftest scope-through-cancellation-test
  (testing "finally runs with correct scoped values when upstream task is cancelled"
    ;; When you cancel the UPSTREAM task, the finally callback should run
    ;; and see the scoped values from when the finally chain was created.
    ;; Note: Cancelling the DOWNSTREAM (link) task doesn't run the callback
    ;; because the upstream hasn't settled yet.
    (scoping [*test-binding* :cancel-finally]
      (let [observed (atom nil)
            original (q/task (Thread/sleep 10000) :result)
            chained  (q/finally original (fn [& _] (reset! observed (ask *test-binding*))))]
        (Thread/sleep 20)
        (q/cancel original)                                 ; Cancel the original task
        (try @chained (catch CancellationException _))
        (Thread/sleep 50)
        (is (= :cancel-finally @observed)))))

  (testing "catch does NOT run when upstream is cancelled"
    (let [handler-ran (atom false)
          original    (q/task (Thread/sleep 10000) :result)
          chained     (q/catch original CancellationException
                        (fn [_]
                          (reset! handler-ran true)
                          :caught))]
      (Thread/sleep 20)
      (q/cancel original)
      (try @chained (catch CancellationException _))
      (Thread/sleep 50)
      (is (false? @handler-ran) "catch should not run on cancellation")))

  (testing "handle does NOT run when upstream is cancelled"
    (let [handler-ran (atom false)
          original    (q/task (Thread/sleep 10000) :result)
          chained     (q/handle original
                        (fn [v e]
                          (reset! handler-ran true)
                          (if e :cancelled v)))]
      (Thread/sleep 20)
      (q/cancel original)
      (try @chained (catch CancellationException _))
      (Thread/sleep 50)
      (is (false? @handler-ran) "handle should not run on cancellation")))

  (testing "Parent cancellation teardown sees parent scoped values"
    (scoping [*test-binding* :teardown-context]
      (let [child-saw-scope (atom nil)
            parent          (q/task
                              (let [child (q/task
                                            (Thread/sleep 100)
                                            :child-result)]
                                ;; Observe scoped value in parent context before awaiting child
                                (reset! child-saw-scope (ask *test-binding*))
                                @child))]
        (Thread/sleep 10)
        (q/cancel parent)
        (try @parent (catch CancellationException _))
        (is (= :teardown-context @child-saw-scope)))))

  (testing "Cancelling chained task propagates backward to cancel upstream"
    ;; When you cancel the downstream (link) task before upstream settles,
    ;; backward cancellation should cancel the upstream task.
    (scoping [*test-binding* :backward-cancel]
      (let [parent-settled (promise)
            parent-scope   (atom nil)
            ;; Create upstream task that will settle (via cancel) and record its scope
            upstream       (q/task
                             ;; This ok fires when upstream settles for any reason
                             (Thread/sleep 10000)
                             :result)
            ;; Subscribe to upstream settling phase to observe when it's cancelled
            _              (-> upstream
                             (q/finally
                               (fn [& _]
                                 (reset! parent-scope (ask *test-binding*))
                                 (deliver parent-settled true))))
            ;; Create chained task
            chained        (q/then upstream identity)]
        (Thread/sleep 20)
        ;; Cancel the chained task - this should propagate back to upstream
        (q/cancel chained)
        ;; Wait for parent to settle (with timeout to avoid test hang)
        (is (true? (deref parent-settled 500 nil))
          "Upstream should settle when chained task is cancelled")
        ;; Verify scoped values were correct when finally ran
        (is (= :backward-cancel @parent-scope)
          "Finally callback should see correct scoped values")))))


(deftest scope-in-ground-callbacks-test
  ;; Ground phase resolves nested tasks. The callbacks should see correct scoped values.

  (testing "Single nested task ground callback sees scoped values"
    (scoping [*test-binding* :single-ground]
      (let [inner-scope (atom nil)]
        @(q/task
           {:inner (q/task
                     (reset! inner-scope (ask *test-binding*))
                     :value)})
        (is (= :single-ground @inner-scope)))))

  (testing "Multiple nested tasks all see scoped values"
    (scoping [*test-binding* :multi-ground]
      (let [scopes (atom [])]
        @(q/task
           {:a (q/task (swap! scopes conj (ask *test-binding*)) 1)
            :b (q/task (swap! scopes conj (ask *test-binding*)) 2)
            :c (q/task (swap! scopes conj (ask *test-binding*)) 3)})
        (is (= [:multi-ground :multi-ground :multi-ground] @scopes)))))

  (testing "Deeply nested tasks inherit scoped values through all levels"
    (scoping [*test-binding* :deep-ground]
      (let [deep-scope (atom nil)]
        @(q/task
           {:level1 {:level2 (q/task
                               {:level3 (q/task
                                          (reset! deep-scope (ask *test-binding*))
                                          :deep)})}})
        (is (= :deep-ground @deep-scope))))))


(deftest scope-in-variadic-then-test
  ;; Variadic q/then waits for multiple tasks then calls f with all values.
  ;; The f should see scoped values from the then call site.

  (testing "Variadic then function sees scoped values"
    (scoping [*test-binding* :variadic-then]
      (is (= :variadic-then
            @(q/then (q/task 1) (q/task 2) (q/task 3)
               (fn [_ _ _] (ask *test-binding*)))))))

  (testing "Variadic then with different scope contexts"
    (let [task-a (scoping [*test-binding* :a-context]
                   (q/task (ask *test-binding*)))
          task-b (scoping [*test-binding* :b-context]
                   (q/task (ask *test-binding*)))
          result (scoping [*test-binding* :then-context]
                   @(q/then task-a task-b
                      (fn [a b] [a b (ask *test-binding*)])))]
      ;; Each task saw its own creation-time scope,
      ;; then function sees its own creation-time scope
      (is (= [:a-context :b-context :then-context] result)))))


(deftest scope-in-qlet-test
  (testing "qlet body sees scoped values from creation context"
    (scoping [*test-binding* :qlet-context]
      (is (= :qlet-context
            @(q/qlet [a (q/task 1)
                      b (q/task 2)]
               (ask *test-binding*))))))

  (testing "qlet bindings expression sees correct scoped values"
    (scoping [*test-binding* :binding-expr]
      (is (= [:binding-expr :binding-expr]
            @(q/qlet [a (q/task (ask *test-binding*))
                      b (q/task (ask *test-binding*))]
               [a b])))))

  (testing "Dependent qlet bindings see correct scoped values"
    (scoping [*test-binding* :dependent]
      (let [observed (atom nil)]
        @(q/qlet [a (q/task (reset! observed (ask *test-binding*)) 10)
                  b (q/task (+ a 20))]
           b)
        (is (= :dependent @observed))))))


(deftest scope-in-coordination-test
  (testing "qdo participants see their creation scoped values"
    (let [scope-a (atom nil)
          scope-b (atom nil)]
      (scoping [*test-binding* :qdo-scope]
        @(q/qdo
           (q/task (reset! scope-a (ask *test-binding*)))
           (q/task (reset! scope-b (ask *test-binding*)))))
      (is (= :qdo-scope @scope-a))
      (is (= :qdo-scope @scope-b))))

  (testing "qfor body sees creation scoped values"
    (scoping [*test-binding* :qfor]
      (is (= [:qfor :qfor :qfor]
            @(q/qfor [_ [1 2 3]]
               (q/task (ask *test-binding*)))))))

  (testing "qmerge values see their creation scoped values"
    (scoping [*test-binding* :qmerge]
      (is (= {:a :qmerge :b :qmerge}
            @(q/qmerge {:a (q/task (ask *test-binding*))
                        :b (q/task (ask *test-binding*))}))))))


(deftest scope-with-compel-test
  (testing "Compelled task sees creation scoped values"
    (scoping [*test-binding* :compelled]
      (is (= :compelled
            @(q/compel (q/task (ask *test-binding*)))))))

  (testing "Compelled task continues with correct scoped values after parent cancel"
    (scoping [*test-binding* :compel-survive]
      (let [observed (promise)
            parent   (q/task
                       @(q/compel
                          (q/task
                            (Thread/sleep 50)
                            (deliver observed (ask *test-binding*))
                            :done)))]
        (Thread/sleep 10)
        (q/cancel parent)
        (try @parent (catch CancellationException _))
        (is (= :compel-survive (deref observed 200 :timeout)))))))
