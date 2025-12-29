(ns co.multiply.quiescent.error-handling-test
  "Tests for error handling: catch, handle, finally, ok, and their CPU variants.
   Includes tests for error propagation, cancellation semantics, and handler errors."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]])
  (:import
    [java.io IOException]
    [java.util.concurrent CancellationException]))


(use-fixtures :once platform-thread-fixture)


(deftest error-propagation-test
  (testing "Error in then chain"
    (is (thrown? Exception
          @(-> (q/task 10)
             (q/then (fn [_] (throw (Exception. "Error!"))))
             (q/then inc)))))

  (testing "Catch specific exception type"
    (is (= :caught
          @(-> (q/task (throw (IllegalArgumentException. "Bad arg")))
             (q/catch IllegalArgumentException (fn [_] :caught))))))

  (testing "Catch doesn't affect successful tasks"
    (is (= 42
          @(-> (q/task 42)
             (q/catch Exception (fn [_] :error))))))

  (testing "Catch with non-matching class propagates original exception"
    (is (thrown-with-msg? IOException #"IO error"
          @(-> (q/task (throw (IOException. "IO error")))
             (q/catch IllegalArgumentException (fn [_] :caught))))))

  (testing "Finally runs on success"
    (let [ran (atom false)]
      @(-> (q/task 42)
         (q/finally (fn [& _] (reset! ran true))))
      (is @ran)))

  (testing "Finally runs on error"
    (let [ran (atom false)]
      (try @(-> (q/task (throw (Exception. "Boom")))
              (q/finally (fn [& _] (reset! ran true))))
           (catch Exception _))
      (is @ran)))

  (testing "Ok only runs on success"
    (let [ran (atom false)]
      @(-> (q/task 42)
         (q/ok (fn [v] (reset! ran v))))
      (is (= 42 @ran)))

    (let [ran (atom false)]
      (try @(-> (q/task (throw (Exception. "Boom")))
              (q/ok (fn [v] (reset! ran true))))
           (catch Exception _))
      (is (false? @ran))))

  (testing "Error in nested structure cancels all tasks"
    (is (thrown? Exception
          @(q/q {:a (q/task 1)
                 :b (q/task (throw (Exception. "Boom!")))
                 :c (q/task 3)})))))


(deftest finally-cancellation-semantics-test
  (testing "finally handler runs when link task is cancelled"
    ;; This is the key semantic: finally ALWAYS runs, even when the link is cancelled
    (let [handler-ran (promise)
          inner       (q/task (Thread/sleep 10000) :result)
          link        (q/finally inner (fn [& _] (deliver handler-ran true)))]
      (Thread/sleep 20)
      @(q/cancel link)
      (is (true? (deref handler-ran 500 false)) "finally handler should run even when link is cancelled")))

  (testing "finally handler receives CancellationException when link is cancelled"
    (let [received-error (promise)
          inner          (q/task (Thread/sleep 10000) :result)
          link           (q/finally inner (fn [_ e _] (deliver received-error e)))]
      (Thread/sleep 20)
      @(q/cancel link)
      (is (instance? CancellationException (deref received-error 500 nil))
        "finally handler should receive CancellationException")))

  (testing "cancelled link result is CancellationException, not handler result"
    (let [inner (q/task (Thread/sleep 10000) :result)
          link  (q/finally inner (fn [& _] :handler-result))]
      (Thread/sleep 20)
      @(q/cancel link)
      (is (thrown? CancellationException @link)
        "cancelled link should throw CancellationException")))

  (testing "finally handler exception propagates on normal completion"
    (let [inner (q/task :result)
          link  (q/finally inner (fn [& _] (throw (Exception. "handler error"))))]
      (is (thrown-with-msg? Exception #"handler error" @link))))

  (testing "finally handler exception propagates on error completion"
    (let [inner (q/task (throw (Exception. "inner error")))
          link  (q/finally inner (fn [& _] (throw (Exception. "handler error"))))]
      (is (thrown-with-msg? Exception #"handler error" @link))))

  (testing "finally passes through inner result when handler succeeds"
    (is (= :result @(q/finally (q/task :result) (fn [& _] :ignored)))))

  (testing "finally passes through inner exception when handler succeeds"
    (let [inner (q/task (throw (Exception. "inner error")))
          link  (q/finally inner (fn [& _] :ignored))]
      (is (thrown-with-msg? Exception #"inner error" @link)))))


;; --------------------------------------------------------------------------
;; General principle tests - apply to multiple handlers
;; --------------------------------------------------------------------------

(defn run-handler-doesnt-run-on-cancellation-test
  "Test that a handler doesn't run when the task is cancelled.
   Works with any handler that accepts [& _] args."
  [name handler-fn]
  (testing (format "%s does NOT run on cancellation" name)
    (let [handler-ran (atom false)
          inner       (q/task (Thread/sleep 10000) :result)
          link        (handler-fn inner (fn [& _] (reset! handler-ran true)))]
      (Thread/sleep 20)
      @(q/cancel link)
      (Thread/sleep 100)
      (is (false? @handler-ran)))))


(defn run-handler-exception-propagates-on-success-test
  "Test that when a handler throws on a successful task, the exception propagates.
   Works with handlers that run on success: ok, done, handle, finally."
  [name handler-fn]
  (testing (format "%s exception propagates" name)
    (is (thrown? Exception
          @(-> (q/task 42)
             (handler-fn (fn [& _] (throw (Exception. "Handler error")))))))))


(defn run-handler-exception-propagates-on-error-test
  "Test that when a handler throws on a failed task, the exception propagates.
   Works with handlers that run on error: err, done, handle, catch, finally."
  [name handler-fn]
  (testing (format "%s exception propagates" name)
    (is (thrown-with-msg? Exception #"Handler error"
          @(-> (q/task (throw (Exception. "Original")))
             (handler-fn (fn [& _] (throw (Exception. "Handler error")))))))))


(defn run-handler-passes-through-value-test
  "Test that a side-effecting handler passes through the original value.
   Works with any handler that accepts [& _] args."
  [name handler-fn]
  (testing (format "%s passes through original value" name)
    (is (= 42 @(-> (q/task 42)
                 (handler-fn (fn [& _] :ignored)))))))


(defn run-handler-passes-through-exception-test
  "Test that a side-effecting handler passes through the original exception.
   Works with any handler that accepts [& _] args."
  [name handler-fn]
  (testing (format "%s passes through original exception" name)
    (is (thrown-with-msg? Exception #"Original error"
          @(-> (q/task (throw (Exception. "Original error")))
             (handler-fn (fn [& _] :ignored)))))))


(deftest handlers-dont-run-on-cancellation-test
  (run-handler-doesnt-run-on-cancellation-test "ok" q/ok)
  (run-handler-doesnt-run-on-cancellation-test "ok-cpu" q/ok-cpu)
  (run-handler-doesnt-run-on-cancellation-test "err" q/err)
  (run-handler-doesnt-run-on-cancellation-test "err-cpu" q/err-cpu)
  (run-handler-doesnt-run-on-cancellation-test "done" q/done)
  (run-handler-doesnt-run-on-cancellation-test "done-cpu" q/done-cpu)
  (run-handler-doesnt-run-on-cancellation-test "handle" q/handle)
  (run-handler-doesnt-run-on-cancellation-test "handle-cpu" q/handle-cpu)
  (run-handler-doesnt-run-on-cancellation-test "catch" q/catch)
  (run-handler-doesnt-run-on-cancellation-test "catch-cpu" q/catch-cpu))


(deftest handler-exceptions-propagate-test
  ;; Handlers that run on success
  (run-handler-exception-propagates-on-success-test "ok" q/ok)
  (run-handler-exception-propagates-on-success-test "ok-cpu" q/ok-cpu)
  (run-handler-exception-propagates-on-success-test "done" q/done)
  (run-handler-exception-propagates-on-success-test "done-cpu" q/done-cpu)
  (run-handler-exception-propagates-on-success-test "handle" q/handle)
  (run-handler-exception-propagates-on-success-test "handle-cpu" q/handle-cpu)
  (run-handler-exception-propagates-on-success-test "finally" q/finally)
  (run-handler-exception-propagates-on-success-test "finally-cpu" q/finally-cpu)
  ;; Handlers that run on error
  (run-handler-exception-propagates-on-error-test "err" q/err)
  (run-handler-exception-propagates-on-error-test "err-cpu" q/err-cpu)
  (run-handler-exception-propagates-on-error-test "done" q/done)
  (run-handler-exception-propagates-on-error-test "done-cpu" q/done-cpu)
  (run-handler-exception-propagates-on-error-test "handle" q/handle)
  (run-handler-exception-propagates-on-error-test "handle-cpu" q/handle-cpu)
  (run-handler-exception-propagates-on-error-test "catch" q/catch)
  (run-handler-exception-propagates-on-error-test "catch-cpu" q/catch-cpu)
  (run-handler-exception-propagates-on-error-test "finally" q/finally)
  (run-handler-exception-propagates-on-error-test "finally-cpu" q/finally-cpu))


(deftest side-effect-handlers-pass-through-value-test
  ;; Side-effecting handlers (ok, err, done) pass through the original value
  ;; Transform handlers (then, handle, catch) return a new value
  (run-handler-passes-through-value-test "ok" q/ok)
  (run-handler-passes-through-value-test "ok-cpu" q/ok-cpu)
  (run-handler-passes-through-value-test "err" q/err)
  (run-handler-passes-through-value-test "err-cpu" q/err-cpu)
  (run-handler-passes-through-value-test "done" q/done)
  (run-handler-passes-through-value-test "done-cpu" q/done-cpu)
  (run-handler-passes-through-value-test "finally" q/finally)
  (run-handler-passes-through-value-test "finally-cpu" q/finally-cpu))


(deftest side-effect-handlers-pass-through-exception-test
  ;; Side-effecting handlers pass through the original exception
  (run-handler-passes-through-exception-test "ok" q/ok)
  (run-handler-passes-through-exception-test "ok-cpu" q/ok-cpu)
  (run-handler-passes-through-exception-test "err" q/err)
  (run-handler-passes-through-exception-test "err-cpu" q/err-cpu)
  (run-handler-passes-through-exception-test "done" q/done)
  (run-handler-passes-through-exception-test "done-cpu" q/done-cpu)
  (run-handler-passes-through-exception-test "finally" q/finally)
  (run-handler-passes-through-exception-test "finally-cpu" q/finally-cpu))


(deftest ok-specific-semantics-test
  (testing "ok does NOT run when inner throws"
    (let [handler-ran (atom false)
          inner       (q/task (throw (Exception. "inner error")))
          link        (q/ok inner (fn [v] (reset! handler-ran true)))]
      (try @link (catch Exception _))
      (is (false? @handler-ran))))

  (testing "ok runs on success"
    (let [handler-ran (promise)
          link        (q/ok (q/task :result) (fn [v] (deliver handler-ran v)))]
      @link
      (is (= :result (deref handler-ran 100 :not-run))))))


(defn run-multi-pair-catch-tests
  "Helper to test multi-pair catch with different catch functions."
  [kind catch-fn]
  ;; Test all static arities to ensure vector construction is correct

  (testing (format "1 pair arity (%s)" kind)
    (is (= :caught
          @(-> (q/task (throw (IllegalArgumentException. "bad")))
             (catch-fn IllegalArgumentException (fn [_] :caught))))))

  (testing (format "2 pair arity (%s)" kind)
    (is (= :io-error
          @(-> (q/task (throw (IOException. "io")))
             (catch-fn
               IllegalArgumentException (fn [_] :bad-arg)
               IOException (fn [_] :io-error))))))

  (testing (format "3 pair arity (%s)" kind)
    (is (= :runtime
          @(-> (q/task (throw (RuntimeException. "rt")))
             (catch-fn
               IllegalArgumentException (fn [_] :bad-arg)
               IOException (fn [_] :io-error)
               RuntimeException (fn [_] :runtime))))))

  (testing (format "4 pair arity (%s)" kind)
    (is (= :null-ptr
          @(-> (q/task (throw (NullPointerException. "npe")))
             (catch-fn
               IllegalArgumentException (fn [_] :bad-arg)
               IOException (fn [_] :io-error)
               IllegalStateException (fn [_] :illegal-state)
               NullPointerException (fn [_] :null-ptr))))))

  (testing (format "5 pair arity (%s)" kind)
    (is (= :security
          @(-> (q/task (throw (SecurityException. "sec")))
             (catch-fn
               IllegalArgumentException (fn [_] :bad-arg)
               IOException (fn [_] :io-error)
               IllegalStateException (fn [_] :illegal-state)
               NullPointerException (fn [_] :null-ptr)
               SecurityException (fn [_] :security))))))

  (testing (format "variadic arity 6+ pairs (%s)" kind)
    (is (= :arithmetic
          @(-> (q/task (throw (ArithmeticException. "arith")))
             (catch-fn
               IllegalArgumentException (fn [_] :bad-arg)
               IOException (fn [_] :io-error)
               IllegalStateException (fn [_] :illegal-state)
               NullPointerException (fn [_] :null-ptr)
               SecurityException (fn [_] :security)
               ArithmeticException (fn [_] :arithmetic))))))

  (testing (format "first match wins (%s)" kind)
    ;; RuntimeException is parent of IllegalArgumentException
    ;; but IllegalArgumentException is listed first, so it wins
    (is (= :specific
          @(-> (q/task (throw (IllegalArgumentException. "bad")))
             (catch-fn
               IllegalArgumentException (fn [_] :specific)
               RuntimeException (fn [_] :general))))))

  (testing (format "handler exception propagates (%s)" kind)
    ;; If first handler throws, second handler should NOT catch it
    (is (thrown-with-msg? IOException #"handler threw"
          @(-> (q/task (throw (IllegalArgumentException. "bad")))
             (catch-fn
               IllegalArgumentException (fn [_] (throw (IOException. "handler threw")))
               Throwable (fn [_] :caught-all))))))

  (testing (format "no match propagates original (%s)" kind)
    (is (thrown-with-msg? IOException #"io error"
          @(-> (q/task (throw (IOException. "io error")))
             (catch-fn
               IllegalArgumentException (fn [_] :bad-arg)
               NullPointerException (fn [_] :null-ptr)))))))


(defn run-multi-pair-catch-assertion-tests
  "Helper to test assertion failures with different catch functions."
  [kind catch-fn]
  ;; These tests verify assertions fire for invalid arguments.
  ;; Assertions must be enabled (default in dev/test).

  (testing (format "non-class in type position (%s)" kind)
    (is (thrown? AssertionError
          @(-> (q/task (throw (Exception. "e")))
             (catch-fn "not-a-class" (fn [_] :caught))))))

  (testing (format "non-function in handler position (%s)" kind)
    (is (thrown? AssertionError
          @(-> (q/task (throw (Exception. "e")))
             (catch-fn Exception :not-a-function))))))


(deftest multi-pair-catch-test
  (run-multi-pair-catch-tests "virtual" q/catch)
  (run-multi-pair-catch-tests "cpu" q/catch-cpu))


(deftest multi-pair-catch-assertion-test
  (run-multi-pair-catch-assertion-tests "virtual" q/catch)
  (run-multi-pair-catch-assertion-tests "cpu" q/catch-cpu))


(deftest handler-error-test
  ;; Most "handler exception propagates" tests are covered by handler-exceptions-propagate-test.
  ;; This test verifies a different behavior: ok doesn't run on upstream error.
  (testing "ok handler error doesn't mask original error"
    ;; If ok only runs on success, then it shouldn't run when there's an error
    (let [ok-ran (atom false)]
      (try
        @(-> (q/task (throw (Exception. "Original error")))
           (q/ok (fn [_] (reset! ok-ran true) (throw (Exception. "Ok error")))))
        (catch Exception e
          (is (= "Original error" (ex-message e)))
          (is (false? @ok-ran)))))))


(defn run-ok-tests
  "Helper to test ok-specific behavior."
  [kind ok-fn]
  (testing (format "runs side effect on success (%s)" kind)
    (let [result (atom nil)]
      @(-> (q/task 42)
         (ok-fn (fn [v] (reset! result v))))
      (is (= 42 @result))))

  (testing (format "does not run on failure (%s)" kind)
    (let [ran (atom false)]
      (try @(-> (q/task (throw (Exception. "Boom")))
              (ok-fn (fn [_] (reset! ran true))))
           (catch Exception _))
      (is (false? @ran)))))


(deftest ok-test
  (run-ok-tests "virtual" q/ok)
  (run-ok-tests "cpu" q/ok-cpu))


(defn run-err-tests
  "Helper to test err-specific behavior."
  [kind err-fn]
  (testing (format "runs side effect on failure (%s)" kind)
    (let [result (atom nil)]
      (try
        @(-> (q/task (throw (Exception. "Original error")))
           (err-fn (fn [e] (reset! result e))))
        (catch Exception _))
      (is (instance? Exception @result))
      (is (= "Original error" (ex-message @result)))))

  (testing (format "does not run on success (%s)" kind)
    (let [ran (atom false)]
      @(-> (q/task 42)
         (err-fn (fn [_] (reset! ran true))))
      (is (false? @ran)))))


(deftest err-test
  (run-err-tests "virtual" q/err)
  (run-err-tests "cpu" q/err-cpu))


(defn run-done-tests
  "Helper to test done-specific behavior."
  [kind done-fn]
  (testing (format "runs side effect on success (%s)" kind)
    (let [result (atom nil)]
      @(-> (q/task 42)
         (done-fn (fn [v e]
                    (reset! result {:value v :error e}))))
      (is (= {:value 42 :error nil} @result))))

  (testing (format "runs side effect on failure (%s)" kind)
    (let [result (atom nil)]
      (try
        @(-> (q/task (throw (Exception. "Original error")))
           (done-fn (fn [v e]
                      (reset! result {:value v :error e}))))
        (catch Exception _))
      (is (nil? (:value @result)))
      (is (instance? Exception (:error @result))))))


(deftest done-test
  (run-done-tests "virtual" q/done)
  (run-done-tests "cpu" q/done-cpu))


(defn run-handle-tests
  "Helper to test handle-specific behavior."
  [kind handle-fn]
  (testing (format "with successful task (%s)" kind)
    (is (= :processed
          @(-> (q/task 42)
             (handle-fn (fn [v e]
                          (is (= 42 v))
                          (is (nil? e))
                          :processed))))))

  (testing (format "with failed task (%s)" kind)
    (let [error (Exception. "Test error")]
      (is (= :recovered
            @(-> (q/task (throw error))
               (handle-fn (fn [v e]
                            (is (nil? v))
                            (is (identical? error e))
                            :recovered)))))))

  (testing (format "can transform success value (%s)" kind)
    (is (= 84
          @(-> (q/task 42)
             (handle-fn (fn [v e]
                          (if e
                            0
                            (* v 2))))))))

  (testing (format "can provide fallback for error (%s)" kind)
    (is (= :default
          @(-> (q/task (throw (Exception. "Error")))
             (handle-fn (fn [v e]
                          (if e
                            :default
                            v))))))))


(deftest handle-test
  (run-handle-tests "virtual" q/handle)
  (run-handle-tests "cpu" q/handle-cpu)

  (testing "handle is more fundamental than done"
    ;; done is implemented similarly to handle but for side effects
    (let [finally-result (atom nil)
          handle-result  (atom nil)]
      @(-> (q/task 42)
         (q/finally (fn [v e _] (reset! finally-result [v e]))))
      @(-> (q/task 42)
         (q/handle (fn [v e]
                     (reset! handle-result [v e])
                     (if e (throw e) v))))
      ;; Both should see the same v/e, but handle returns a value
      (is (= @finally-result @handle-result)))))


(deftest cpu-executor-chaining-test
  ;; These tests verify that -cpu variants work correctly without making
  ;; assumptions about which specific executor they use (since cpu-executor
  ;; may be aliased to virtual-executor to avoid blocking issues).

  (testing "then-cpu single task transforms value"
    (is (= 15 @(q/then-cpu (q/task 10)
                 (fn [v] (+ v 5))))))

  (testing "then-cpu multiple tasks combines values"
    (is (= 60 @(q/then-cpu (q/task 10) (q/task 20) (q/task 30)
                 (fn [a b c] (+ a b c))))))

  (testing "ok-cpu runs side effect and passes through value"
    (let [side-effect (atom nil)]
      (is (= 42 @(-> (q/task 42)
                   (q/ok-cpu (fn [v] (reset! side-effect v))))))
      (is (= 42 @side-effect))))

  (testing "finally-cpu runs on success"
    (let [called (atom nil)]
      (is (= 42 @(-> (q/task 42)
                   (q/finally-cpu (fn [v e _] (reset! called {:value v :error e}))))))
      (is (= {:value 42 :error nil} @called))))

  (testing "finally-cpu runs on failure"
    (let [called (atom nil)]
      (is (thrown? Exception
            @(-> (q/task (throw (Exception. "Error")))
               (q/finally-cpu (fn [v e _] (reset! called {:value v :error e}))))))
      (is (nil? (:value @called)))
      (is (some? (:error @called)))))

  (testing "catch-cpu handles error"
    (is (= :caught @(-> (q/task (throw (Exception. "Error")))
                      (q/catch-cpu (fn [e] :caught)))))))
