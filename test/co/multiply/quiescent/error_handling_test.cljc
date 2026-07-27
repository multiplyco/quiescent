(ns co.multiply.quiescent.error-handling-test
  (:require
    [clojure.test :refer [is]]
    [co.multiply.quiescent :as q :refer [q]]
    [co.multiply.quiescent.test-support :refer [clause def-results make-exception result with-task]]))


;; # Exception scaffolding for cross-platform catch tests
;; ################################################################################

(defn illegal-argument-exception
  [msg]
  #?(:clj  (IllegalArgumentException. ^String msg)
     :cljs (ex-info msg {:illegal-argument true})))


(def illegal-argument-clause
  #?(:clj  IllegalArgumentException
     :cljs #(-> % ex-data :illegal-argument some?)))


(defn io-exception
  [msg]
  #?(:clj  (java.io.IOException. ^String msg)
     :cljs (ex-info msg {:io-exception true})))


(def io-clause
  #?(:clj  java.io.IOException
     :cljs #(-> % ex-data :io-exception some?)))


(defn runtime-exception
  [msg]
  #?(:clj  (RuntimeException. ^String msg)
     :cljs (ex-info msg {:runtime-exception true})))


(def runtime-clause
  #?(:clj  RuntimeException
     :cljs #(-> % ex-data :runtime-exception some?)))


(defn null-pointer-exception
  [msg]
  #?(:clj  (NullPointerException. ^String msg)
     :cljs (ex-info msg {:null-pointer true})))


(def null-pointer-clause
  #?(:clj  NullPointerException
     :cljs #(-> % ex-data :null-pointer some?)))


(defn illegal-state-exception
  [msg]
  #?(:clj  (IllegalStateException. ^String msg)
     :cljs (ex-info msg {:illegal-state true})))


(def illegal-state-clause
  #?(:clj  IllegalStateException
     :cljs #(-> % ex-data :illegal-state some?)))


(defn security-exception
  [msg]
  #?(:clj  (SecurityException. ^String msg)
     :cljs (ex-info msg {:security-exception true})))


(def security-clause
  #?(:clj  SecurityException
     :cljs #(-> % ex-data :security-exception some?)))


(defn arithmetic-exception
  [msg]
  #?(:clj  (ArithmeticException. ^String msg)
     :cljs (ex-info msg {:arithmetic-exception true})))


(def arithmetic-clause
  #?(:clj  ArithmeticException
     :cljs #(-> % ex-data :arithmetic-exception some?)))


(def-results error-propagation-test
  (clause :basic-then "Error in then chain"
    (with-task (-> (q/task 10)
                 (q/then (fn [_] (throw (make-exception "Error!"))))
                 (q/then inc))
      (result [_v e c]
        (is (some? e))
        (is (false? c)))))

  (clause :basic-catch "Catch specific exception type"
    (with-task (-> (q/task (throw (illegal-argument-exception "Bad arg")))
                 (q/catch illegal-argument-clause (constantly :caught)))
      (result [v]
        (is (= :caught v)))))

  (clause :catch-on-success "Catch doesn't affect successful tasks"
    (with-task (-> (q/task 42)
                 (q/catch illegal-argument-clause (fn [_] :error)))
      (result [v]
        (is (= 42 v)))))

  (clause :catch-uncaught "Catch with non-matching class propagates original exception"
    (with-task (-> (q/task (throw (make-exception "IO error")))
                 (q/catch illegal-argument-clause (fn [_] :caught)))
      (result [v e c]
        (is (some? e) "Threw exception.")
        (is (false? c) "Not cancelled."))))

  (clause :finally-on-success "Finally runs on success"
    (let [ran  (q/promise)
          task (-> (q/task 42)
                 (q/finally (fn [& _] (ran true))))]
      (with-task [task ran]
        (result [v]
          (is (= [42 true] v))))))

  (clause :finally-on-error "Finally runs on error"
    (let [ran  (q/promise)
          task (-> (q/task (throw (make-exception "Oh no!")))
                 (q/finally (fn [& _] (ran true))))]
      (with-task ran
        (result [v]
          (is (true? v))
          (is (q/exceptional? task))))))

  (clause :ok-on-success "Ok runs on success"
    (let [ran (atom false)]
      (with-task (-> (q/task 42)
                   (q/ok (fn [v] (reset! ran v)))
                   (q/await))
        (result []
          (is (= 42 @ran))))))

  (clause :ok-on-error "Ok doesn't run on error"
    (let [ran (atom false)]
      (with-task (-> (q/task (throw (make-exception "Boom")))
                   (q/ok (fn [& _] (reset! ran true)))
                   (q/await))
        (result []
          (is (false? @ran))))))

  (clause :err-on-error "Err runs on error"
    (let [ran (atom false)]
      (with-task (-> (q/task (throw (make-exception "Boom")))
                   (q/err (fn [_] (reset! ran true)))
                   (q/await))
        (result []
          (is (true? @ran))))))

  (clause :err-on-success "Err doesn't run on success"
    (let [ran (atom false)]
      (with-task (-> (q/task 42)
                   (q/err (fn [& _] (reset! ran true)))
                   (q/await))
        (result []
          (is (false? @ran))))))

  (clause :done-on-success "Done runs on success with value and nil error"
    (let [received (atom nil)]
      (with-task (-> (q/task 42)
                   (q/done (fn [v e] (reset! received {:value v :error e})))
                   (q/await))
        (result []
          (is (= {:value 42 :error nil} @received))))))

  (clause :done-on-error "Done runs on error with nil value and error"
    (let [received (atom nil)]
      (with-task (-> (q/task (throw (make-exception "Boom")))
                   (q/done (fn [v e] (reset! received {:value v :error e})))
                   (q/await))
        (result []
          (is (nil? (:value @received)))
          (is (some? (:error @received)))))))

  (clause :handle-on-success "Handle transforms successful value"
    (with-task (-> (q/task 42)
                 (q/handle (fn [v e]
                             (if e :error (* v 2)))))
      (result [v]
        (is (= 84 v)))))

  (clause :handle-on-error "Handle recovers from error"
    (with-task (-> (q/task (throw (make-exception "Boom")))
                 (q/handle (fn [v e]
                             (if e :recovered v))))
      (result [v]
        (is (= :recovered v)))))

  (clause :nested-structure "Error in nested structure cancels all tasks"
    (with-task (q {:a (q/task 1)
                   :b (q/task (throw (make-exception "Boom!")))
                   :c (q/task 3)})
      (result [_v e c]
        (is (some? e))
        (is (false? c)))))

  ;; `monitor` is the deliberate exception to the rule these clauses establish:
  ;; its side effect fires while the monitored task is still in flight, so
  ;; propagating would cancel live work rather than merely report on finished
  ;; work. Its exception is contained instead — see `monitor-test/:contained`.

  (clause :nested-structure-siblings "Error in nested structure cancels siblings"
    (let [sibling (q/sleep 1000 :never)]
      (with-task (q {:a sibling
                     :b (q/task (throw (make-exception "Boom!")))})
        (result [_v e c]
          (is (some? e))
          (is (false? c))
          (is (q/cancelled? sibling)))))))


(defn run-handler-exception-propagates-on-success-test
  "Test that when a handler throws on a successful task, the exception propagates.
   Works with handlers that run on success: ok, done, handle, finally."
  [handler-fn]
  (let [e1 (make-exception "Handler error")]
    (with-task (-> (q/task 42)
                 (handler-fn (fn [& _] (throw e1))))
      (result [_v e]
        (is (identical? e1 e) "")))))


(defn run-handler-exception-propagates-on-error-test
  "Test that when a handler throws on a failed task, the exception propagates.
   Works with handlers that run on error: err, done, handle, catch, finally."
  [handler-fn]
  (let [e1 (make-exception "Original")]
    (with-task (-> (q/task (throw e1))
                 (handler-fn (fn [& _] (throw e1))))
      (result [_v e]
        (is (identical? e1 e))))))


(defn run-handler-passes-through-value-test
  "Test that a side-effecting handler passes through the original value.
   Works with any handler that accepts [& _] args."
  [handler-fn]
  (with-task (-> (q/task 42)
               (handler-fn (fn [& _] :ignored)))
    (result [v]
      (is (= 42 v)))))


(defn run-handler-passes-through-exception-test
  "Test that a side-effecting handler passes through the original exception.
   Works with any handler that accepts [& _] args."
  [handler-fn]
  (let [e1 (make-exception "Original error")]
    (with-task (-> (q/task (throw e1))
                 (handler-fn (fn [& _] :ignored)))
      (result [_v e]
        (is (identical? e1 e))))))


;; # Multi-pair catch
;; ################################################################################

(def-results multi-pair-catch-test
  (clause :one-pair "1 pair arity"
    (with-task (-> (q/task (throw (illegal-argument-exception "bad")))
                 (q/catch illegal-argument-clause (fn [_] :caught)))
      (result [v]
        (is (= :caught v)))))

  (clause :two-pair "2 pair arity"
    (with-task (-> (q/task (throw (io-exception "io")))
                 (q/catch
                   illegal-argument-clause (fn [_] :bad-arg)
                   io-clause (fn [_] :io-error)))
      (result [v]
        (is (= :io-error v)))))

  (clause :three-pair "3 pair arity"
    (with-task (-> (q/task (throw (runtime-exception "rt")))
                 (q/catch
                   illegal-argument-clause (fn [_] :bad-arg)
                   io-clause (fn [_] :io-error)
                   runtime-clause (fn [_] :runtime)))
      (result [v]
        (is (= :runtime v)))))

  (clause :four-pair "4 pair arity"
    (with-task (-> (q/task (throw (null-pointer-exception "npe")))
                 (q/catch
                   illegal-argument-clause (fn [_] :bad-arg)
                   io-clause (fn [_] :io-error)
                   illegal-state-clause (fn [_] :illegal-state)
                   null-pointer-clause (fn [_] :null-ptr)))
      (result [v]
        (is (= :null-ptr v)))))

  (clause :five-pair "5 pair arity"
    (with-task (-> (q/task (throw (security-exception "sec")))
                 (q/catch
                   illegal-argument-clause (fn [_] :bad-arg)
                   io-clause (fn [_] :io-error)
                   illegal-state-clause (fn [_] :illegal-state)
                   null-pointer-clause (fn [_] :null-ptr)
                   security-clause (fn [_] :security)))
      (result [v]
        (is (= :security v)))))

  (clause :six-pair "variadic arity 6+ pairs"
    (with-task (-> (q/task (throw (arithmetic-exception "arith")))
                 (q/catch
                   illegal-argument-clause (fn [_] :bad-arg)
                   io-clause (fn [_] :io-error)
                   illegal-state-clause (fn [_] :illegal-state)
                   null-pointer-clause (fn [_] :null-ptr)
                   security-clause (fn [_] :security)
                   arithmetic-clause (fn [_] :arithmetic)))
      (result [v]
        (is (= :arithmetic v)))))

  (clause :first-match-wins "first match wins"
    (with-task (-> (q/task (throw (illegal-argument-exception "bad")))
                 (q/catch
                   illegal-argument-clause (fn [_] :specific)
                   runtime-clause (fn [_] :general)))
      (result [v]
        (is (= :specific v)))))

  (clause :handler-exception-propagates "handler exception propagates"
    (let [e1 (io-exception "handler threw")]
      (with-task (-> (q/task (throw (illegal-argument-exception "bad")))
                   (q/catch
                     illegal-argument-clause (fn [_] (throw e1))
                     io-clause (fn [_] :caught-all)))
        (result [_v e]
          (is (identical? e1 e))))))

  (clause :no-match-propagates "no match propagates original"
    (let [e1 (io-exception "io error")]
      (with-task (-> (q/task (throw e1))
                   (q/catch
                     illegal-argument-clause (fn [_] :bad-arg)
                     null-pointer-clause (fn [_] :null-ptr)))
        (result [_v e]
          (is (identical? e1 e)))))))


;; # Handler exception propagation
;; ################################################################################

(def-results handler-exception-on-success-test
  (clause :ok "ok exception propagates on success"
    (run-handler-exception-propagates-on-success-test q/ok))
  (clause :done "done exception propagates on success"
    (run-handler-exception-propagates-on-success-test q/done))
  (clause :handle "handle exception propagates on success"
    (run-handler-exception-propagates-on-success-test q/handle))
  (clause :finally "finally exception propagates on success"
    (run-handler-exception-propagates-on-success-test q/finally)))


(def-results handler-exception-on-error-test
  (clause :err "err exception propagates on error"
    (run-handler-exception-propagates-on-error-test q/err))
  (clause :done "done exception propagates on error"
    (run-handler-exception-propagates-on-error-test q/done))
  (clause :handle "handle exception propagates on error"
    (run-handler-exception-propagates-on-error-test q/handle))
  (clause :catch "catch exception propagates on error"
    (run-handler-exception-propagates-on-error-test q/catch))
  (clause :finally "finally exception propagates on error"
    (run-handler-exception-propagates-on-error-test q/finally)))


;; # Side-effect handlers pass-through
;; ################################################################################

(def-results handler-passes-through-value-test
  (clause :ok "ok passes through value"
    (run-handler-passes-through-value-test q/ok))
  (clause :err "err passes through value"
    (run-handler-passes-through-value-test q/err))
  (clause :done "done passes through value"
    (run-handler-passes-through-value-test q/done))
  (clause :finally "finally passes through value"
    (run-handler-passes-through-value-test q/finally)))


(def-results handler-passes-through-exception-test
  (clause :ok "ok passes through exception"
    (run-handler-passes-through-exception-test q/ok))
  (clause :err "err passes through exception"
    (run-handler-passes-through-exception-test q/err))
  (clause :done "done passes through exception"
    (run-handler-passes-through-exception-test q/done))
  (clause :finally "finally passes through exception"
    (run-handler-passes-through-exception-test q/finally)))


;; # CPU handler variants (CLJ only)
;; ################################################################################

#?(:clj (def-results handler-exception-on-success-cpu-test
          (clause :ok-cpu "ok-cpu exception propagates on success"
            (run-handler-exception-propagates-on-success-test q/ok-cpu))
          (clause :done-cpu "done-cpu exception propagates on success"
            (run-handler-exception-propagates-on-success-test q/done-cpu))
          (clause :handle-cpu "handle-cpu exception propagates on success"
            (run-handler-exception-propagates-on-success-test q/handle-cpu))
          (clause :finally-cpu "finally-cpu exception propagates on success"
            (run-handler-exception-propagates-on-success-test q/finally-cpu))))


#?(:clj (def-results handler-exception-on-error-cpu-test
          (clause :err-cpu "err-cpu exception propagates on error"
            (run-handler-exception-propagates-on-error-test q/err-cpu))
          (clause :done-cpu "done-cpu exception propagates on error"
            (run-handler-exception-propagates-on-error-test q/done-cpu))
          (clause :handle-cpu "handle-cpu exception propagates on error"
            (run-handler-exception-propagates-on-error-test q/handle-cpu))
          (clause :catch-cpu "catch-cpu exception propagates on error"
            (run-handler-exception-propagates-on-error-test q/catch-cpu))
          (clause :finally-cpu "finally-cpu exception propagates on error"
            (run-handler-exception-propagates-on-error-test q/finally-cpu))))


#?(:clj (def-results handler-passes-through-value-cpu-test
          (clause :ok-cpu "ok-cpu passes through value"
            (run-handler-passes-through-value-test q/ok-cpu))
          (clause :err-cpu "err-cpu passes through value"
            (run-handler-passes-through-value-test q/err-cpu))
          (clause :done-cpu "done-cpu passes through value"
            (run-handler-passes-through-value-test q/done-cpu))
          (clause :finally-cpu "finally-cpu passes through value"
            (run-handler-passes-through-value-test q/finally-cpu))))


#?(:clj (def-results handler-passes-through-exception-cpu-test
          (clause :ok-cpu "ok-cpu passes through exception"
            (run-handler-passes-through-exception-test q/ok-cpu))
          (clause :err-cpu "err-cpu passes through exception"
            (run-handler-passes-through-exception-test q/err-cpu))
          (clause :done-cpu "done-cpu passes through exception"
            (run-handler-passes-through-exception-test q/done-cpu))
          (clause :finally-cpu "finally-cpu passes through exception"
            (run-handler-passes-through-exception-test q/finally-cpu))))


(def-results multi-pair-catch-assertion-test
  (clause :non-class-in-type-position "non-class in type position throws assertion"
    (with-task (q/task
                 (try (-> (q/task (throw (make-exception "e")))
                        (q/catch
                          "not-a-class" (fn [_] :caught)))
                   false
                   (catch #?(:clj AssertionError :cljs js/Error) _e true)))
      (result [v]
        (is (true? v)))))

  (clause :non-function-in-handler-position "non-function in handler position throws assertion"
    (with-task (q/task
                 (try (-> (q/task (throw (make-exception "e")))
                        (q/catch #?(:clj Exception :cljs (fn [_] true)) :not-a-function))
                   false
                   (catch #?(:clj AssertionError :cljs js/Error) _e true)))
      (result [v]
        (is (true? v))))))


#?(:clj (def-results cpu-executor-chaining-test
          (clause :then-cpu-single "then-cpu single task transforms value"
            (with-task (q/then-cpu (q/task 10) (fn [v] (+ v 5)))
              (result [v]
                (is (= 15 v)))))

          (clause :then-cpu-multiple "then-cpu multiple tasks combines values"
            (with-task (q/then-cpu (q/task 10) (q/task 20) (q/task 30)
                         (fn [a b c] (+ a b c)))
              (result [v]
                (is (= 60 v)))))

          (clause :ok-cpu-side-effect "ok-cpu runs side effect and passes through value"
            (let [side-effect (atom nil)]
              (with-task (-> (q/task 42)
                           (q/ok-cpu (fn [v] (reset! side-effect v))))
                (result [v]
                  (is (= 42 v))
                  (is (= 42 @side-effect))))))

          (clause :finally-cpu-on-success "finally-cpu runs on success"
            (let [called (atom nil)]
              (with-task (-> (q/task 42)
                           (q/finally-cpu (fn [v e _] (reset! called {:value v :error e}))))
                (result [v]
                  (is (= 42 v))
                  (is (= {:value 42 :error nil} @called))))))

          (clause :finally-cpu-on-failure "finally-cpu runs on failure"
            (let [called (atom nil)]
              (with-task (-> (q/task (throw (make-exception "Error")))
                           (q/finally-cpu (fn [v e _] (reset! called {:value v :error e}))))
                (result [_v e]
                  (is (some? e))
                  (is (nil? (:value @called)))
                  (is (some? (:error @called)))))))

          (clause :catch-cpu-handles-error "catch-cpu handles error"
            (with-task (-> (q/task (throw (make-exception "Error")))
                         (q/catch-cpu (fn [_] :caught)))
              (result [v]
                (is (= :caught v)))))))


(def-results exceptional-test
  (clause :incomplete-false "Returns false for incomplete task"
    (let [task (q/sleep 1000 :result)]
      (is (false? (q/exceptional? task)))))

  (clause :complete-false "Returns false for successfully completed task"
    (let [task (q 42)]
      (with-task task
        (result []
          (is (false? (q/exceptional? task)))))))

  (clause :error-true "Returns true for exceptionally completed task"
    (let [task (q/task (throw (make-exception "Error")))]
      (with-task task
        (result []
          (is (true? (q/exceptional? task)))))))

  (clause :cancelled-true "Returns true for cancelled task"
    (let [task (doto (q/sleep 1000 :result) (q/cancel))]
      (with-task task
        (result []
          (is (true? (q/exceptional? task))))))))


(def-results failed-task-test
  (clause :created "failed-task creates pre-failed task"
    (let [e1   (make-exception "Already failed")
          task (q/failed-task e1)]
      (with-task task
        (result [v e c]
          (is (realized? task))
          (is (identical? e1 e))
          (is (false? c)))))))
