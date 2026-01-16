(ns co.multiply.quiescent.ancestry-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.test-support :refer [allow-platform-park make-exception with-task]]
    [co.multiply.scoped :refer [ask]]))


(allow-platform-park)


(defn capture-this!
  "Helper to capture *this* without returning it (which would cause grounding issues)"
  [atom]
  (reset! atom (ask impl/*this*))
  nil)


(defn run-then-ancestry-tests-1
  "Helper to test then ancestry with different then functions."
  [kind then-fn]
  (testing (str "1-arity binds *this* to link task (" kind ")")
    (let [this-ref (atom nil)
          link     (then-fn (q/task :a)
                     (fn [& _] (capture-this! this-ref)))]
      (with-task link
        (fn [_v _e _c]
          (is (identical? @this-ref link)))))))


(defn run-then-ancestry-tests-2
  "Helper to test then ancestry with different then functions."
  [kind then-fn]
  (testing (str "2-arity binds *this* to link task (" kind ")")
    (let [this-ref (atom nil)
          link     (then-fn (q/task :a) (q/task :b)
                     (fn [& _] (capture-this! this-ref)))]
      (with-task link
        (fn [_v _e _c]
          (is (identical? @this-ref link)))))))


(defn run-then-ancestry-tests-3
  "Helper to test then ancestry with different then functions."
  [kind then-fn]
  (testing (str "3-arity binds *this* to link task (" kind ")")
    (let [this-ref (atom nil)
          link     (then-fn (q/task :a) (q/task :b) (q/task :c)
                     (fn [& _] (capture-this! this-ref)))]
      (with-task link
        (fn [_v _e _c]
          (is (identical? @this-ref link)))))))


(deftest then-ancestry-test-1
  (run-then-ancestry-tests-1 "virtual" q/then)
  #?(:clj (run-then-ancestry-tests-1 "cpu" q/then-cpu)))


(deftest then-ancestry-test-2
  (run-then-ancestry-tests-2 "virtual" q/then)
  #?(:clj (run-then-ancestry-tests-2 "cpu" q/then-cpu)))


(deftest then-ancestry-test-3
  (run-then-ancestry-tests-3 "virtual" q/then)
  #?(:clj (run-then-ancestry-tests-3 "cpu" q/then-cpu)))


(defn run-chain-ancestry-tests-1
  "Helper to test then ancestry with different then functions."
  [kind handle-fn body-fn]
  (testing (str "3-arity binds *this* to link task (" kind ")")
    (let [this-ref (atom nil)
          link     (handle-fn (q/task (body-fn))
                     (fn [& _] (capture-this! this-ref)))]
      (with-task link
        (fn [_v _e _c]
          (is (identical? @this-ref link)))))))


(deftest handle-ancestry-test-success
  (run-chain-ancestry-tests-1 "virtual" q/handle (constantly :a))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/handle (constantly :a))))


(deftest handle-ancestry-test-error
  (run-chain-ancestry-tests-1 "virtual" q/handle #(throw (make-exception "Oh no!")))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/handle #(throw (make-exception "Oh no!")))))


;; finally tests - success and error cases (cancellation case stays CLJ-only in ancestry_test.clj)

(deftest finally-ancestry-test-success
  (run-chain-ancestry-tests-1 "virtual" q/finally (constantly :success))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/finally-cpu (constantly :success))))


(deftest finally-ancestry-test-error
  (run-chain-ancestry-tests-1 "virtual" q/finally #(throw (make-exception "error")))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/finally-cpu #(throw (make-exception "error")))))


;; ok tests - only runs on success

(deftest ok-ancestry-test
  (run-chain-ancestry-tests-1 "virtual" q/ok (constantly :success))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/ok-cpu (constantly :success))))


;; err tests - only runs on error

(deftest err-ancestry-test
  (run-chain-ancestry-tests-1 "virtual" q/err #(throw (make-exception "error")))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/err-cpu #(throw (make-exception "error")))))


;; catch tests - only runs on error

(deftest catch-ancestry-test
  (run-chain-ancestry-tests-1 "virtual" q/catch #(throw (make-exception "error")))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/catch-cpu #(throw (make-exception "error")))))


;; done tests - runs on completion (success or error)

(deftest done-ancestry-test-success
  (run-chain-ancestry-tests-1 "virtual" q/done (constantly :success))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/done-cpu (constantly :success))))


(deftest done-ancestry-test-error
  (run-chain-ancestry-tests-1 "virtual" q/done #(throw (make-exception "error")))
  #?(:clj (run-chain-ancestry-tests-1 "cpu" q/done-cpu #(throw (make-exception "error")))))


#?(:clj (deftest child-registration-test
          (testing "Child created in then registers with link, not outer task"
            (let [child-cancelled (promise)
                  child-started   (promise)
                  outer           (q/task
                                    @(q/then (q/task :done)
                                       (fn [_]
                                         @(q/task
                                            (deliver child-started true)
                                            (try @(promise)
                                              (catch InterruptedException _
                                                (deliver child-cancelled true)
                                                :interrupted))))))]
              @child-started
              ;; Cancel outer - this should cascade to link, which should cascade to child
              @(q/cancel outer)
              (is @child-cancelled "Child in then should be cancelled when outer is cancelled (via link)")))

          (testing "Child created in finally registers with link"
            (let [child-cancelled (promise)
                  child-started   (promise)
                  link            (q/finally (q/task :done)
                                             (fn [& _]
                                               @(q/task
                                                  (deliver child-started true)
                                                  (try @(promise)
                                                    (catch InterruptedException _
                                                      (deliver child-cancelled true)
                                                      :interrupted)))))]
              @child-started
              @(q/cancel link)
              (is @child-cancelled "Child in finally should be cancelled when link is cancelled")))))
