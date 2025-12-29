(ns co.multiply.quiescent.ancestry-test
  "Tests for task ancestry - verifying that *this* is correctly bound
   in all execution contexts so child tasks register with the right parent."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]]
    [co.multiply.scoped :refer [ask]]))


(use-fixtures :once platform-thread-fixture)


;; Helper to capture *this* without returning it (which would cause grounding issues)
(defn capture-this! [atom]
  (reset! atom (ask impl/*this*))
  nil)


(defn run-then-ancestry-tests
  "Helper to test then ancestry with different then functions."
  [kind then-fn]
  (testing (format "1-arity binds *this* to link task (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task :done)
          link     (then-fn base (fn [_] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link))))

  (testing (format "2-arity binds *this* to link task (%s)" kind)
    (let [this-ref (atom nil)
          link     (then-fn (q/task :a) (q/task :b)
                     (fn [_ _] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link))))

  (testing (format "3-arity binds *this* to link task (%s)" kind)
    (let [this-ref (atom nil)
          link     (then-fn (q/task :a) (q/task :b) (q/task :c)
                     (fn [_ _ _] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link)))))


(deftest then-ancestry-test
  (run-then-ancestry-tests "virtual" q/then)
  (run-then-ancestry-tests "cpu" q/then-cpu))


(defn run-handle-ancestry-tests
  "Helper to test handle ancestry with different handle functions."
  [kind handle-fn]
  (testing (format "binds *this* to link task on success (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task :success)
          link     (handle-fn base (fn [_ _] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link))))

  (testing (format "binds *this* to link task on error (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task (throw (Exception. "error")))
          link     (handle-fn base (fn [_ _] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link)))))


(deftest handle-ancestry-test
  (run-handle-ancestry-tests "virtual" q/handle)
  (run-handle-ancestry-tests "cpu" q/handle-cpu))


(defn run-finally-ancestry-tests
  "Helper to test finally ancestry with different finally functions."
  [kind finally-fn]
  (testing (format "binds *this* to link task on success (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task :success)
          link     (finally-fn base (fn [& _] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link))))

  (testing (format "binds *this* to link task on error (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task (throw (Exception. "error")))
          link     (finally-fn base (fn [& _] (capture-this! this-ref)))]
      (try @link (catch Exception _))
      (is (identical? @this-ref link)))))


(deftest finally-ancestry-test
  (run-finally-ancestry-tests "virtual" q/finally)
  (run-finally-ancestry-tests "cpu" q/finally-cpu)

  ;; Cancellation case is specific to finally (it's the only handler that runs on cancellation)
  (testing "binds *this* to link task on cancellation"
    (let [this-ref (atom nil)
          started  (promise)
          base     (q/task (deliver started true) (Thread/sleep 10000) :result)
          link     (q/finally base (fn [& _] (capture-this! this-ref)))]
      @started
      @(q/cancel base)
      (try @link (catch Exception _))
      (is (identical? @this-ref link)))))


(defn run-ok-ancestry-tests
  "Helper to test ok ancestry with different ok functions."
  [kind ok-fn]
  (testing (format "binds *this* to link task (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task :success)
          link     (ok-fn base (fn [_] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link)))))


(deftest ok-ancestry-test
  (run-ok-ancestry-tests "virtual" q/ok)
  (run-ok-ancestry-tests "cpu" q/ok-cpu))


(defn run-err-ancestry-tests
  "Helper to test err ancestry with different err functions."
  [kind err-fn]
  (testing (format "binds *this* to link task (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task (throw (Exception. "error")))
          link     (err-fn base (fn [_] (capture-this! this-ref)))]
      (try @link (catch Exception _))
      (is (identical? @this-ref link)))))


(deftest err-ancestry-test
  (run-err-ancestry-tests "virtual" q/err)
  (run-err-ancestry-tests "cpu" q/err-cpu))


(defn run-catch-ancestry-tests
  "Helper to test catch ancestry with different catch functions."
  [kind catch-fn]
  (testing (format "binds *this* to link task (%s)" kind)
    (let [this-ref (atom nil)
          base     (q/task (throw (Exception. "error")))
          link     (catch-fn base (fn [_] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link)))))


(deftest catch-ancestry-test
  (run-catch-ancestry-tests "virtual" q/catch)
  (run-catch-ancestry-tests "cpu" q/catch-cpu)

  ;; Multi-pair catch with type is tested separately (not a CPU variant concern)
  (testing "catch with type binds *this* to link task"
    (let [this-ref (atom nil)
          base     (q/task (throw (IllegalArgumentException. "bad arg")))
          link     (q/catch base
                     IllegalArgumentException (fn [_] (capture-this! this-ref)))]
      @link
      (is (identical? @this-ref link)))))


(deftest child-registration-test
  (testing "Child created in then registers with link, not outer task"
    (let [child-cancelled? (atom false)
          child-started?   (promise)
          outer            (q/task
                             (let [link (q/then (q/task :done)
                                          (fn [_]
                                            (let [child (q/task
                                                          (deliver child-started? true)
                                                          (try
                                                            (Thread/sleep 10000)
                                                            :child-done
                                                            (catch InterruptedException _
                                                              (reset! child-cancelled? true)
                                                              :interrupted)))]
                                              @child)))]
                               @link))]
      @child-started?
      ;; Cancel outer - this should cascade to link, which should cascade to child
      @(q/cancel outer)
      (Thread/sleep 100)
      (is @child-cancelled?
        "Child in then should be cancelled when outer is cancelled (via link)")))

  (testing "Child created in finally registers with link"
    (let [child-cancelled? (atom false)
          child-started?   (promise)
          base             (q/task :done)
          link             (q/finally base
                             (fn [& _]
                               (let [child (q/task
                                             (deliver child-started? true)
                                             (try
                                               (Thread/sleep 10000)
                                               :child-done
                                               (catch InterruptedException _
                                                 (reset! child-cancelled? true)
                                                 :interrupted)))]
                                 @child)))]
      @child-started?
      @(q/cancel link)
      (Thread/sleep 100)
      (is @child-cancelled?
        "Child in finally should be cancelled when link is cancelled"))))
