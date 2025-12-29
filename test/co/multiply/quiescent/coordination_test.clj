(ns co.multiply.quiescent.coordination-test
  "Tests for task coordination primitives: variadic then, all-of, qfor,
   qmerge, race, qdo, and related coordination functions."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]])
  (:import
    [java.util.concurrent CancellationException]))


(use-fixtures :once platform-thread-fixture)


(defn- log-run
  "Helper that returns a task which adds n to the ran atom."
  [ran n]
  (q/task (swap! ran conj n)))


(deftest variadic-then-test
  (testing "Single task then"
    (is (= 11 @(q/then (q/task 10) inc))))

  (testing "Multiple scalars then"
    (is (= 60 @(q/then 10 20 30 +))))

  (testing "Multiple tasks then"
    (is (= 60 @(q/then (q/task 10) (q/task 20) (q/task 30) +))))

  (testing "Multiple tasks with destructuring-like behavior"
    (is (= {:sum 30 :product 200}
          @(q/then (q/task 10) (q/task 20)
             (fn [a b]
               {:sum     (+ a b)
                :product (* a b)}))))))


(defn run-then-arity-tests
  "Helper to test all arities of then with different then functions."
  [kind then-fn]
  (testing (format "fixed arities 1-10 (%s)" kind)
    (is (= 1 @(then-fn 1 identity)) "arity 1")
    (is (= 3 @(then-fn 1 2 +)) "arity 2")
    (is (= 6 @(then-fn 1 2 3 +)) "arity 3")
    (is (= 10 @(then-fn 1 2 3 4 +)) "arity 4")
    (is (= 15 @(then-fn 1 2 3 4 5 +)) "arity 5")
    (is (= 21 @(then-fn 1 2 3 4 5 6 +)) "arity 6")
    (is (= 28 @(then-fn 1 2 3 4 5 6 7 +)) "arity 7")
    (is (= 36 @(then-fn 1 2 3 4 5 6 7 8 +)) "arity 8")
    (is (= 45 @(then-fn 1 2 3 4 5 6 7 8 9 +)) "arity 9")
    (is (= 55 @(then-fn 1 2 3 4 5 6 7 8 9 10 +)) "arity 10"))

  (testing (format "variadic arity 11+ (%s)" kind)
    (is (= 66 @(then-fn 1 2 3 4 5 6 7 8 9 10 11 +)) "arity 11")
    (is (= 78 @(then-fn 1 2 3 4 5 6 7 8 9 10 11 12 +)) "arity 12")))


(deftest then-all-arities-test
  (run-then-arity-tests "virtual" q/then)
  (run-then-arity-tests "cpu" q/then-cpu))


(deftest qmerge-all-arities-test
  (testing "All fixed arities of q/qmerge work correctly"
    (is (= {} @(q/qmerge)) "arity 0")
    (is (= {:a 1} @(q/qmerge {:a 1})) "arity 1")
    (is (= {:a 1 :b 2} @(q/qmerge {:a 1} {:b 2})) "arity 2")
    (is (= {:a 1 :b 2 :c 3} @(q/qmerge {:a 1} {:b 2} {:c 3})) "arity 3")
    (is (= {:a 1 :b 2 :c 3 :d 4} @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4})) "arity 4")
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5} @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5})) "arity 5")
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6} @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6})) "arity 6")
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7} @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7})) "arity 7")
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8} @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8})) "arity 8")
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9} @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9})) "arity 9")
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9 :j 10} @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9} {:j 10})) "arity 10"))

  (testing "Variadic arity (11+) of q/qmerge works correctly"
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9 :j 10 :k 11}
          @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9} {:j 10} {:k 11})) "arity 11")
    (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9 :j 10 :k 11 :l 12}
          @(q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9} {:j 10} {:k 11} {:l 12})) "arity 12")))


(deftest qdo-all-arities-test
  (testing "All fixed arities of q/qdo work correctly"
    (is (nil? @(q/qdo)) "arity 0")
    (is (= :a @(q/qdo :a)) "arity 1")
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) :done)) "arity 2 returns last")
      (is (= #{1} @ran) "arity 2 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) :done)) "arity 3 returns last")
      (is (= #{1 2} @ran) "arity 3 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) :done)) "arity 4 returns last")
      (is (= #{1 2 3} @ran) "arity 4 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) :done)) "arity 5 returns last")
      (is (= #{1 2 3 4} @ran) "arity 5 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) :done)) "arity 6 returns last")
      (is (= #{1 2 3 4 5} @ran) "arity 6 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) :done)) "arity 7 returns last")
      (is (= #{1 2 3 4 5 6} @ran) "arity 7 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) :done)) "arity 8 returns last")
      (is (= #{1 2 3 4 5 6 7} @ran) "arity 8 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) :done)) "arity 9 returns last")
      (is (= #{1 2 3 4 5 6 7 8} @ran) "arity 9 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) (log-run ran 9) :done)) "arity 10 returns last")
      (is (= #{1 2 3 4 5 6 7 8 9} @ran) "arity 10 runs all")))

  (testing "Variadic arity (11+) of q/qdo works correctly"
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) (log-run ran 9) (log-run ran 10) :done)) "arity 11 returns last")
      (is (= #{1 2 3 4 5 6 7 8 9 10} @ran) "arity 11 runs all"))
    (let [ran (atom #{})]
      (is (= :done @(q/qdo (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) (log-run ran 9) (log-run ran 10) (log-run ran 11) :done)) "arity 12 returns last")
      (is (= #{1 2 3 4 5 6 7 8 9 10 11} @ran) "arity 12 runs all"))))


(deftest task-coordination-test
  (testing "qfor parallel map"
    (is (= [2 4 6] @(q/qfor [x [1 2 3]]
                      (q/task (* x 2))))))

  (testing "qmerge merges maps"
    (is (= {:a 1 :b 2 :c 3} @(q/qmerge {:a (q/task 1)}
                               {:b (q/task 2)}
                               {:c (q/task 3)}))))

  (testing "qmerge with single map"
    (is (= {:a 1} @(q/qmerge {:a (q/task 1)}))))

  (testing "race returns first completed"
    (let [result @(q/race
                    (q/task (Thread/sleep 100) :slow)
                    (q/task (Thread/sleep 10) :fast))]
      (is (= :fast result))))

  (testing "race with immediate value"
    (let [result @(q/race :immediate (q/task (Thread/sleep 100) :slow))]
      (is (= :immediate result)))))


(deftest race-all-fail-test
  (testing "Race collects all errors when all tasks fail"
    (let [e1     (Exception. "Error 1")
          e2     (Exception. "Error 2")
          e3     (Exception. "Error 3")
          result (q/race
                   (q/task (throw e1))
                   (q/task (throw e2))
                   (q/task (throw e3)))]
      (try
        @result
        (is false "Should have thrown")
        (catch Exception e
          ;; Should get ex-info with all errors
          (is (= "All tasks failed." (ex-message e)))
          (let [errors (-> e ex-data :errors)]
            (is (= 3 (count errors)))
            (is (some #{e1} errors))
            (is (some #{e2} errors))
            (is (some #{e3} errors)))))))

  (testing "Race with single failure throws that error directly"
    (let [e (Exception. "Single error")]
      (try
        @(q/race (q/task (throw e)))
        (is false "Should have thrown")
        (catch Exception ex
          ;; Should throw the original exception, not wrapped
          (is (identical? e ex)))))))


(deftest race-edge-cases-test
  (testing "Race with no tasks returns nil"
    (is (nil? @(q/race))))

  (testing "Race with only non-taskables returns first"
    (is (= :first @(q/race :first :second :third))))

  (testing "Race with mix cancels tasks and returns non-taskable"
    (let [task (q/task (Thread/sleep 1000) :slow)]
      ;; Non-taskable wins immediately, task should be cancelled
      (is (= :immediate @(q/race :immediate task)))
      ;; Give time for cancellation to propagate
      (Thread/sleep 20)
      (is (thrown? CancellationException @task)))))


(deftest race-stateful-test
  (testing "race-stateful works like race for basic case"
    (let [released (atom [])]
      (is (= :fast
            @(q/race-stateful
               #(swap! released conj %)
               (q/task (Thread/sleep 100) :slow)
               (q/task (Thread/sleep 10) :fast))))
      ;; Slow task was cancelled, not completed - no release
      (Thread/sleep 50)
      (is (= [] @released))))

  (testing "release called for simultaneous completions"
    (let [released (atom [])]
      ;; q/q runs synchronously, so all complete "simultaneously"
      (is (contains? #{:a :b :c}
            @(q/race-stateful
               #(swap! released conj %)
               (q/q :a)
               (q/q :b)
               (q/q :c))))
      ;; Give time for release to run (it's on a virtual thread)
      (Thread/sleep 50)
      ;; Two of the three should be released (the losers)
      (is (= 2 (count @released)))
      ;; The winner should not be in released
      (is (not-any? #{@(q/race-stateful identity (q/q :a) (q/q :b) (q/q :c))} @released))))

  (testing "release not called for exceptional tasks"
    (let [released (atom [])]
      (is (= :success
            @(q/race-stateful
               #(swap! released conj %)
               (q/q :success)
               (q/failed-task (Exception. "fail")))))
      (Thread/sleep 50)
      ;; Only non-exceptional losers get released
      (is (= [] @released))))

  (testing "release not called for cancelled tasks"
    (let [released (atom [])]
      (is (= :immediate
            @(q/race-stateful
               #(swap! released conj %)
               (q/q :immediate)
               (q/task (Thread/sleep 1000) :slow))))
      (Thread/sleep 50)
      ;; Cancelled task didn't complete, so no release
      (is (= [] @released))))

  (testing "release receives the orphaned value"
    (let [released (atom nil)]
      @(q/race-stateful
         #(reset! released %)
         (q/q {:id 1})
         (q/q {:id 2}))
      (Thread/sleep 50)
      ;; Released value should be a map with :id
      (is (map? @released))
      (is (contains? @released :id)))))


(deftest advanced-features-test
  (testing "qdo returns only last value"
    (is (= :last
          @(q/qdo
             (q/task :first)
             (q/task :second)
             (q/task :last)))))

  (testing "qdo cancels all if one throws"
    (let [task-ran (atom false)]
      (is (thrown? Exception
            @(q/qdo
               (q/task (Thread/sleep 100) (reset! task-ran true))
               (q/task (throw (Exception. "Boom!")))))))))
