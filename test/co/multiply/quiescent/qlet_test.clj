(ns co.multiply.quiescent.qlet-test
  "Tests for qlet macro: sequential-looking syntax with automatic parallelization
   through dependency analysis. Includes destructuring, shadowing, and error handling."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]]))


(use-fixtures :once platform-thread-fixture)


(deftest basic-let-test
  (testing "Simple sequential bindings"
    (is (= 30 @(q/qlet [a 10
                        b 20
                        c (+ a b)]
                 c))))

  (testing "Plain values stay synchronous"
    (is (= 6 @(q/qlet [x 1
                       y 2
                       z 3]
                (+ x y z))))))


(deftest dependency-ordering-test
  (testing "Dependencies execute in correct order"
    (let [results (atom [])
          task-a  (q/task
                    (swap! results conj :a)
                    (Thread/sleep 10)
                    10)
          task-b  (q/qlet [a task-a
                           b (do (swap! results conj :b)
                               (+ a 5))]
                    b)]
      @task-b
      ;; :a must come before :b since b depends on a
      (is (= [:a :b] @results))))

  (testing "Nested dependencies"
    (is (= 60
          @(q/qlet [a (q/task 10)
                    b (q/task (+ a 20))                     ; Depends on a
                    c (q/task (+ b 30))]                    ; Depends on b
             c)))))


(deftest parallel-execution-test
  (testing "Independent tasks run in parallel"
    (let [start-time (System/currentTimeMillis)
          result     @(q/qlet [a (q/task (Thread/sleep 50) 10)
                               b (q/task (Thread/sleep 50) 20) ; Independent, runs parallel
                               c (+ a b)]                   ; Waits for both
                        c)
          duration   (- (System/currentTimeMillis) start-time)]
      (is (= 30 result))
      ;; Should take ~50ms not 100ms (with some tolerance for scheduling)
      (is (< duration 80) "Independent tasks should run in parallel")))

  (testing "Multiple independent branches"
    (let [start-time (System/currentTimeMillis)
          result     @(q/qlet [a (q/task (Thread/sleep 30) 1)
                               b (q/task (Thread/sleep 30) 2)
                               c (q/task (Thread/sleep 30) 3)
                               d (+ a b c)]
                        d)
          duration   (- (System/currentTimeMillis) start-time)]
      (is (= 6 result))
      (is (< duration 50) "All three independent tasks should run in parallel"))))


(deftest destructuring-test
  (testing "Map destructuring"
    (is (= 30 @(q/qlet [{:keys [x y]} (q/task {:x 10 :y 20})
                        sum (+ x y)]
                 sum))))

  (testing "Vector destructuring"
    (is (= 60 @(q/qlet [[a b c] (q/task [10 20 30])
                        sum (+ a b c)]
                 sum))))

  (testing "Rest destructuring"
    (is (= [1 2 [3 4 5]] @(q/qlet [[a b & rest] (q/task [1 2 3 4 5])]
                            [a b rest]))))

  (testing "Nested destructuring"
    (is (= 30 @(q/qlet [{:keys [data]} (q/task {:data [10 20]})
                        [x y] data
                        sum (+ x y)]
                 sum)))))


(deftest shadowing-test
  (testing "Later bindings shadow earlier ones"
    (is (= 2
          @(q/qlet [x 1
                    y (+ x 10)                              ; Uses x=1
                    x 2                                     ; Shadows x
                    z (+ x 5)]                              ; Uses x=2
             x))))

  (testing "Shadow with dependency"
    (is (= [11 2 7]
          @(q/qlet [x 1
                    y (+ x 10)                              ; Uses x=1, y=11
                    x 2                                     ; Shadow x
                    z (+ x 5)]                              ; Uses x=2, z=7
             [y x z]))))

  (testing "Destructuring then shadowing (matches regular let semantics)"
    ;; This is the critical test for the fix - ensures qlet behaves like let
    (is (= "World"
          @(q/qlet [{:keys [title]} {:title "Hello"}
                    title "World"]
             title)))
    ;; Compare to regular let for validation
    (is (= "World"
          (let [{:keys [title]} {:title "Hello"}
                title "World"]
            title))))

  (testing "Destructuring with shadowing - complex example"
    (is (= {:original {:value 1} :shadowed 11}
          @(q/qlet [{:keys [value] :as orig} (q/task {:value 1})
                    value (q/task (+ value 10))]            ; Shadow destructured value
             {:original orig :shadowed value}))))

  (testing "Multiple levels of shadowing with destructuring"
    (is (= [1 11 22]
          @(q/qlet [a            (q/task {:value 1})
                    {:keys [value]} a                       ; Extract value=1
                    first-value  value                      ; Capture it
                    value        (+ value 10)               ; Shadow to 11
                    second-value value                      ; Capture
                    value        (+ value 11)]              ; Shadow to 22
             [first-value second-value value]))))

  (testing "Destructuring binds symbol used in subsequent binding (map keys)"
    ;; Regression test: ensure symbols bound in destructuring patterns are recognized
    (is (= {:original "s3://example" :uri "presigned-s3://example"}
          @(q/qlet [{orig-uri :uri} (q/task {:uri "s3://example"})
                    {presigned :uri} (q/task {:uri (str "presigned-" orig-uri)})]
             {:original orig-uri :uri presigned}))))

  (testing "Destructuring binds symbol used in subsequent binding (:keys)"
    (is (= {:first 10 :doubled 20}
          @(q/qlet [{:keys [value]} (q/task {:value 10})
                    doubled (* value 2)]
             {:first value :doubled doubled}))))

  (testing "Destructuring binds symbol used in subsequent binding (:as)"
    (is (= {:whole {:x 1 :y 2} :sum 3}
          @(q/qlet [{:keys [x y] :as whole} (q/task {:x 1 :y 2})
                    sum (+ x y)]
             {:whole whole :sum sum}))))

  (testing "Destructuring binds symbol used in subsequent binding (vector)"
    (is (= {:first 1 :second 2 :swapped [2 1]}
          @(q/qlet [[a b] (q/task [1 2])
                    swapped [b a]]
             {:first a :second b :swapped swapped}))))

  (testing "Destructuring binds symbol used in subsequent binding (:or defaults)"
    (is (= {:value 42 :processed 52}
          @(q/qlet [{:keys [value missing] :or {missing 10}} (q/task {:value 42})
                    processed (+ value missing)]
             {:value value :processed processed})))))


(deftest mixed-task-nontask-test
  (testing "Mix of tasks and plain values"
    (is (= 66                                               ; (10 + 20 + 3) * 2 = 66
          @(q/qlet [a 10                                    ; Plain value
                    b (q/task 20)                           ; Task
                    c [1 2 3]                               ; Plain collection
                    d (+ a b (count c))]
             (* d 2)))))

  (testing "User controls task creation"
    (let [counter (atom 0)
          result  @(q/qlet [a (do (swap! counter inc) 10)   ; Sync, no task
                            b (q/task (do (swap! counter inc) 20))] ; Explicit task
                     (+ a b))]
      (is (= 30 result))
      (is (= 2 @counter)))))


(deftest body-dependencies-test
  (testing "Body waits for all referenced bindings"
    (let [results (atom [])
          task    @(q/qlet [a (q/task
                                (swap! results conj :a)
                                (Thread/sleep 20)
                                10)
                            b (q/task
                                (swap! results conj :b)
                                (Thread/sleep 20)
                                20)
                            c (q/task
                                (swap! results conj :c)
                                (Thread/sleep 20)
                                30)]
                     (do
                       (swap! results conj :body)
                       (+ a b c)))]
      (is (= 60 task))
      ;; Body should execute last
      (is (= :body (last @results))))))


(deftest complex-dag-test
  (testing "Complex dependency graph with multiple levels"
    (let [start-time (System/currentTimeMillis)
          result     @(q/qlet [;; Level 0: All independent
                               a (q/task (Thread/sleep 30) 1)
                               b (q/task (Thread/sleep 30) 2)
                               c (q/task (Thread/sleep 30) 3)

                               ;; Level 1: Depends on level 0
                               d (q/task (+ a b))           ; Depends on a, b
                               e (q/task (+ b c))           ; Depends on b, c

                               ;; Level 2: Depends on level 1
                               f (q/task (+ d e))]          ; Depends on d, e
                        f)
          duration   (- (System/currentTimeMillis) start-time)]
      (is (= 8 result))                                     ; d=1+2=3, e=2+3=5, f=3+5=8
      ;; Level 0: 30ms (parallel), Level 1: minimal, Level 2: minimal
      ;; Should take ~30-40ms not 90ms
      (is (< duration 60) "Complex DAG should maximize parallelism"))))


(deftest error-handling-test
  (testing "Errors propagate correctly"
    (is (thrown? Exception
          @(q/qlet [a 10
                    b (throw (Exception. "Test error"))
                    c (+ a b)]
             c))))

  (testing "Error in dependent task"
    (is (thrown? Exception
          @(q/qlet [a (q/task 10)
                    b (q/task (throw (Exception. "Task error")))
                    c (+ a b)]
             c)))))


(deftest empty-body-test
  (testing "Body with no dependencies"
    (is (= 42 @(q/qlet []
                 42))))

  (testing "Body references no bindings"
    (is (= 100 @(q/qlet [a 10
                         b 20]
                  (+ 50 50))))))


(deftest readme-example-test
  (testing "README-style example"
    (let [fetch-user   (fn [id] (q/task (Thread/sleep 20) {:id id :name "Alice"}))
          fetch-posts  (fn [user-id] (q/task (Thread/sleep 20) [{:user-id user-id :title "Post 1"}]))
          process-user (fn [user] (q/task (Thread/sleep 20) (assoc user :processed true)))
          combine      (fn [profile posts] {:profile profile :posts posts})
          start-time   (System/currentTimeMillis)
          result       @(q/qlet [user    (fetch-user 1)     ; Starts immediately
                                 posts   (fetch-posts 1)    ; Parallel with user fetch
                                 profile (process-user user) ; Waits for user
                                 result  (combine profile posts)] ; Waits for both
                          result)
          duration     (- (System/currentTimeMillis) start-time)]
      (is (= {:profile {:id 1 :name "Alice" :processed true}
              :posts   [{:user-id 1 :title "Post 1"}]}
            result))
      ;; user and posts run in parallel (20ms), then profile (20ms), then combine
      ;; Total: ~40ms not 60ms
      (is (< duration 80) "Should benefit from parallel execution"))))


(deftest immediate-return-test
  (testing "qlet returns immediately even with sync code"
    (let [task (q/qlet [x (do (Thread/sleep 100) 42)]
                 x)]
      ;; Task should be created instantly
      (is (not (realized? task)))
      ;; Eventually resolves
      (is (= 42 @task)))))


(deftest if-qlet-test
  (testing "Truthy branch executes when test is truthy"
    (is (= :found @(q/if-qlet [x (q/task 42)]
                     :found
                     :not-found))))

  (testing "Falsy branch executes when test is nil"
    (is (= :not-found @(q/if-qlet [x (q/task nil)]
                         :found
                         :not-found))))

  (testing "Falsy branch executes when test is false"
    (is (= :not-found @(q/if-qlet [x (q/task false)]
                         :found
                         :not-found))))

  (testing "Binding is available in truthy branch"
    (is (= 52 @(q/if-qlet [x (q/task 42)]
                 (+ x 10)
                 0))))

  (testing "Destructuring in binding"
    (is (= 30 @(q/if-qlet [{:keys [a b]} (q/task {:a 10 :b 20})]
                 (+ a b)
                 0))))

  (testing "Destructuring with nil value takes else branch"
    (is (= :missing @(q/if-qlet [{:keys [value]} (q/task nil)]
                       value
                       :missing))))

  (testing "Works with plain values (not just tasks)"
    (is (= :found @(q/if-qlet [x {:data 42}]
                     :found
                     :not-found))))

  (testing "Returns a Task"
    (let [result (q/if-qlet [x (q/task 1)] :yes :no)]
      (is (q/task? result))
      (is (= :yes @result)))))


(deftest when-qlet-test
  (testing "Body executes when test is truthy"
    (is (= :executed @(q/when-qlet [x (q/task 42)]
                        :executed))))

  (testing "Returns nil when test is nil"
    (is (nil? @(q/when-qlet [x (q/task nil)]
                 :should-not-run))))

  (testing "Returns nil when test is false"
    (is (nil? @(q/when-qlet [x (q/task false)]
                 :should-not-run))))

  (testing "Binding is available in body"
    (is (= 52 @(q/when-qlet [x (q/task 42)]
                 (+ x 10)))))

  (testing "Multiple body expressions"
    (let [side-effect (atom nil)]
      (is (= :final @(q/when-qlet [x (q/task 42)]
                       (reset! side-effect x)
                       :final)))
      (is (= 42 @side-effect))))

  (testing "Side effects don't run when test is falsy"
    (let [side-effect (atom :untouched)]
      @(q/when-qlet [x (q/task nil)]
         (reset! side-effect :touched))
      (is (= :untouched @side-effect))))

  (testing "Destructuring in binding"
    (is (= [1 2] @(q/when-qlet [[a b] (q/task [1 2 3])]
                    [a b]))))

  (testing "Works with plain values (not just tasks)"
    (is (= 10 @(q/when-qlet [x 10]
                 x))))

  (testing "Returns a Task"
    (let [result (q/when-qlet [x (q/task 1)] x)]
      (is (q/task? result))
      (is (= 1 @result)))))
