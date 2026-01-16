(ns co.multiply.quiescent.qlet-test
  (:require
    [clojure.test :refer [is testing]]
    [co.multiply.quiescent :as q :refer [q qlet]]
    [co.multiply.quiescent.test-support :refer [clause def-results make-exception result with-task]]))


(def-results basic-test
  (clause :no-deps "Body with no dependencies"
    (with-task (q/qlet [] 42)
      (result [v]
        (is (= 42 v)))))

  (clause :no-refs "Body references no bindings"
    (with-task (q/qlet [a 10 b 20]
                 (+ 50 50))
      (result [v]
        (is (= 100 v)))))


  (clause :simple-deps "Simple sequential bindings"
    (with-task (qlet [a 10
                      b 20
                      c (+ a b)]
                 (+ c 30))
      (result [v]
        (is (= 60 v)))))

  (clause :simple-addition "Plain values stay synchronous"
    (with-task (qlet [x 1 y 2 z 3]
                 (+ x y z))
      (result [v]
        (is (= 6 v)))))

  (clause :inline-tasks "Tasks are allowed"
    (with-task (qlet [a (q/task 10)
                      b (q/task (+ a 20))                   ; Depends on a
                      c (q/task (+ b 30))]                  ; Depends on b
                 c)
      (result [v]
        (is (= 60 v)))))

  (clause :body-awaits "Body waits for all referenced bindings"
    (let [results (atom [])]
      (with-task (q/qlet [a (q/task (swap! results conj :a) 10)
                          b (q/task (swap! results conj :b) 20)
                          c (q/task (swap! results conj :c) 30)]
                   (swap! results conj :body)
                   (+ a b c))
        (result [v]
          (is (= 60 v))
          (is (= #{:a :b :c} (set (take 3 @results))))
          (is (= :body (last @results)))))))

  (clause :mix-task-plain "Mix of tasks and plain values"
    (with-task (q/qlet [a 10
                        b (q/task 20)
                        c [1 2 3]
                        d (q (+ a b (count c)))]
                 (* d 2))
      (result [v]
        (is (= 66 v)))))

  (clause :dep-ordering "Dependencies execute in correct order"
    (let [results (atom [])]
      (with-task (qlet [a (q/task (swap! results conj :a) 10)
                        b (do (swap! results conj :b) (+ a 5))]
                   b)
        (result [v]
          (is (= 15 v))
          (is (= [:a :b] @results))))))

  (clause :parallel "Independent tasks run in parallel"
    (let [timing (atom 1000)]
      (with-task (-> (qlet [a (q/sleep 10 10)
                            b (q/sleep 10 20)               ; Independent, runs parallel
                            c (+ a b)]                      ; Waits for both
                       c)
                   (q/time
                     (fn [_v _e _c t]
                       (reset! timing t))))
        (result []
          (is (< @timing 20) "Independent tasks should run in parallel")))))

  (testing :complex-dag "Complex dependency graph with multiple levels"
           (let [timing (atom 1000)]
             (with-task (-> (q/qlet [;; Level 0: All independent
                                     a (q/sleep 5 1)
                                     b (q/sleep 5 2)
                                     c (q/sleep 5 3)

                                     ;; Level 1: Depends on level 0
                                     d (q/task (+ a b))            ; Depends on a, b
                                     e (q/task (+ b c))            ; Depends on b, c

                                     ;; Level 2: Depends on level 1
                                     f (q/task (+ d e))]           ; Depends on d, e
                              f)
                          (q/time
                            (fn [_v _e _c t]
                              (reset! timing t))))
               (result [v]
                 (is (= 8 v))
                 (is (< @timing 15) "Should maximize parallelism"))))))


(def-results destructuring-test
  (clause :map "Map destructuring"
    (with-task (qlet [{:keys [x y]} (q/task {:x 10 :y 20})
                      sum (+ x y)]
                 sum)
      (result [v]
        (is (= 30 v)))))

  (clause :vec "Vector destructuring"
    (with-task (qlet [[a b c] (q/task [10 20 30])
                      sum (+ a b c)]
                 sum)
      (result [v]
        (is (= 60 v)))))

  (clause :rest "Rest destructuring"
    (with-task (qlet [[a b & rest] (q/task [1 2 3 4 5])]
                 [a b (vec rest)])
      (result [v]
        (is (= [1 2 [3 4 5]] v)))))

  (clause :nested "Nested destructuring"
    (with-task (qlet [{:keys [data]} (q/task {:data [10 20]})
                      [x y] data
                      sum (+ x y)]
                 sum)
      (result [v]
        (is (= 30 v))))))


(def-results shadowing-test
  (clause :basic "Later bindings shadow earlier ones"
    (with-task (qlet [x 1
                      y (+ x 10)
                      x 2
                      z (+ x 5)]
                 [x y z])
      (result [v]
        (is (= [2 11 7] v)))))

  (clause :destructure-basic-keys "Destructuring then shadowing (matches regular let semantics)"
    (with-task (qlet [{:keys [title]} {:title "Hello"}
                      title "World"]
                 title)
      (result [v]
        (is (= "World" v)))))

  (clause :destructure-complex "Destructuring with shadowing - complex example"
    (with-task (qlet [{:keys [value] :as orig} (q/task {:value 1})
                      value (q/task (+ value 10))]          ; Shadow destructured value
                 {:original orig :shadowed value})
      (result [v]
        (is (= {:original {:value 1} :shadowed 11} v)))))

  (clause :destructure-multiple "Multiple levels of shadowing with destructuring"
    (with-task (qlet [a            (q/task {:value 1})
                      {:keys [value]} a                     ; Extract value=1
                      first-value  value                    ; Capture it
                      value        (+ value 10)             ; Shadow to 11
                      second-value value                    ; Capture
                      value        (+ value 11)]            ; Shadow to 22
                 [first-value second-value value])
      (result [v]
        (is (= [1 11 22] v)))))

  (clause :destructure-keys "Destructuring binds symbol used in subsequent binding (:keys)"
    (with-task (qlet [{:keys [value]} (q/task {:value 10})
                      doubled (* value 2)]
                 {:first value :doubled doubled})
      (result [v]
        (is (= {:first 10 :doubled 20} v)))))

  (clause :destructure-keys-symbol "Destructuring binds symbol used in subsequent binding (map keys)"
    (with-task (qlet [{orig-uri :uri} (q/task {:uri "s3://example"})
                      {presigned :uri} (q/task {:uri (str "presigned-" orig-uri)})]
                 {:original orig-uri :uri presigned})
      (result [v]
        (is (= {:original "s3://example" :uri "presigned-s3://example"} v)))))

  (clause :destructure-as "Destructuring binds symbol used in subsequent binding (:as)"
    (with-task (qlet [{:keys [x y] :as whole} (q/task {:x 1 :y 2})
                      sum (+ x y)]
                 {:whole whole :sum sum})
      (result [v]
        (is (= {:whole {:x 1 :y 2} :sum 3} v)))))

  (clause :destructure-vec "Destructuring binds symbol used in subsequent binding (vector)"
    (with-task (qlet [[a b] (q/task [1 2])
                      swapped [b a]]
                 {:first a :second b :swapped swapped})
      (result [v]
        (is (= {:first 1 :second 2 :swapped [2 1]} v)))))

  (clause :destructure-or "Destructuring binds symbol used in subsequent binding (:or defaults)"
    (with-task (qlet [{:keys [value missing] :or {missing 10}} (q/task {:value 42})
                      processed (+ value missing)]
                 {:value value :processed processed})
      (result [v]
        (is (= {:value 42 :processed 52} v))))))


(def-results error-handling-test
  (clause :propagate "Errors propagate correctly"
    (with-task (q/qlet [a 10
                        b (throw (make-exception "Test error"))
                        c (+ a b)]
                 c)
      (result [_v e c]
        (is (false? c))
        (is (some? e)))))


  (clause :propagate-dep "Error in dependent task"
    (with-task (q/qlet [a (q/task 10)
                        b (q/task (throw (make-exception "Task error")))
                        c (+ a b)]
                 c)
      (result [_v e c]
        (is (false? c))
        (is (some? e))))))


(def-results readme-example-test
  (testing :complex "README-style example"
           (let [fetch-user   (fn [id] (q/sleep 5 {:id id :name "Alice"}))
                 fetch-posts  (fn [user-id] (q/sleep 5 [{:user-id user-id :title "Post 1"}]))
                 process-user (fn [user] (q/sleep 5 (assoc user :processed true)))
                 timing       (atom 1000)]
             (with-task (-> (q/qlet [user    (fetch-user 1)        ; Starts immediately
                                     posts   (fetch-posts 1)       ; Parallel with user fetch
                                     profile (process-user user)]  ; Waits for user
                              {:profile profile :posts posts})
                          (q/time
                            (fn [_v _e _c t]
                              (reset! timing t))))
               (result [v]
                 (is (< @timing 15) "Should benefit from parallel execution")
                 (is (= {:profile {:id 1 :name "Alice" :processed true}
                         :posts   [{:user-id 1 :title "Post 1"}]}
                        v)))))))


(def-results if-qlet-test
  (clause :truthy "Truthy branch executes when test is truthy"
    (with-task (q/if-qlet [x (q/task 42)]
                 (+ x 10)
                 :not-found)
      (result [v]
        (is (= 52 v)))))

  (clause :falsy "Falsy branch executes when test is nil"
    (with-task (q/if-qlet [x (q/task nil)]
                 :found
                 :not-found)
      (result [v]
        (is (= :not-found v)))))

  (testing :false "Falsy branch executes when test is false"
           (with-task (q/if-qlet [x (q/task false)]
                        :found
                        :not-found)
             (result [v]
               (is (= :not-found v)))))

  (testing :destructure "Destructuring in binding"
           (with-task (q/if-qlet [{:keys [a b]} (q/task {:a 10 :b 20})]
                        (+ a b)
                        0)
             (result [v]
               (is (= 30 v)))))

  (testing :destructure-nil "Destructuring with nil value takes else branch"
           (with-task (q/if-qlet [{:keys [value]} (q/task nil)]
                        value
                        :missing)
             (result [v]
               (is (= :missing v)))))

  (testing :plain-values "Works with plain values (not just tasks)"
           (with-task (q/if-qlet [x {:data 42}]
                        :found
                        :not-found)
             (result [v]
               (is (= :found v)))))

  (testing :returns-task "Returns a Task"
           (is (q/task? (q/if-qlet [x (q/task 1)] :yes :no)))))


(def-results when-qlet-test
  (clause :truthy "Body executes when test is truthy"
    (with-task (q/when-qlet [x (q/task 42)]
                 (+ x 10))
      (result [v]
        (is (= 52 v)))))

  (clause :falsy "Returns nil when test is nil"
    (with-task (q/when-qlet [x (q/task nil)]
                 :should-not-run)
      (result [v]
        (is (nil? v)))))

  (clause :false "Returns nil when test is false"
    (let [side-effect (atom :untouched)]
      (with-task (q/when-qlet [x (q/task false)]
                   (reset! side-effect :touched)
                   :should-not-run)
        (result [v]
          (is (nil? v))
          (is (= :untouched @side-effect))))))

  (clause :destructure "Destructuring in binding"
    (with-task (q/when-qlet [[a b] (q/task [1 2 3])]
                 [a b])
      (result [v]
        (is (= [1 2] v)))))

  (clause :plain-values "Works with plain values (not just tasks)"
    (with-task (q/when-qlet [x 10]
                 x)
      (result [v]
        (is (= 10 v)))))

  (clause :returns-task "Returns a Task"
    (is (q/task? (q/when-qlet [x (q/task 1)] x)))))
