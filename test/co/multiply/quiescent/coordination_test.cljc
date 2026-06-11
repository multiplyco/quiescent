(ns co.multiply.quiescent.coordination-test
  #?(:cljs (:require-macros [co.multiply.quiescent.coordination-test :refer [deep-qdo]]))
  (:require
    [clojure.test :refer [is]]
    [co.multiply.quiescent :as q :refer [q qjoin]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park clause def-results make-exception result with-task]]))


(allow-platform-park)


(def-results then-test
  (clause :single-task "Single task then"
    (with-task (q/then (q/task 10) inc)
      (result [v]
        (is (= 11 v)))))

  (clause :multiple-tasks "Multiple tasks then"
    (with-task (q/then (q/task 10) (q/task 20) (q/task 30) +)
      (result [v]
        (is (= 60 v)))))

  (clause :multiple-scalars "Multiple scalars then"
    (with-task (q/then 10 20 30 +)
      (result [v]
        (is (= 60 v))))))


(def-results qfor-test
  (clause :parallel-map "qfor parallel map"
    (with-task (q/qfor [x [1 2 3]]
                 (q/task (* x 2)))
      (result [v]
        (is (= [2 4 6] v))))))


(def-results qmerge-test
  (clause :merges-maps "qmerge merges maps with tasks"
    (with-task (q/qmerge {:a (q/task 1)}
                 {:b (q/task 2)}
                 {:c (q/task 3)})
      (result [v]
        (is (= {:a 1 :b 2 :c 3} v)))))

  (clause :single-map "qmerge with single map"
    (with-task (q/qmerge {:a (q/task 1)})
      (result [v]
        (is (= {:a 1} v))))))


(def-results race-test
  (clause :nils "racing nils results in nil winning"
    (with-task (q/race (q nil) (q nil))
      (result [v]
        (is (nil? v)))))

  (clause :first-completed "race returns first completed"
    (with-task (q/race
                 (q/sleep 100 :slow)
                 (q/sleep 10 :fast))
      (result [v]
        (is (= :fast v)))))

  (clause :immediate-value "race with immediate value"
    (with-task (q/race :immediate (q/sleep 100 :slow))
      (result [v]
        (is (= :immediate v)))))


  (clause :single-error "Race with single failure throws that error directly"
    (let [e1 (make-exception "Single error")]
      (with-task (q/race (q/task (throw e1)))
        (result [_v e]
          (is (identical? e1 e))))))

  (clause :collect-errors "Race collects all errors when all tasks fail"
    (let [e1 (make-exception "Error 1")
          e2 (make-exception "Error 2")
          e3 (make-exception "Error 3")]
      (with-task (q/race (q/task (throw e1)) (q/task (throw e2)) (q/task (throw e3)))
        (result [_v e]
          (is (= "All tasks failed." (ex-message e)))
          (let [errors (-> e ex-data :errors)]
            (is (= 3 (count errors)))
            (is (some #{e1} errors))
            (is (some #{e2} errors))
            (is (some #{e3} errors)))))))

  (clause :all-cancelled "Race with all tasks cancelled is cancelled"
    (let [t1 (doto (q/sleep 5000 :a) (q/cancel))
          t2 (doto (q/sleep 5000 :b) (q/cancel))
          t3 (doto (q/sleep 5000 :c) (q/cancel))]
      (with-task (q/race t1 t2 t3)
        (result [_v _e c]
          (is (true? c) "Race should be cancelled")))))

  (clause :mixed-cancelled-failing "Race with mix of cancelled and failed throws error, not cancellation"
    (let [t1 (doto (q/sleep 5000 :a) (q/cancel))
          t2 (q/task (throw (make-exception "Real error")))]
      (with-task (q/race t1 t2)
        (result [_v e c]
          (is (false? c) "Race is not cancelled.")
          (is (some? e) "The race is exceptional.")))))

  (clause :empty "Race with no tasks returns nil"
    (with-task (q/race)
      (result [v]
        (is (nil? v)))))

  (clause :scalars "Race with only non-taskables returns first"
    (with-task (q/race :first :second :third)
      (result [v]
        (is (= :first v)))))

  (clause :scalar-task "Race with mix cancels tasks and returns non-taskable"
    (let [task (q/sleep 1000 :slow)]
      (with-task (q/race task :immediate)
        (result [v]
          (is (= :immediate v))
          (is (q/cancelled? task))))))

  (clause :stateful-nils "racing nils results in nil winning"
    (let [released (atom [])]
      (with-task (q/race-stateful #(swap! released conj %) (q nil) (q nil))
        (result [v]
          (is (nil? v))))))

  (clause :stateful-basic "race-stateful works like race for basic case"
    (with-task (q/race-stateful (constantly nil)
                 (q/sleep 100 :slow) (q/sleep 10 :fast))
      (result [v]
        (is (= :fast v)))))

  (clause :stateful-basic-release "release called for simultaneous completions"
    (let [released (atom [])
          task     (q/race-stateful #(swap! released conj %)
                     (q :a) (q :b) (q :c))]
      (with-task (qjoin (q/await task) task)
        (result [v]
          (is (contains? #{:a :b :c} v) "There should be a winner.")
          (is (= 2 (count @released)) "Two releases should be recorded.")
          (is (nil? (some #{v} @released)) "Winner should not be among released.")))))

  (clause :stateful-exception-no-release "release not called for exceptional tasks"
    (let [released (atom [])
          release  (fn [v] (swap! released conj v))]
      (with-task (q/race-stateful release
                   (q :success) (q/failed-task (make-exception "fail")))
        (result [v]
          (is (= :success v) "There is a winner.")
          (is (empty? @released) "There are no releases.")))))

  (clause :stateful-cancelled-no-release "release not called for cancelled tasks"
    (let [released (atom [])]
      (with-task (q/race-stateful #(swap! released conj %)
                   (q :immediate) (q/sleep 1000 :slow))
        (result [v]
          (is (= :immediate v) "There is a winner.")
          (is (empty? @released) "No release for cancelled tasks.")))))

  (clause :stateful-no-release-winner "same task raced multiple times does not release winner"
    (let [released (atom [])
          t1       (q :r1)
          t2       (q :r2)
          task     (q/race-stateful #(swap! released conj %)
                     t1 t1 t2 t2)]
      (with-task (qjoin (q/sleep 5 :done) task)
        (result [v]
          (let [loser (if (= v :r1) :r2 :r1)]
            (is (contains? #{:r1 :r2} v) "There is a winner.")
            (is (= [loser] @released) "Only loser released, exactly once")))))))


#?(:clj (defmacro deep-qdo
          "Expand to a `qdo` of `n` clauses, each a task conj'ing its index onto the
           atom bound to `ran`. Exercises deeply nested `then` chains in the expansion."
          [ran n]
          `(q/qdo ~@(map (fn [i] `(q/task (swap! ~ran conj ~i))) (range n)))))


(def-results qdo-test
  (clause :empty "qdo with no clauses returns nil"
    (with-task (q/qdo)
      (result [v]
        (is (nil? v)))))

  (clause :single-clause "qdo with one clause returns its value"
    (with-task (q/qdo :a)
      (result [v]
        (is (= :a v)))))

  (clause :returns-last-value "qdo returns only last value"
    (with-task (q/qdo (q/task :first) (q/task :second) (q/task :last))
      (result [v]
        (is (= :last v) "Returns last value."))))

  (clause :sequential-order "qdo awaits each clause before evaluating the next"
    (let [order (atom [])]
      (with-task (q/qdo (q/sleep 20 #(swap! order conj :first))
                   (swap! order conj :second)
                   :done)
        (result [v]
          (is (= :done v) "Returns last value.")
          (is (= [:first :second] @order) "Later clauses wait for earlier tasks.")))))

  (clause :deep-chain "qdo handles 50 sequential clauses"
    (let [ran (atom [])]
      (with-task (deep-qdo ran 50)
        (result [v]
          (is (= (vec (range 50)) @ran) "All clauses ran, strictly in order.")
          (is (= (vec (range 50)) v) "Returns the last clause's value.")))))

  (clause :failure-short-circuits "qdo skips remaining clauses after a failure"
    (let [ran (atom false)]
      (with-task (q/qdo (q/task (throw (make-exception "Boom!")))
                   (reset! ran true))
        (result [_v e c]
          (is (false? c) "`qdo` is not cancelled.")
          (is (some? e) "`qdo` is exceptional.")
          (is (false? @ran) "Remaining clauses never run."))))))


(def-results qjoin-test
  (clause :returns-last-value "qjoin returns only last value"
    (with-task (q/qjoin (q/task :first) (q/task :second) (q/task :last))
      (result [v]
        (is (= :last v) "Returns last value."))))

  (clause :exception-propagates "qjoin cancels all if one throws"
    (let [task-ran  (atom false)
          slow-task (q/sleep 100 #(reset! task-ran true))]
      (with-task (q/qjoin slow-task (q/task (throw (make-exception "Boom!"))))
        (result [_v e c]
          (is (false? c) "`qjoin` is not cancelled.")
          (is (some? e) "`qjoin` is exceptional.")
          (is (q/cancelled? slow-task) "Slow task is cancelled."))))))
