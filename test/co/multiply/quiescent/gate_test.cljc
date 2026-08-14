(ns co.multiply.quiescent.gate-test
  (:require
    [clojure.test :refer [is]]
    [co.multiply.quiescent :as q :refer [q]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park clause def-results expect make-exception result with-task]]))


(allow-platform-park)


;; # Basic Gate Behavior
;; ################################################################################

(def-results gate-basic
  (clause :single-task "Single task through gate"
    (let [g (q/gate 2)]
      (with-task (q/gate-task g 42)
        (result [v]
          (is (= 42 v))))))

  (clause :multiple-tasks "Multiple tasks complete through gate"
    (let [g (q/gate 2)]
      (with-task (q [(q/gate-task g 1)
                     (q/gate-task g 2)
                     (q/gate-task g 3)])
        (result [v]
          (is (= [1 2 3] v))))))

  (clause :more-permits-than-tasks "More permits than tasks (no waiting)"
    (let [g (q/gate 10)]
      (with-task (q [(q/gate-task g :a)
                     (q/gate-task g :b)
                     (q/gate-task g :c)])
        (result [v]
          (is (= [:a :b :c] v)))))))


;; # Concurrency Limiting
;; ################################################################################

(def-results gate-concurrency
  (clause :limits-concurrent "Gate limits concurrent execution"
    (let [g          (q/gate 2)
          concurrent (atom 0)
          max-seen   (atom 0)
          make-task  (fn [_]
                       (q/gate-task g
                         (let [n (swap! concurrent inc)]
                           (swap! max-seen max n)
                           (q/sleep 1
                             (fn []
                               (swap! concurrent dec)
                               :done)))))]
      (with-task (q (mapv make-task (range 6)))
        (result [v]
          (is (= (repeat 6 :done) v))
          (is (<= @max-seen 2) "Should never exceed 2 concurrent")))))

  (clause :serial-with-one-permit "Gate with 1 permit runs tasks serially"
    (let [g          (q/gate 1)
          concurrent (atom 0)
          max-seen   (atom 0)
          make-task  (fn [i]
                       (q/gate-task g
                         (let [n (swap! concurrent inc)]
                           (swap! max-seen max n)
                           (q/sleep 1
                             (fn []
                               (swap! concurrent dec)
                               i)))))]
      (with-task (q (mapv make-task (range 4)))
        (result [v]
          (is (= [0 1 2 3] v))
          (is (= 1 @max-seen) "Should never exceed 1 concurrent"))))))


;; # Error Handling
;; ################################################################################

(def-results gate-errors
  (clause :error-releases-permit "Task error releases permit for next task"
    (let [g         (q/gate 1)
          completed (atom [])
          _task1    (q/gate-task g (throw (make-exception "fail")))
          task2     (q/gate-task g
                      (swap! completed conj :task2)
                      :success)]
      (with-task task2
        (result [v]
          (is (= :success v))
          (is (= [:task2] @completed) "Task2 should run after task1 fails")))))

  (clause :errors-propagate "Errors propagate from gated tasks"
    (let [g (q/gate 2)]
      (with-task (q/gate-task g (throw (make-exception "test error")))
        (result [_v e]
          (is (some? e))))))

  (clause :error-churn "Permits survive a churn of failing tasks"
    (let [g         (q/gate 1)
          completed (atom [])
          _tasks    (mapv (fn [i]
                            (q/gate-task g
                              (if (even? i)
                                (throw (make-exception "churn"))
                                (do (swap! completed conj i) i))))
                      (range 20))]
      (with-task (q/gate-task g :done)
        (result [v]
          (is (= :done v))
          (is (= [1 3 5 7 9 11 13 15 17 19] @completed)
            "Every non-failing task should run, in order"))))))


;; # Cancellation
;; ################################################################################

(def-results gate-cancellation
  (clause :cancel-gate-throws "Cancelling a gate is a loud type error, not a no-op"
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo)
          (q/cancel (q/gate 1)))
      "A gate is a coordinator; it holds no claim on gated work"))

  (clause :gate-survives-creator "Gate created inside a task survives that task's settle"
    (let [holder (q/promise)
          _outer (q/task (holder (q/gate 2)) :done)]
      (with-task (q/then holder
                   (fn [g]
                     (q/then (q/sleep 20)
                       (fn [_] (q/gate-task g :alive)))))
        (result [v]
          (is (= :alive v)
            "A gate has no lifecycle of its own; it must keep admitting work after its creator settles")))))

  (clause :cohort-isolation "Cancelling one cohort does not cancel an adjacent cohort sharing the gate"
    (let [g        (q/gate 1)
          a-ran    (atom 0)
          cohort-a (q/qfor [n (range 4)]
                     (q/gate-task g
                       (q/sleep 10000 (fn [] (swap! a-ran inc) n))))
          cohort-b (q/qfor [n (range 4)]
                     (q/gate-task g (q/sleep 5 n)))]
      (with-task (q/then (q/sleep 20)
                   (fn [_]
                     (q/then (q/cancel cohort-a)
                       (fn [_] cohort-b))))
        (result [v]
          (is (= [0 1 2 3] v)
            "Adjacent cohort runs to completion through the shared gate")
          (is (zero? @a-ran)
            "Cancelled cohort's queued and running bodies never produce their effects")))))

  (clause :task-cancel-releases-permit "Cancelled task releases permit"
    (let [g         (q/gate 1)
          task1     (q/gate-task g
                      (q/sleep 10000 :should-not-complete))
          completed (atom false)
          task2     (q/gate-task g
                      (reset! completed true)
                      :task2-done)]
      (with-task (q/then (q/sleep 20)
                   (fn [_]
                     (q/then (q/cancel task1)
                       (fn [_] task2))))
        (result [v]
          (is (= :task2-done v))
          (is (true? @completed) "Task2 should run after task1 cancelled")))))

  (clause :many-cancelled-while-queued "Long run of tasks cancelled while queued doesn't wedge the gate"
    (let [g      (q/gate 1)
          _head  (q/gate-task g (q/sleep 100 :head))
          queued (mapv (fn [i] (q/gate-task g i)) (range 10000))
          tail   (q/gate-task g :tail)]
      (run! q/cancel queued)
      (with-task tail
        (result [v]
          (is (= :tail v) "Tail task should run after cancelled entries are skipped")))))

  (clause :creator-cascade "Creator settle cascades to fire-and-forget gated tasks"
    (let [g      (q/gate 2)
          ran    (atom false)
          holder (q/promise)
          ;; Deliver the task wrapped in a fn so the promise doesn't ground it.
          _outer (q/task
                   (holder (constantly (q/gate-task g (q/sleep 100 #(reset! ran true)))))
                   :done)]
      (with-task (q/then holder (fn [get-t] (expect (get-t) (fn [_v _e c] [c @ran]))))
        (result [[c ran?]]
          (is (true? c) "Gated task should be cascade-cancelled when its creator settles")
          (is (false? ran?) "Cancelled body should never produce its side effect")))))

  (clause :compel-detaches "compel detaches a gated task from its creator"
    (let [g      (q/gate 2)
          holder (q/promise)
          _outer (q/task
                   (holder (constantly (q/compel (q/gate-task g (q/sleep 30 :survived)))))
                   :done)]
      (with-task (q/then holder (fn [get-t] (expect (get-t) (fn [v _e c] [v c]))))
        (result [[v c]]
          (is (= :survived v) "Compelled gated task should survive its creator's settle")
          (is (false? c)))))))


;; # Stress
;; ################################################################################

(def-results gate-stress
  (clause :cancel-storm "Cancellation storm neither leaks permits nor wedges the gate"
    (let [g     (q/gate 2)
          tasks (mapv (fn [i] (q/gate-task g (q/sleep 2 i))) (range 100))]
      (doseq [i (range 0 100 3)]
        (q/cancel (nth tasks i)))
      (with-task (q/then (q (mapv (fn [t] (expect t (fn [_v _e _c] true))) tasks))
                   (fn [statuses] (q/gate-task g [(count statuses) :after])))
        (result [v]
          (is (= [100 :after] v)
            "Every task settles and the gate still admits work afterwards")))))

  #?(:clj
     (clause :concurrent-enqueue "Permit ceiling holds under multi-threaded enqueue with failures"
       (let [g          (q/gate 4)
             total      200
             concurrent (atom 0)
             max-seen   (atom 0)
             tasks      (object-array total)
             threads    (mapv (fn [t]
                                (Thread.
                                  (fn []
                                    (doseq [i (range t total 8)]
                                      (aset tasks i
                                        (q/gate-task g
                                          (let [c (swap! concurrent inc)]
                                            (swap! max-seen max c)
                                            (q/sleep 1
                                              (fn []
                                                (swap! concurrent dec)
                                                (when (zero? (mod i 7))
                                                  (throw (make-exception "stress")))
                                                i)))))))))
                          (range 8))]
         (run! #(Thread/.start ^Thread %) threads)
         (run! #(Thread/.join ^Thread %) threads)
         (with-task (q (mapv (fn [t] (expect t (fn [_v _e _c] true))) tasks))
           (result [v]
             (is (= total (count v)) "Every task settles")
             (is (<= @max-seen 4) "Should never exceed 4 concurrent")))))))


;; # Chaining
;; ################################################################################

(def-results gate-chaining
  (clause :then-chain "Can chain on gated tasks"
    (let [g (q/gate 2)]
      (with-task (-> (q/gate-task g 10)
                   (q/then (fn [v] (* v 2))))
        (result [v]
          (is (= 20 v))))))

  (clause :nested-gated-tasks "Nested gated tasks work"
    (let [g (q/gate 1)]
      (with-task (q/gate-task g (q/gate-task g 42))
        (result [v]
          (is (= 42 v))))))

  (clause :fast-path-expires "Reentrant fast path expires when the permit holder settles"
    (let [g          (q/gate 1)
          concurrent (atom 0)
          max-seen   (atom 0)
          p          (q/promise)
          track      (fn [ms tag]
                       (swap! concurrent inc)
                       (swap! max-seen max @concurrent)
                       (q/sleep ms (fn [] (swap! concurrent dec) tag)))
          ;; Spawner settles immediately; its compelled descendant enqueues
          ;; only after the permit was released, while `hog` holds it.
          _spawner   (q/gate-task g
                       (q/compel
                         (q/then (q/sleep 50)
                           (fn [_]
                             (q/then (q/gate-task g (track 100 :child))
                               (fn [v] (p v))))))
                       :spawned)
          _hog       (q/gate-task g (track 300 :hog))]
      (with-task p
        (result [v]
          (is (= :child v))
          (is (= 1 @max-seen)
            "Descendant enqueued after the holder settled must wait for a permit"))))))
