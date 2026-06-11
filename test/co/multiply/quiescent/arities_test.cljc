(ns co.multiply.quiescent.arities-test
  (:require
    [clojure.test :refer [is]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.test-support :refer [allow-platform-park clause def-results result with-task]]))


(allow-platform-park)


(defn- log-run
  "Helper that returns a task which adds n to the ran atom."
  [ran n]
  (q/task (swap! ran conj n)))


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


(def-results virtual-then-arities-test
  (clause :arity-1
    (with-task (q/then 1 identity)
      (result [v] (is (= 1 v)))))

  (clause :arity-2
    (with-task (q/then 1 2 +)
      (result [v] (is (= 3 v)))))

  (clause :arity-3
    (with-task (q/then 1 2 3 +)
      (result [v] (is (= 6 v)))))

  (clause :arity-4
    (with-task (q/then 1 2 3 4 +)
      (result [v] (is (= 10 v)))))

  (clause :arity-5
    (with-task (q/then 1 2 3 4 5 +)
      (result [v] (is (= 15 v)))))

  (clause :arity-6
    (with-task (q/then 1 2 3 4 5 6 +)
      (result [v] (is (= 21 v)))))

  (clause :arity-7
    (with-task (q/then 1 2 3 4 5 6 7 +)
      (result [v] (is (= 28 v)))))

  (clause :arity-8
    (with-task (q/then 1 2 3 4 5 6 7 8 +)
      (result [v] (is (= 36 v)))))

  (clause :arity-9
    (with-task (q/then 1 2 3 4 5 6 7 8 9 +)
      (result [v] (is (= 45 v)))))

  (clause :arity-10
    (with-task (q/then 1 2 3 4 5 6 7 8 9 10 +)
      (result [v] (is (= 55 v)))))

  (clause :arity-11 "Variadic"
    (with-task (q/then 1 2 3 4 5 6 7 8 9 10 11 +)
      (result [v] (is (= 66 v)))))

  (clause :arity-12 "Variadic"
    (with-task (q/then 1 2 3 4 5 6 7 8 9 10 11 12 +)
      (result [v] (is (= 78 v))))))


#?(:clj (def-results cpu-then-arities-test
          (clause :arity-1
            (with-task (q/then-cpu 1 identity)
              (result [v] (is (= 1 v)))))

          (clause :arity-2
            (with-task (q/then-cpu 1 2 +)
              (result [v] (is (= 3 v)))))

          (clause :arity-3
            (with-task (q/then-cpu 1 2 3 +)
              (result [v] (is (= 6 v)))))

          (clause :arity-4
            (with-task (q/then-cpu 1 2 3 4 +)
              (result [v] (is (= 10 v)))))

          (clause :arity-5
            (with-task (q/then-cpu 1 2 3 4 5 +)
              (result [v] (is (= 15 v)))))

          (clause :arity-6
            (with-task (q/then-cpu 1 2 3 4 5 6 +)
              (result [v] (is (= 21 v)))))

          (clause :arity-7
            (with-task (q/then-cpu 1 2 3 4 5 6 7 +)
              (result [v] (is (= 28 v)))))

          (clause :arity-8
            (with-task (q/then-cpu 1 2 3 4 5 6 7 8 +)
              (result [v] (is (= 36 v)))))

          (clause :arity-9
            (with-task (q/then-cpu 1 2 3 4 5 6 7 8 9 +)
              (result [v] (is (= 45 v)))))

          (clause :arity-10
            (with-task (q/then-cpu 1 2 3 4 5 6 7 8 9 10 +)
              (result [v] (is (= 55 v)))))

          (clause :arity-11 "Variadic"
            (with-task (q/then-cpu 1 2 3 4 5 6 7 8 9 10 11 +)
              (result [v] (is (= 66 v)))))

          (clause :arity-12 "Variadic"
            (with-task (q/then-cpu 1 2 3 4 5 6 7 8 9 10 11 12 +)
              (result [v] (is (= 78 v)))))))


(def-results qmerge-arities-test
  (clause :arity-0
    (with-task (q/qmerge)
      (result [v] (is (= {} v)))))

  (clause :arity-1
    (with-task (q/qmerge {:a 1})
      (result [v] (is (= {:a 1} v)))))

  (clause :arity-2
    (with-task (q/qmerge {:a 1} {:b 2})
      (result [v] (is (= {:a 1 :b 2} v)))))

  (clause :arity-3
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3})
      (result [v] (is (= {:a 1 :b 2 :c 3} v)))))

  (clause :arity-4
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4} v)))))

  (clause :arity-5
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5} v)))))

  (clause :arity-6
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6} v)))))

  (clause :arity-7
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7} v)))))

  (clause :arity-8
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8} v)))))

  (clause :arity-9
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9} v)))))

  (clause :arity-10
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9} {:j 10})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9 :j 10} v)))))

  (clause :arity-11 "Variadic"
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9} {:j 10} {:k 11})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9 :j 10 :k 11} v)))))

  (clause :arity-12 "Variadic"
    (with-task (q/qmerge {:a 1} {:b 2} {:c 3} {:d 4} {:e 5} {:f 6} {:g 7} {:h 8} {:i 9} {:j 10} {:k 11} {:l 12})
      (result [v] (is (= {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9 :j 10 :k 11 :l 12} v))))))


(def-results qjoin-arities-test
  (clause :arity-0
    (with-task (q/qjoin)
      (result [v] (is (nil? v)))))

  (clause :arity-1
    (with-task (q/qjoin :a)
      (result [v] (is (= :a v)))))

  (clause :arity-2
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1} @ran))))))

  (clause :arity-3
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2} @ran))))))

  (clause :arity-4
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3} @ran))))))

  (clause :arity-5
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4} @ran))))))

  (clause :arity-6
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4 5} @ran))))))

  (clause :arity-7
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4 5 6} @ran))))))

  (clause :arity-8
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4 5 6 7} @ran))))))

  (clause :arity-9
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4 5 6 7 8} @ran))))))

  (clause :arity-10
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) (log-run ran 9) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4 5 6 7 8 9} @ran))))))

  (clause :arity-11 "Variadic"
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) (log-run ran 9) (log-run ran 10) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4 5 6 7 8 9 10} @ran))))))

  (clause :arity-12 "Variadic"
    (let [ran (atom #{})]
      (with-task (q/qjoin (log-run ran 1) (log-run ran 2) (log-run ran 3) (log-run ran 4) (log-run ran 5) (log-run ran 6) (log-run ran 7) (log-run ran 8) (log-run ran 9) (log-run ran 10) (log-run ran 11) :done)
        (result [v]
          (is (= :done v))
          (is (= #{1 2 3 4 5 6 7 8 9 10 11} @ran)))))))
