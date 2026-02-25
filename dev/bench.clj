(ns bench
  (:require
    [clj-async-profiler.core :as prof]
    [co.multiply.quiescent :as q :refer [q qfor qlet qdo]]
    [co.multiply.quiescent.channel :as qc :refer [chan put! take!]]
    [criterium.core :as c]))


(comment
  ;; Benchmark something

  (q/throw-on-platform-park! false)

  (let [gate (q/gate 4)]
    @(qfor [n (range 100)]
       (q/gate-task gate
         (q/sleep (+ 5 (rand-int 95))
           (fn []
             (println "Done:" n)
             n)))))

  (prof/profile
    (dotimes [i 10000]
      (q nil)))

  (let [numbers (vec (range 1000))]
    (prof/profile
      (dotimes [i 10000]
        (qfor [n numbers]
          (q/task n)))))

  (prof/serve-ui 8080)

  (c/bench @(q nil))

  (let [numbers (vec (range 1000))]
    (c/bench
      @(qfor [n numbers]
         (q n))))

  (let [numbers (vec (range 10000))]
    (c/bench
      @(qfor [n numbers]
         (q/task n))))

  (qfor [groups (range 100)]
    (q/task
      (qfor [n (range 1000)]
        (q/task n))))


  (let [numbers (vec (range 1000000))]
    (c/bench
      @(qfor [n numbers]
         (q/sleep (mod n 1000) n))))

  (let [numbers (vec (range 1000))
        groups  (vec (range 100))]
    (c/bench
      @(qfor [m groups]
         (q/task
           (qfor [n numbers]
             (q/task n))))))

  (c/bench
    @(q {:hello         (q :world)
         :animals       #{(q :dog) (q :cat) (q :capybara)}
         :frozen-places [(q "North pole") (q "My freezer")]}))

  (let [g (q/gate 1)]
    @(qfor [n (range 1000)]
       (-> (q/task (* n 2))
         (q/then
           (fn stringify
             [n]
             (q/gate-task g
               (q/sleep (+ 10 (rand-int 100))
                 #(str "n-" n)))))
         (q/then
           (fn keywordify
             [s]
             (println "Done:" s)
             (keyword s))))))


  (def test-ch (chan 5))

  (qc/capacity test-ch) ;; => 8

  (put! test-ch true)   ;; => true
  (put! test-ch :hello) ;; => true
  (put! test-ch nil)    ;; => true
  (put! test-ch 0)      ;; => true

  (take! test-ch)       ;; => true
  (take! test-ch)       ;; => :hello
  (take! test-ch)       ;; => nil
  (take! test-ch)       ;; => 0

  (let [ch (chan 5)]
    (q/deref-cpu
      (q/task
        (q/task
          (dotimes [n 10]
            (put! ch (str "first-" n))))
        (q/task
          (dotimes [n 10]
            (put! ch (str "second-" n))))
        (repeatedly 10 #(take! ch)))))

  ;; => ("first-0" "first-1" "first-2" "first-3" "first-4" "first-5" "second-0" "first-6" "first-7" "first-8")

  #__)
