(ns bench
  (:require
    [clj-async-profiler.core :as prof]
    [co.multiply.quiescent :as q :refer [q qfor]]
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
  #__)
