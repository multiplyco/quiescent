(ns co.multiply.quiescent.promise-test
  (:require
    [clojure.test :refer [is testing]]
    [co.multiply.quiescent :as q :refer [qdo qlet]]
    [co.multiply.quiescent.test-support :refer [clause def-results make-exception result with-task]]))


(def-results promise-test
  (clause :deliver "Promise can be delivered"
    (let [p (q/promise)]
      (with-task (qdo (p 42) p)
        (result [v]
          (is (= 42 v))))))

  (testing :in-qlet "Promise used in qlet"
           (let [p    (q/promise)
                 task (qlet [v      p
                             result (+ v 10)]
                        result)]
             (with-task (qdo (p 100) task)
               (result [v]
                 (is (= 110 v)))))))


(def-results promise-fail-test
  (clause :success "Promise can be completed"
    (let [e1 (make-exception "Failed!")
          p  (doto (q/promise)
               (#(% :success))
               (#(% :secondary-success))
               (#(q/fail % e1)))]
      (with-task p
        (result [v e c]
          (is (false? c))
          (is (nil? e))
          (is (= :success v))))))

  (clause :failure "Promise can be failed with exception"
    (let [e1 (make-exception "Failed!")
          e2 (make-exception "Secondary fail!")
          p  (doto (q/promise)
               (#(q/fail % e1))
               (#(% :success))
               (#(q/fail % e2)))]
      (with-task p
        (result [_v e c]
          (is (false? c))
          (is (identical? e1 e))
          (is (not (identical? e2 e)) "Failing a failed task is a no-op.")
          #?(:clj (try @p
                    (is false "Should have thrown")
                    (catch Throwable ex
                      (is (identical? e1 ex)))))))))

  (clause :cancellation "Promise can be cancelled, and cancellation propagates"
    (let [e1   (make-exception "Failed!")
          p    (q/promise)
          link (q/then p (constantly :never-runs))]
      (with-task (qdo
                   (q/then (q/cancel p)
                     (fn [& _]
                       (p :success)
                       (q/fail p e1)))
                   p link)
        (result [_v e c]
          (is (q/cancelled? p))
          (is (q/cancelled? link))
          (is (true? c))
          (is (some? e)))))))
