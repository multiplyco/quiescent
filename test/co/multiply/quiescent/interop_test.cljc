(ns co.multiply.quiescent.interop-test
  (:require
    #?(:cljs [cljs.core.async.impl.channels :refer [ManyToManyChannel]])
    [clojure.core.async :refer [chan put!]]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q :refer [q]]
    [co.multiply.quiescent.adapter.core-async]
    [co.multiply.quiescent.test-support :refer [clause def-results make-exception result with-task allow-platform-park]])
  #?(:clj (:import
            [clojure.core.async.impl.channels ManyToManyChannel]
            [java.util.concurrent CancellationException CompletableFuture])))


(allow-platform-park)


#?(:clj (def-results completable-future
          (clause :q-convert "q with CompletableFuture"
            (with-task (q (CompletableFuture/completedFuture 1))
              (result [v]
                (is (= 1 v)))))

          (clause :q-convert-nested "task/q with CompletableFuture in data structure"
            (with-task (q {:a (CompletableFuture/completedFuture 1)})
              (result [v]
                (is (= {:a 1} v)))))

          (clause :q-convert-multiple "Multiple CompletableFutures in structure"
            (with-task (q {:a (CompletableFuture/completedFuture 1)
                           :b (CompletableFuture/completedFuture 2)})
              (result [v]
                (is (= {:a 1 :b 2} v)))))

          (clause :cancellation "Cancelled CF propagates as cancellation, not exception"
            (let [cf          (doto (CompletableFuture.) (CompletableFuture/.cancel true))
                  catch-ran   (atom false)
                  finally-ran (atom false)
                  cancelled?  (atom nil)
                  task        (-> (q cf)
                                (q/catch
                                  (fn [_]
                                    (reset! catch-ran true)))
                                (q/finally
                                  (fn [_ _ c]
                                    (reset! finally-ran true)
                                    (reset! cancelled? c))))]
              (with-task (q/await task)
                (result []
                  (is (false? @catch-ran) "catch should NOT run for cancellation")
                  (is (true? @finally-ran) "finally should run for cancellation")
                  (is (true? @cancelled?) "finally should receive cancelled=true")
                  (is (q/cancelled? task) "task should be marked as cancelled")))))

          (clause :as-cf "Convert task to CompletableFuture"
            (let [cf (q/as-cf (q/task 42))]
              (is (instance? CompletableFuture cf))
              (is (= 42 @cf))))

          (clause :as-cf-task-error "Task error propagates to CF"
            (is (thrown? Exception @(q/as-cf (q/task (throw (Exception. "Error")))))))

          (clause :as-cf-cf-error "Task cancellation propagates to CF"
            (let [task (q/sleep 10000 :result)
                  cf   (q/as-cf task)]
              (with-task (q/cancel task)
                (result []
                  (is (CompletableFuture/.isCancelled cf))))))

          (clause :as-cf-propagation "CF cancellation does NOT propagate to task (one-way only)"
            (let [task (q/sleep 100 :result)
                  cf   (doto (q/as-cf task) (CompletableFuture/.cancel true))]
              (with-task task
                (result [v]
                  (is (CompletableFuture/.isCancelled cf))
                  (is (= :result v))))))))


#?(:clj (def-results future
          (clause :q-convert "Java Futures ARE automatically coerced in data structures"
            (with-task (q (future 42))
              (result [v]
                (is (= 42 v)))))

          (clause :q-convert-nested "Futures in nested structures are auto-coerced"
            (with-task (q {:nested {:fut1 (future 100)
                                    :fut2 (future 200)}})
              (result [v]
                (is (= 100 (get-in v [:nested :fut1])))
                (is (= 200 (get-in v [:nested :fut2]))))))

          (clause :fut-cancellation "Future cancellation propagates to Task"
            (let [fut  (future (Thread/sleep 10000) :result)
                  task (q fut)]
              (.cancel fut true)
              (with-task task
                (result [_v e c]
                  (is (true? c))
                  (is (some? e))))))

          (testing :task-cancellation "Task cancellation propagates to Future"
                   (let [fut  (future (Thread/sleep 10000) :result)
                         task (q fut)]
                     (with-task (q/cancel task)
                       (result [v]
                         (is (.isCancelled fut))))))))


#?(:cljs (def-results js-promise
           (clause :q-convert "q with js/Promise"
             (with-task (q (js/Promise.resolve 1))
               (result [v]
                 (is (= 1 v)))))

           (clause :q-convert-nested "task/q with js/Promise in data structure"
             (with-task (q {:a (js/Promise.resolve 1)})
               (result [v]
                 (is (= {:a 1} v)))))

           (clause :q-convert-multiple "Multiple js/Promises in structure"
             (with-task (q {:a (js/Promise.resolve 1)
                            :b (js/Promise.resolve 2)})
               (result [v]
                 (is (= {:a 1 :b 2} v)))))

           (clause :as-jsp "Convert task to JS Promise"
             (let [p (q/as-jsp (q/task :result))]
               (with-task (q p)
                 (result [v]
                   (is (= :result v))))))

           (clause :as-jsp-error "Task error propagates to JS Promise"
             (let [p (q/as-jsp (q/task (throw (make-exception "Error"))))]
               (with-task (q p)
                 (result [_v e]
                   (is (some? e))))))

           (clause :as-jsp-cancellation "Task cancellation rejects JS Promise"
             (let [task (q/sleep 10000 :result)
                   p    (q/as-jsp task)]
               (with-task (q/then (q/cancel task) (fn [_] (q p)))
                 (result [_v e]
                   (is (some? e) "Cancelled task should reject the JS Promise")))))))


(def-results core-async
  (clause :not-auto-ground "core.async channels are NOT automatically coerced in data structures"
    (let [ch (chan)]
      (put! ch 42)
      ;; Channel should remain a channel, not be converted to task
      (with-task (q/as-task {:channel ch})
        (result [v]
          (is (instance? ManyToManyChannel (:channel v)))))))

  (testing :direct-conversion "Manual conversion with q works"
           (let [ch (chan)]
             (put! ch 123)
             (with-task (q/as-task ch)
               (result [v]
                 (is (= 123 v)))))))
