(ns co.multiply.quiescent.interop-test
  "Tests for interoperability with CompletableFuture, core.async channels,
   Clojure futures, and blocking deref operations."
  (:require
    [clojure.core.async]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.adapter.core-async]
    [co.multiply.quiescent.test-support :refer [platform-thread-fixture]])
  (:import
    [clojure.core.async.impl.channels ManyToManyChannel]
    [java.util.concurrent CancellationException CompletableFuture]))


(use-fixtures :once platform-thread-fixture)


(deftest completable-future-interop-test
  (testing "as-task with CompletableFuture"
    (is (= 1 @(q/as-task (CompletableFuture/completedFuture 1)))))

  (testing "task/q with CompletableFuture directly"
    (is (= 1 @(q/q (CompletableFuture/completedFuture 1)))))

  (testing "task/q with CompletableFuture in data structure"
    (is (= {:a 1} @(q/q {:a (CompletableFuture/completedFuture 1)}))))

  (testing "Multiple CompletableFutures in structure"
    (is (= {:a 1 :b 2}
          @(q/q {:a (CompletableFuture/completedFuture 1)
                :b (CompletableFuture/completedFuture 2)})))))


(deftest channel-not-auto-coerced-test
  (testing "core.async channels are NOT automatically coerced in data structures"
    (let [ch (clojure.core.async/chan)]
      (clojure.core.async/put! ch 42)
      ;; Channel should remain a channel, not be converted to task
      (let [result @(q/q {:channel ch})]
        (is (instance? ManyToManyChannel (:channel result)))
        ;; Can still read from the channel
        (is (= 42 (clojure.core.async/<!! (:channel result)))))))

  (testing "Channels in nested structures stay as channels"
    (let [ch1 (clojure.core.async/chan)
          ch2 (clojure.core.async/chan)]
      (clojure.core.async/put! ch1 :a)
      (clojure.core.async/put! ch2 :b)
      (let [result @(q/q {:nested {:ch1 ch1
                                   :ch2 ch2}})]
        (is (instance? ManyToManyChannel (get-in result [:nested :ch1])))
        (is (instance? ManyToManyChannel (get-in result [:nested :ch2]))))))

  (testing "Manual conversion with as-task works"
    (let [ch (clojure.core.async/chan)]
      (clojure.core.async/put! ch 123)
      ;; Explicit conversion should work
      (is (= 123 @(q/as-task ch)))))

  (testing "Manual conversion in data structure"
    (let [ch (clojure.core.async/chan)]
      (clojure.core.async/put! ch :value)
      ;; Wrap with as-task to convert
      (is (= {:result :value}
            @(q/q {:result (q/as-task ch)}))))))


(deftest future-auto-coerced-test
  (testing "Java Futures ARE automatically coerced in data structures"
    (let [fut (future 42)]
      ;; Future should be automatically converted to task
      (let [result @(q/q {:future fut})]
        ;; Should get the dereferenced value, not the future itself
        (is (= 42 (:future result))))))

  (testing "Futures in nested structures are auto-coerced"
    (let [fut1 (future 100)
          fut2 (future 200)]
      (let [result @(q/q {:nested {:fut1 fut1
                                   :fut2 fut2}})]
        (is (= 100 (get-in result [:nested :fut1])))
        (is (= 200 (get-in result [:nested :fut2]))))))

  (testing "Manual conversion with as-task also works for Future"
    (let [fut (future 100)]
      ;; Explicit conversion should work
      (is (= 100 @(q/as-task fut)))))

  (testing "Future cancellation propagates to Task"
    (let [fut  (future (Thread/sleep 10000) :result)
          task (q/as-task fut)]
      ;; Cancel the future directly
      (.cancel fut true)
      ;; Task should see the cancellation
      (is (thrown? CancellationException @task))))

  (testing "Task cancellation propagates to Future"
    (let [fut  (future (Thread/sleep 10000) :result)
          task (q/as-task fut)]
      ;; Cancel the task and wait for cancellation to complete
      @(q/cancel task)
      ;; Future should be cancelled
      (is (.isCancelled fut)))))


(deftest blocking-deref-timeout-test
  (testing "Blocking deref with timeout returns default"
    (let [task (q/task (Thread/sleep 1000) :result)]
      ;; Should timeout and return default
      (is (= :timed-out (deref task 50 :timed-out)))))

  (testing "Blocking deref completes before timeout"
    (let [task (q/task (Thread/sleep 10) :result)]
      ;; Should complete successfully
      (is (= :result (deref task 1000 :timed-out)))))

  (testing "Blocking deref respects thread interruption"
    (let [task         (q/task (Thread/sleep 10000) :result)
          worker-error (atom nil)
          worker       (Thread.
                         (fn []
                           (try
                             (deref task 20000 :default)
                             (catch Throwable e
                               (reset! worker-error e)))))]
      (.start worker)
      (Thread/sleep 10)
      (.interrupt worker)
      (.join worker 1000)
      (is (instance? InterruptedException @worker-error)))))


(deftest as-cf-test
  (testing "Convert task to CompletableFuture"
    (let [cf (q/as-cf (q/task 42))]
      (is (instance? CompletableFuture cf))
      (is (= 42 @cf))))

  (testing "Task error propagates to CF"
    (let [cf (q/as-cf (q/task (throw (Exception. "Error"))))]
      (is (thrown? Exception @cf))))

  (testing "Task cancellation propagates to CF"
    (let [task (q/task (Thread/sleep 10000) :result)
          cf   (q/as-cf task)]
      @(q/cancel task)                                      ; Wait for cancellation to complete (cancel returns Task[Boolean])
      (is (.isCancelled cf))))

  (testing "CF cancellation does NOT propagate to task (one-way only)"
    (let [task (q/task (Thread/sleep 100) :result)
          cf   (q/as-cf task)]
      (.cancel cf true)
      ;; CF is cancelled but task continues
      (is (.isCancelled cf))
      (is (= :result @task)))))


(deftest get-now-test
  (testing "get-now returns default for incomplete task"
    (let [task (q/task (Thread/sleep 1000) :result)]
      (is (= :not-ready (q/get-now task :not-ready)))))

  (testing "get-now returns value for completed task"
    (let [task (q/task 42)]
      (Thread/sleep 10)
      (is (= 42 (q/get-now task :default)))))

  (testing "get-now throws for exceptionally completed task"
    (let [task (q/task (throw (Exception. "Error")))]
      (Thread/sleep 10)
      (is (thrown? Exception (q/get-now task :default)))))

  (testing "get-now is non-blocking"
    (let [task       (q/task (Thread/sleep 100) :result)
          start-time (System/currentTimeMillis)
          result     (q/get-now task :not-ready)
          elapsed    (- (System/currentTimeMillis) start-time)]
      (is (= :not-ready result))
      (is (< elapsed 50) "Should return immediately, not block"))))


(deftest exceptional-test
  (testing "Returns false for incomplete task"
    (let [task (q/task (Thread/sleep 1000) :result)]
      (is (false? (q/exceptional? task)))))

  (testing "Returns false for successfully completed task"
    (let [task (q/task 42)]
      @task
      (is (false? (q/exceptional? task)))))

  (testing "Returns true for exceptionally completed task"
    (let [task (q/task (throw (Exception. "Error")))]
      (Thread/sleep 10)
      (is (true? (q/exceptional? task)))))

  (testing "Returns true for failed-task"
    (let [task (q/failed-task (Exception. "Pre-failed"))]
      (is (true? (q/exceptional? task)))))

  (testing "Returns true for cancelled task"
    (let [task (q/task (Thread/sleep 1000) :result)]
      (q/cancel task)
      (Thread/sleep 10)
      (is (true? (q/exceptional? task))))))
