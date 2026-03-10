(ns co.multiply.quiescent.signal-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent.test-support :refer [timeout-fixture]])
  (:import
    [co.multiply.quiescent.impl.channel RelayLock RelayLock$Node]
    [java.util.concurrent CountDownLatch]
    [java.util.concurrent.atomic AtomicLong AtomicBoolean]))


(use-fixtures :each (timeout-fixture))


(defn- start-virtual-thread
  ^Thread [^Runnable f]
  (.start (Thread/ofVirtual) f))


(deftest signal-before-await-test
  (testing "Resume before suspend — returns immediately (banked permit)"
    (let [lock    (RelayLock.)
          result  (AtomicBoolean. false)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (Thread/sleep 10)
          (.resume lock)))
      (start-virtual-thread
        (fn []
          (let [node (RelayLock$Node. (Thread/currentThread))]
            (.suspend lock node))
          (.set result true)
          (.countDown done)))
      (.await done)
      (is (.get result)))))


(deftest await-then-signal-test
  (testing "Suspend parks, resume wakes"
    (let [lock    (RelayLock.)
          result  (AtomicBoolean. false)
          started (CountDownLatch. 1)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (.countDown started)
          (let [node (RelayLock$Node. (Thread/currentThread))]
            (.suspend lock node))
          (.set result true)
          (.countDown done)))
      (.await started)
      (Thread/sleep 10)
      (.resume lock)
      (.await done)
      (is (.get result)
        "Suspended thread should have woken up"))))


(deftest locked-producer-consumer-test
  (testing "RelayLock acquire/suspend/resume/release: locked producer/consumer with buffer"
    (let [put-lock  (RelayLock.)
          take-lock (RelayLock.)
          buffer    (long-array 8)
          capacity  (alength buffer)
          count     (AtomicLong. 0) ;; current buffer occupancy
          put-idx   (AtomicLong. 0)
          take-idx  (AtomicLong. 0)
          produced  (AtomicLong. 0)
          consumed  (AtomicLong. 0)
          target    20000
          done      (CountDownLatch. 2)]
      ;; Producer
      (start-virtual-thread
        (fn []
          (dotimes [i target]
            (let [node (.acquire put-lock)]
              (try
                ;; Wait for space
                (loop []
                  (when (>= (.get count) capacity)
                    (.suspend put-lock node)
                    (recur)))
                ;; Deposit
                (let [idx (mod (.getAndIncrement put-idx) capacity)]
                  (aset buffer idx (long i)))
                (.incrementAndGet count)
                (.incrementAndGet produced)
                (.resume take-lock)
                (finally
                  (.release put-lock node)))))
          (.countDown done)))
      ;; Consumer
      (start-virtual-thread
        (fn []
          (dotimes [_ target]
            (let [node (.acquire take-lock)]
              (try
                ;; Wait for value
                (loop []
                  (when (<= (.get count) 0)
                    (.suspend take-lock node)
                    (recur)))
                ;; Consume
                (let [idx (mod (.getAndIncrement take-idx) capacity)]
                  (aget buffer idx))
                (.decrementAndGet count)
                (.incrementAndGet consumed)
                (.resume put-lock)
                (finally
                  (.release take-lock node)))))
          (.countDown done)))
      (.await done)
      (is (= target (.get produced))
        "All items should be produced")
      (is (= target (.get consumed))
        "All items should be consumed"))))


(deftest locked-mpmc-test
  (testing "RelayLock acquire/suspend/resume/release: multiple producers and consumers"
    (let [put-lock  (RelayLock.)
          take-lock (RelayLock.)
          buffer    (long-array 8)
          capacity  (alength buffer)
          count     (AtomicLong. 0)
          put-idx   (AtomicLong. 0)
          take-idx  (AtomicLong. 0)
          consumed  (AtomicLong. 0)
          n-prod    4
          n-cons    4
          per-prod  5000
          total     (* n-prod per-prod)
          done      (CountDownLatch. (+ n-prod n-cons))]
      ;; Producers
      (dotimes [_ n-prod]
        (start-virtual-thread
          (fn []
            (dotimes [i per-prod]
              (let [node (.acquire put-lock)]
                (try
                  (loop []
                    (when (>= (.get count) capacity)
                      (.suspend put-lock node)
                      (recur)))
                  (let [idx (mod (.getAndIncrement put-idx) capacity)]
                    (aset buffer idx (long i)))
                  (.incrementAndGet count)
                  (.resume take-lock)
                  (finally
                    (.release put-lock node)))))
            (.countDown done))))
      ;; Consumers
      (dotimes [_ n-cons]
        (start-virtual-thread
          (fn []
            (dotimes [_ (/ total n-cons)]
              (let [node (.acquire take-lock)]
                (try
                  (loop []
                    (when (<= (.get count) 0)
                      (.suspend take-lock node)
                      (recur)))
                  (let [idx (mod (.getAndIncrement take-idx) capacity)]
                    (aget buffer idx))
                  (.decrementAndGet count)
                  (.incrementAndGet consumed)
                  (.resume put-lock)
                  (finally
                    (.release take-lock node)))))
            (.countDown done))))
      (.await done)
      (is (= total (.get consumed))
        "All items should be consumed"))))


(deftest rapid-signal-no-waiter-test
  (testing "Rapid resumes with nobody suspended — no crash"
    (let [lock (RelayLock.)
          n    10000]
      (dotimes [_ n]
        (.resume lock))
      (is true "Completed without error"))))
