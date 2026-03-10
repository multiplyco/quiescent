(ns co.multiply.quiescent.relay-lock-test
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


(deftest uncontended-acquire-release-test
  (testing "Single thread can acquire and release repeatedly"
    (let [lock (RelayLock.)
          n    10000]
      (dotimes [_ n]
        (let [node (.acquire lock)]
          (.release node)))
      (is true "Completed without deadlock"))))


(deftest two-threads-mutual-exclusion-test
  (testing "Two threads never hold the lock simultaneously"
    (let [lock       (RelayLock.)
          counter    (AtomicLong. 0)
          violations (AtomicLong. 0)
          n          10000
          latch      (CountDownLatch. 2)]
      (dotimes [_ 2]
        (start-virtual-thread
          (fn []
            (dotimes [_ n]
              (let [node (.acquire lock)]
                (try
                  (let [v (.incrementAndGet counter)]
                    (when (> v 1)
                      (.incrementAndGet violations)))
                  (.decrementAndGet counter)
                  (finally
                    (.release node)))))
            (.countDown latch))))
      (.await latch)
      (is (zero? (.get violations))
        "No concurrent access detected"))))


(deftest many-threads-mutual-exclusion-test
  (testing "Many threads never hold the lock simultaneously"
    (let [lock       (RelayLock.)
          counter    (AtomicLong. 0)
          violations (AtomicLong. 0)
          n-threads  16
          n-ops      5000
          latch      (CountDownLatch. n-threads)]
      (dotimes [_ n-threads]
        (start-virtual-thread
          (fn []
            (dotimes [_ n-ops]
              (let [node (.acquire lock)]
                (try
                  (let [v (.incrementAndGet counter)]
                    (when (> v 1)
                      (.incrementAndGet violations)))
                  (.decrementAndGet counter)
                  (finally
                    (.release node)))))
            (.countDown latch))))
      (.await latch)
      (is (zero? (.get violations))
        "No concurrent access detected"))))


(deftest counter-stress-test
  (testing "Shared counter incremented under lock is consistent"
    (let [lock      (RelayLock.)
          counter   (AtomicLong. 0)
          n-threads 8
          n-ops     10000
          latch     (CountDownLatch. n-threads)]
      (dotimes [_ n-threads]
        (start-virtual-thread
          (fn []
            (dotimes [_ n-ops]
              (let [node (.acquire lock)]
                (try
                  (.incrementAndGet counter)
                  (finally
                    (.release node)))))
            (.countDown latch))))
      (.await latch)
      (is (= (* n-threads n-ops) (.get counter))
        "All increments should be visible"))))


(deftest interrupt-deferred-test
  (testing "Interrupted thread still completes the handoff"
    (let [lock     (RelayLock.)
          result   (AtomicBoolean. false)
          node     (.acquire lock)
          started  (CountDownLatch. 1)
          done     (CountDownLatch. 1)
          t        (start-virtual-thread
                     (fn []
                       (.countDown started)
                       (let [n (.acquire lock)]
                         (try
                           (.set result true)
                           (finally
                             (.release n))))
                       (.countDown done)))]
      (.await started)
      (Thread/sleep 10)
      (.interrupt t)
      (.release node)
      (.await done)
      (is (.get result)
        "Interrupted thread acquired the lock and ran"))))
