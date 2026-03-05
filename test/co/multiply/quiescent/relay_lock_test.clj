(ns co.multiply.quiescent.relay-lock-test
  (:require
    [clojure.test :refer [deftest is testing]])
  (:import
    [co.multiply.quiescent.impl.channel RelayLock RelayLock$Node]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.atomic AtomicLong AtomicBoolean]))


(defn- start-virtual-thread
  ^Thread [^Runnable f]
  (.start (Thread/ofVirtual) f))


(deftest uncontended-acquire-release-test
  (testing "Single thread can acquire and release repeatedly"
    (let [lock (RelayLock.)
          n    10000]
      (dotimes [_ n]
        (let [node (.acquire lock)]
          (.release lock node)))
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
                    (.release lock node)))))
            (.countDown latch))))
      (is (.await latch 10 TimeUnit/SECONDS)
        "Both threads should complete within timeout")
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
                    (.release lock node)))))
            (.countDown latch))))
      (is (.await latch 30 TimeUnit/SECONDS)
        "All threads should complete within timeout")
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
                    (.release lock node)))))
            (.countDown latch))))
      (is (.await latch 30 TimeUnit/SECONDS)
        "All threads should complete within timeout")
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
                             (.release lock n))))
                       (.countDown done)))]
      (.await started 5 TimeUnit/SECONDS)
      (Thread/sleep 10)
      (.interrupt t)
      (.release lock node)
      (is (.await done 10 TimeUnit/SECONDS)
        "Interrupted thread should complete within timeout")
      (is (.get result)
        "Interrupted thread acquired the lock and ran"))))
