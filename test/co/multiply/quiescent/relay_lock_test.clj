(ns co.multiply.quiescent.relay-lock-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent.test-support :refer [timeout-fixture]])
  (:import
    [co.multiply.quiescent.impl.channel RelayLock RelayLock$Node]
    [java.util.concurrent CountDownLatch]
    [java.util.concurrent.atomic AtomicLong AtomicBoolean]))


(def ^:private ^java.lang.reflect.Field slot-field
  (doto (.getDeclaredField RelayLock "slot")
    (.setAccessible true)))


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


(deftest fifo-ordering-test
  (testing "Threads acquire the lock in arrival order"
    (let [^RelayLock lock  (RelayLock.)
          n                8
          order            (java.util.concurrent.ConcurrentLinkedQueue.)
          done             (CountDownLatch. n)
          ;; Hold the lock so all subsequent threads park in the chain
          blocker          (.acquire lock)
          ;; Launch threads one at a time, spinning on slot to confirm
          ;; each has entered the chain before launching the next
          _threads
          (reduce
            (fn [acc i]
              (let [prev-slot (.get slot-field lock)
                    t (start-virtual-thread
                        (fn []
                          (let [node (.acquire lock)]
                            (try
                              (.add order (int i))
                              (finally
                                (.release node))))
                          (.countDown done)))]
                ;; Spin until this thread has swapped into the slot
                (while (identical? prev-slot (.get slot-field lock)))
                (conj acc t)))
            []
            (range n))]
      ;; Release the blocker — threads should wake in chain order
      (.release blocker)
      (.await done)
      (is (= (vec (range n)) (vec order))
        "Acquire order must match arrival order"))))


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
