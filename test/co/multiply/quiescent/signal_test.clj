(ns co.multiply.quiescent.signal-test
  (:require
    [clojure.test :refer [deftest is testing]])
  (:import
    [co.multiply.quiescent.impl.channel RelayLock Signal]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.atomic AtomicLong AtomicBoolean]))


(defn- start-virtual-thread
  ^Thread [^Runnable f]
  (.start (Thread/ofVirtual) f))


(deftest signal-before-await-test
  (testing "Signal before await — park returns immediately (banked permit)"
    (let [sig     (Signal.)
          result  (AtomicBoolean. false)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (Thread/sleep 10)
          (.signal sig)))
      (start-virtual-thread
        (fn []
          (.await sig)
          (.set result true)
          (.countDown done)))
      (is (.await done 5 TimeUnit/SECONDS)
        "Await should complete within timeout")
      (is (.get result)))))


(deftest await-then-signal-test
  (testing "Await parks, signal wakes"
    (let [sig     (Signal.)
          result  (AtomicBoolean. false)
          started (CountDownLatch. 1)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (.countDown started)
          (.await sig)
          (.set result true)
          (.countDown done)))
      (.await started 5 TimeUnit/SECONDS)
      (Thread/sleep 10)
      (.signal sig)
      (is (.await done 5 TimeUnit/SECONDS)
        "Await should complete within timeout")
      (is (.get result)
        "Awaiting thread should have woken up"))))


(deftest locked-producer-consumer-test
  (testing "RelayLock + Signal: locked producer/consumer with buffer"
    (let [put-lock  (RelayLock.)
          take-lock (RelayLock.)
          sig       (Signal.)
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
                    (.await sig)
                    (recur)))
                ;; Deposit
                (let [idx (mod (.getAndIncrement put-idx) capacity)]
                  (aset buffer idx (long i)))
                (.incrementAndGet count)
                (.incrementAndGet produced)
                (.signal sig)
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
                    (.await sig)
                    (recur)))
                ;; Consume
                (let [idx (mod (.getAndIncrement take-idx) capacity)]
                  (aget buffer idx))
                (.decrementAndGet count)
                (.incrementAndGet consumed)
                (.signal sig)
                (finally
                  (.release take-lock node)))))
          (.countDown done)))
      (is (.await done 15 TimeUnit/SECONDS)
        "Producer and consumer should complete within timeout")
      (is (= target (.get produced))
        "All items should be produced")
      (is (= target (.get consumed))
        "All items should be consumed"))))


(deftest locked-mpmc-test
  (testing "RelayLock + Signal: multiple producers and consumers"
    (let [put-lock  (RelayLock.)
          take-lock (RelayLock.)
          sig       (Signal.)
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
                      (.await sig)
                      (recur)))
                  (let [idx (mod (.getAndIncrement put-idx) capacity)]
                    (aset buffer idx (long i)))
                  (.incrementAndGet count)
                  (.signal sig)
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
                      (.await sig)
                      (recur)))
                  (let [idx (mod (.getAndIncrement take-idx) capacity)]
                    (aget buffer idx))
                  (.decrementAndGet count)
                  (.incrementAndGet consumed)
                  (.signal sig)
                  (finally
                    (.release take-lock node)))))
            (.countDown done))))
      (is (.await done 30 TimeUnit/SECONDS)
        "All threads should complete within timeout")
      (is (= total (.get consumed))
        "All items should be consumed"))))


(deftest rapid-signal-no-waiter-test
  (testing "Rapid signals with nobody waiting — no crash"
    (let [sig (Signal.)
          n   10000]
      (dotimes [_ n]
        (.signal sig))
      (is true "Completed without error"))))
