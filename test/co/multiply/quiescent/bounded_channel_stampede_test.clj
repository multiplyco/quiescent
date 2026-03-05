(ns co.multiply.quiescent.bounded-channel-stampede-test
  (:require
    [clojure.test :refer [deftest is testing]])
  (:import
    [co.multiply.quiescent.impl.channel BoundedChannel IChannel]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.atomic AtomicLong]))


(defn- start-virtual-thread
  ^Thread [^Runnable f]
  (.start (Thread/ofVirtual) f))


(deftest mpmc-2p1c-direct-test
  (testing "2 producers, 1 consumer, direct channel API"
    (let [ch        (BoundedChannel. 8)
          counter   (AtomicLong. 0)
          n-puts    2000
          latch     (CountDownLatch. 3)]
      ;; 2 producers
      (dotimes [_ 2]
        (start-virtual-thread
          (fn []
            (dotimes [_ n-puts]
              (.put ch :v))
            (.countDown latch))))
      ;; 1 consumer
      (start-virtual-thread
        (fn []
          (dotimes [_ (* 2 n-puts)]
            (let [v (.take ch)]
              (when (not (identical? v IChannel/CANCELLED))
                (.incrementAndGet counter))))
          (.countDown latch)))
      (is (.await latch 15 TimeUnit/SECONDS)
        "All threads should complete within 15 seconds")
      (is (= (* 2 n-puts) (.get counter))))))


(deftest mpmc-1p2c-direct-test
  (testing "1 producer, 2 consumers, direct channel API"
    (let [ch        (BoundedChannel. 8)
          counter   (AtomicLong. 0)
          n-puts    4000
          latch     (CountDownLatch. 3)]
      ;; 1 producer
      (start-virtual-thread
        (fn []
          (dotimes [_ n-puts]
            (.put ch :v))
          (.countDown latch)))
      ;; 2 consumers
      (dotimes [_ 2]
        (start-virtual-thread
          (fn []
            (dotimes [_ (/ n-puts 2)]
              (let [v (.take ch)]
                (when (not (identical? v IChannel/CANCELLED))
                  (.incrementAndGet counter))))
            (.countDown latch))))
      (is (.await latch 15 TimeUnit/SECONDS)
        "All threads should complete within 15 seconds")
      (is (= n-puts (.get counter))))))



(deftest mpmc-4p4c-direct-test
  (testing "4 producers, 4 consumers, direct channel API"
    (let [ch        (BoundedChannel. 8)
          counter   (AtomicLong. 0)
          per-thread 2500
          n-prod    4
          n-cons    4
          total     (* n-prod per-thread)
          latch     (CountDownLatch. (+ n-prod n-cons))]
      (dotimes [_ n-prod]
        (start-virtual-thread
          (fn []
            (dotimes [_ per-thread]
              (.put ch :v))
            (.countDown latch))))
      (dotimes [_ n-cons]
        (start-virtual-thread
          (fn []
            (dotimes [_ (/ total n-cons)]
              (let [v (.take ch)]
                (when (not (identical? v IChannel/CANCELLED))
                  (.incrementAndGet counter))))
            (.countDown latch))))
      (is (.await latch 30 TimeUnit/SECONDS)
        "All threads should complete within 30 seconds")
      (is (= total (.get counter))))))
