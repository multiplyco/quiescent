(ns co.multiply.quiescent.channel.publication-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent.test-support :refer [timeout-fixture]])
  (:import [co.multiply.quiescent.impl.channel Alt BoundedChannel IChannel]
    [java.util.concurrent CountDownLatch TimeUnit]))


(use-fixtures :each (timeout-fixture 20000))


(deftest mutable-payload-publication-and-backpressure
  (doseq [capacity [1 8] xf? [false true] alt? [false true]]
    (testing (str "capacity=" capacity " transducer=" xf? " alt=" alt?)
      (let [ch (if xf? (BoundedChannel. capacity (map identity)) (BoundedChannel. capacity))
            ^IChannel source (if alt? (Alt. false false (into-array IChannel [ch])) ch)
            done (CountDownLatch. 2)
            failure (atom nil)
            n 2000
            spawn (fn [f]
                    (Thread/startVirtualThread
                      (fn []
                        (try (f)
                          (catch Throwable e (compare-and-set! failure nil e))
                          (finally (.countDown done))))))
            producer (spawn
                       (fn []
                         (dotimes [i n]
                           (let [payload (long-array 4)]
                             (dotimes [j 4] (aset-long payload j (+ (* i 4) j)))
                             (when-not (.put ch payload)
                               (throw (ex-info "Premature close" {})))))
                         (.seal ch)))
            consumer (spawn
                       (fn []
                         (dotimes [i n]
                           (let [^longs payload (.take source)]
                             (dotimes [j 4]
                               (when-not (= (+ (* i 4) j) (aget payload j))
                                 (throw (ex-info "Unpublished or out-of-order payload" {:i i :j j}))))))
                         (when-not (identical? IChannel/CANCELLED (.take source))
                           (throw (ex-info "Expected exhaustion after all payloads" {})))))]
        (try
          (is (.await done 10 TimeUnit/SECONDS) "Transfer and backpressure must make progress")
          (is (nil? @failure) (str @failure))
          (finally
            (.cancel ch nil)
            (.join ^Thread producer 1000)
            (.join ^Thread consumer 1000)))))))
