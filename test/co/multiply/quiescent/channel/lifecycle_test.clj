(ns co.multiply.quiescent.channel.lifecycle-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.channel :as ch]
    [co.multiply.quiescent.test-support :refer [allow-platform-park timeout-fixture]])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.locks LockSupport]))


(allow-platform-park)
(use-fixtures :each (timeout-fixture 15000))


(defn- await-parked!
  [^Thread thread]
  (let [deadline (+ (System/nanoTime) 2000000000)]
    (loop []
      (when (not= Thread$State/WAITING (.getState thread))
        (when (or (not (.isAlive thread)) (> (System/nanoTime) deadline))
          (throw (ex-info "Channel operation did not park" {:state (.getState thread)})))
        (LockSupport/parkNanos 100000)
        (recur)))))


(deftest take-forms-wait-while-open-and-empty
  (doseq [[label take-fn] [[:take ch/take!]
                           [:default #(ch/take! % ::exhausted)]
                           [:branch #(ch/if-take! [v %] v ::exhausted)]]]
    (testing (name label)
      (let [channel (ch/chan 1)
            result (promise)
            worker (Thread/startVirtualThread
                     (fn []
                       (deliver result
                         (try (take-fn channel)
                           (catch Throwable e e)))))]
        (try
          (await-parked! worker)
          (is (not (realized? result)))
          (ch/put! channel :value)
          (is (= :value (deref result 2000 ::timeout)))
          (finally
            (ch/seal! channel)
            (.join worker 1000)))))))


(deftest seal-releases-queued-takes
  (let [channel (ch/chan 1)
        results (object-array 8)
        done (CountDownLatch. 8)
        workers (mapv
                  (fn [i]
                    (Thread/startVirtualThread
                      (fn []
                        (try
                          (aset results i (ch/take! channel ::exhausted))
                          (catch Throwable e (aset results i e))
                          (finally (.countDown done))))))
                  (range 8))]
    (try
      ;; The first waiter holds its side's lock; the rest wait in its chain.
      (run! await-parked! workers)
      (is (= 8 (.getCount done)) "All operations are pending before sealing")
      (ch/seal! channel)
      (is (.await done 2 TimeUnit/SECONDS) "Sealing must release the whole chain")
      (is (every? #(= ::exhausted %) (vec results)))
      (is (= ::exhausted (ch/take! channel ::exhausted)))
      (finally
        (ch/seal! channel)
        (doseq [^Thread thread workers] (.join thread 1000))))))


(deftest task-lifecycle-wiring-is-explicit
  (doseq [outcome [:success :failure :cancel] wire? [false true]]
    (testing (str outcome " finally=" wire?)
      (let [ready (promise)
            proceed (CountDownLatch. 1)
            owner (q/task
                    (let [channel (ch/chan 4)]
                      (ch/put! channel :buffered)
                      (deliver ready channel)
                      (.await proceed)
                      (when (= :failure outcome)
                        (throw (ex-info "Owner failed" {})))
                      :finished))
            channel @ready
            observed (if wire?
                       (q/finally owner (fn [_ _ _] (ch/seal! channel)))
                       owner)]
        (try
          (if (= :cancel outcome)
            @(q/cancel owner)
            (.countDown proceed))
          @(q/await observed)
          (is (= (= :cancel outcome) (q/cancelled? owner)))
          (is (= wire? (ch/sealed? channel)))
          (is (= :buffered (ch/take! channel)) "Task settlement never discards values")
          (if wire?
            (is (= ::exhausted (ch/take! channel ::exhausted)))
            (do
              (is (true? (ch/put! channel :still-open)))
              (is (= :still-open (ch/take! channel ::exhausted)))))
          (finally
            (.countDown proceed)
            (ch/seal! channel)))))))
