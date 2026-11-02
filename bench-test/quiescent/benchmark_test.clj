(ns quiescent.benchmark-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
    [co.multiply.quiescent.channel :as ch]
    [quiescent.benchmark :as bench]
    [quiescent.allocation :as allocation]
    [quiescent.scenarios :as scenarios]
    [quiescent.workload :as workload])
  (:import [java.util.concurrent CountDownLatch]))


(deftest option-validation
  (doseq [opts [{:unknown true} {:cases []} {:cases [:typo]} {:frameworks [:typo]}
                {:forks 0} {:profile :typo} {:suite :typo} {:list? :yes}]]
    (is (thrown? clojure.lang.ExceptionInfo (bench/options opts)))))


(deftest balanced-schedule
  (let [opts (bench/options {:cases [:single] :forks 4})
        plan (bench/schedule opts)]
    (is (= [:quiescent :core-async :core-async :quiescent
            :quiescent :core-async :core-async :quiescent]
           (mapv :framework plan)))
    (is (= 8 (count (distinct (map :directory plan)))))
    (is (= 4 (count (filter #(= :quiescent (:framework %)) plan))))))


(deftest independent-validation-detects-loss-and-duplication
  (let [spec [{:type :throughput :n 4}]
        result {:role :consumer :workload 0 :count 4 :sum 6 :finished-ns 1}]
    (is (seq (workload/validate! spec [(assoc result :values [0 1 2 3])] true)))
    ;; Count and sum alone cannot distinguish these: exact preflight must.
    (is (thrown? clojure.lang.ExceptionInfo
          (workload/validate! spec [(assoc result :values [0 0 3 3])] true)))
    (is (thrown? clojure.lang.ExceptionInfo
          (workload/validate! spec [(assoc result :count 3)] false)))))


(deftest every-topology-completes-with-exact-results
  (is (= (count scenarios/cases) (count scenarios/ids) (count (distinct scenarios/ids))))
  (doseq [spec scenarios/cases framework [:quiescent :core-async]]
    (testing (str (:id spec) " " framework)
      (let [result (workload/execute! spec framework {:small? true :exact? true :timeout-ms 15000})]
        (is (= :exact (:validation result)))
        (is (pos? (:elapsed-ns result)))
        (is (= (count (scenarios/workloads spec true)) (count (:workloads result))))))))


(deftest worker-artifacts-must-match-the-measurement
  (let [opts (bench/options {:profile :smoke :cases [:single] :forks 1})
        request {:options opts :context {} :entry (first (bench/schedule opts))}
        sample {:elapsed-ns 100 :validation :count-and-sum}
        run {:status :complete :preflight :passed :postflight :passed
             :request request :context {} :process {:pid 123}
             :settings (bench/profiles :smoke) :spec (first scenarios/cases)
             :allocation {:status :unsupported}
             :warmup [sample] :samples [sample sample] :mean-ns 100.0}]
    (is (= run (bench/validate-worker! run request 123)))
    (doseq [bad [(assoc run :status :running) (assoc run :samples [sample])
                 (assoc run :mean-ns 50.0) (assoc run :context {:changed true})
                 (assoc run :postflight :failed) (assoc run :process {:pid 99})]]
      (is (thrown? clojure.lang.ExceptionInfo (bench/validate-worker! bad request 123))))))


(deftest whole-jvm-allocation-captures-virtual-threads
  (when-let [bean (allocation/bean)]
    (let [probe (allocation/probe! bean)]
      (is (= :passed (:status probe)))
      (is (:virtual-thread-terminated? probe))
      (is (<= (:expected-payload-bytes probe) (:observed-bytes probe))))))


(deftest failed-trial-cleanup-does-not-block-behind-an-earlier-put
  (let [channel (ch/chan 1)
        _ (ch/put! channel :full)
        ^java.lang.reflect.Field lock-field
        (doto (.getDeclaredField (class channel) "putLock") (.setAccessible true))
        lock (.get lock-field channel)
        ;; Hold ownership so cleanup's seal cannot complete before the test
        ;; releases it. This models a producer suspended on a full buffer.
        blocker (.acquire ^co.multiply.quiescent.impl.channel.RelayLock lock)
        result (promise)
        preparation {:ready (CountDownLatch. 0) :done (CountDownLatch. 1)
                     :gates [] :results [] :handles [] :channels [channel] :specs []}]
    (try
      (with-redefs [workload/prepare (fn [& _] preparation)]
        (let [worker (Thread/startVirtualThread
                       #(deliver result
                          (try
                            (workload/execute! {:id :blocked} :quiescent {:timeout-ms 25})
                            (catch Throwable e e))))
              error (deref result 2000 ::timeout)]
          (is (instance? clojure.lang.ExceptionInfo error)
            "The transfer timeout must escape even while cleanup is queued")
          (when (instance? clojure.lang.ExceptionInfo error)
            (is (= :transfer (get-in (ex-data error) [:cause :phase]))))
          (.join worker 100)))
      (finally (.release blocker)))
    ;; Complete any queued cleanup and leave no channel workers behind.
    (ch/seal! channel)
    (is (= :full (ch/take! channel)))
    (is (= ::end (ch/take! channel ::end)))))


(defn -main
  [& _]
  (let [result (run-tests 'quiescent.benchmark-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
