(ns co.multiply.quiescent.channel.alt-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent.channel :refer [alt chan put! take! cancel! seal! poll]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park timeout-fixture]])
  (:import
    [co.multiply.quiescent.impl.channel IChannel]
    [java.util.concurrent CancellationException CountDownLatch TimeUnit]
    [java.util.concurrent.atomic AtomicLong]))


(allow-platform-park)
(use-fixtures :each (timeout-fixture))


(defn- start-virtual-thread
  ^Thread [^Runnable f]
  (.start (Thread/ofVirtual) f))


;; # Prio take
;; ################################################################################

(deftest prio-take-first-ready-test
  (testing "Takes from first channel when it has a value"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          a   (alt :prio ch1 ch2)]
      (put! ch1 :a)
      (put! ch2 :b)
      (is (= :a (take! a))))))


(deftest prio-take-second-ready-test
  (testing "Takes from second channel when first is empty"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          a   (alt :prio ch1 ch2)]
      (put! ch2 :b)
      (is (= :b (take! a))))))


(deftest prio-take-always-prefers-first-test
  (testing "With values on both, always takes from first"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          a   (alt :prio ch1 ch2)]
      (dotimes [i 4]
        (put! ch1 i)
        (put! ch2 (+ i 100)))
      (is (= [0 1 2 3] (mapv (fn [_] (take! a)) (range 4)))))))


;; # Fair take — M2
;; ################################################################################

;; (deftest fair-take-round-robins-test
;;   (testing "Fair mode distributes takes across channels"
;;     (let [ch1 (chan 4)
;;           ch2 (chan 4)
;;           a   (alt :fair ch1 ch2)]
;;       (dotimes [i 4]
;;         (put! ch1 i)
;;         (put! ch2 (+ i 100)))
;;       (let [results (mapv (fn [_] (take! a)) (range 4))
;;             from-1  (filter #(< % 100) results)
;;             from-2  (filter #(>= % 100) results)]
;;         (is (pos? (count from-1)) "Should take some from ch1")
;;         (is (pos? (count from-2)) "Should take some from ch2")))))


;; # Prio put — M4
;; ################################################################################

;; (deftest prio-put-first-has-space-test
;;   (testing "Puts to first channel when it has space"
;;     (let [ch1 (chan 4)
;;           ch2 (chan 4)
;;           a   (alt :prio ch1 ch2)]
;;       (put! a :v)
;;       (is (= :v (take! ch1))))))
;;
;;
;; (deftest prio-put-first-full-test
;;   (testing "Puts to second channel when first is full"
;;     (let [ch1 (chan 1)
;;           ch2 (chan 4)
;;           a   (alt :prio ch1 ch2)]
;;       (put! ch1 :fill)
;;       (put! a :v)
;;       (is (= :v (take! ch2))))))


;; # Fair put — M4
;; ################################################################################

;; (deftest fair-put-round-robins-test
;;   (testing "Fair mode distributes puts across channels"
;;     (let [ch1 (chan 8)
;;           ch2 (chan 8)
;;           a   (alt :fair ch1 ch2)]
;;       (dotimes [_ 4] (put! a :v))
;;       (let [c1 (.count ch1)
;;             c2 (.count ch2)]
;;         (is (= 4 (+ c1 c2)))
;;         (is (pos? c1) "Should put some to ch1")
;;         (is (pos? c2) "Should put some to ch2")))))


;; # Blocking / parking
;; ################################################################################

(deftest take-parks-until-value-arrives-test
  (testing "Take parks when all channels empty, wakes when value arrives"
    (let [ch1    (chan 4)
          ch2    (chan 4)
          a      (alt :prio ch1 ch2)
          result (promise)
          _      (start-virtual-thread
                   (fn [] (deliver result (take! a))))]
      (Thread/sleep 50)
      (is (not (realized? result)) "Should be parked")
      (put! ch2 :woke)
      (is (= :woke (deref result 5000 :timeout))))))


;; (deftest put-parks-until-space-available-test
;;   (testing "Put parks when all channels full, wakes when space freed"
;;     (let [ch1  (chan 1)
;;           ch2  (chan 1)
;;           a    (alt :prio ch1 ch2)
;;           done (promise)]
;;       (put! ch1 :fill1)
;;       (put! ch2 :fill2)
;;       (start-virtual-thread
;;         (fn [] (put! a :v) (deliver done true)))
;;       (Thread/sleep 50)
;;       (is (not (realized? done)) "Should be parked")
;;       (take! ch1)
;;       (is (deref done 5000 false)))))


;; # Composition (alt-within-alt) — M5
;; ################################################################################

;; (deftest nested-alt-test
;;   (testing "Alt within alt works as a single channel"
;;     (let [ch1   (chan 4)
;;           ch2   (chan 4)
;;           ch3   (chan 4)
;;           inner (alt :fair ch2 ch3)
;;           outer (alt :prio ch1 inner)]
;;       (put! ch3 :from-inner)
;;       (is (= :from-inner (take! outer))))))
;;
;;
;; (deftest nested-alt-prio-prefers-outer-test
;;   (testing "Prio outer alt prefers its first channel over inner alt"
;;     (let [ch1   (chan 4)
;;           ch2   (chan 4)
;;           inner (alt :fair ch2)
;;           outer (alt :prio ch1 inner)]
;;       (put! ch1 :outer)
;;       (put! ch2 :inner)
;;       (is (= :outer (take! outer))))))


;; # Lifecycle / cancellation
;; ################################################################################

(deftest any-cancelled-channel-surfaces-immediately-test
  (testing ":any mode surfaces cancellation when a channel is cancelled"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          a   (alt :prio :any ch1 ch2)]
      (cancel! ch1)
      (is (thrown? CancellationException (take! a))))))


(deftest all-skips-cancelled-channel-test
  (testing ":all mode skips cancelled channels and takes from remaining"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          a   (alt :prio ch1 ch2)]
      (cancel! ch1)
      (put! ch2 :still-here)
      (is (= :still-here (take! a))))))


(deftest all-sealed-drained-returns-cancelled-test
  (testing ":all mode returns cancelled when all channels sealed and drained"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          a   (alt :prio ch1 ch2)]
      (seal! ch1)
      (seal! ch2)
      (is (thrown? CancellationException (take! a))))))


(deftest sealed-channel-drains-first-test
  (testing "Alt drains remaining values from sealed channel"
    (let [ch1 (chan 4)
          a   (alt :prio ch1)]
      (put! ch1 :v)
      (seal! ch1)
      (is (= :v (take! a))))))


;; # Null values
;; ################################################################################

(deftest nil-value-passes-through-test
  (testing "nil is a valid value, not confused with UNAVAILABLE"
    (let [ch (chan 4)
          a  (alt :prio ch)]
      (put! ch nil)
      (is (nil? (take! a))))))


;; # Collections in constructor
;; ################################################################################

(deftest flatten-channels-test
  (testing "Collections of channels are flattened"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          ch3 (chan 4)
          a   (alt :fair [ch1 ch2] ch3)]
      (put! ch3 :v)
      (is (= :v (take! a))))))


;; # Concurrency stress
;; ################################################################################

(deftest minimal-put-take-seal-test
  (testing "Single producer, single consumer, seal terminates"
    (let [ch1  (chan 4)
          ch2  (chan 4)
          a    (alt :prio ch1 ch2)
          done (CountDownLatch. 2)
          vals (java.util.concurrent.ConcurrentLinkedQueue.)]
      (start-virtual-thread
        (fn []
          (dotimes [i 10] (put! ch1 i))
          (seal! ch1)
          (seal! ch2)
          (.countDown done)))
      (start-virtual-thread
        (fn []
          (loop []
            (let [v (IChannel/.take a)]
              (if (identical? v IChannel/CANCELLED)
                (.countDown done)
                (do (.add vals v) (recur)))))))
      (is (.await done 5 TimeUnit/SECONDS) "Should complete within 5 seconds")
      (is (= 10 (.size vals)) "All 10 values consumed"))))


(deftest mpmc-stress-test
  (testing "Multiple producers and consumers through alt — no lost values"
    (let [ch1       (chan 16)
          ch2       (chan 16)
          a         (alt :prio ch1 ch2)
          n-prod    4
          n-cons    4
          n-per     1000
          total     (* n-prod n-per)
          counter   (AtomicLong. 0)
          prod-done (CountDownLatch. n-prod)
          all-done  (CountDownLatch. (+ n-prod n-cons))]
      ;; Producers put directly to underlying channels
      (dotimes [p n-prod]
        (start-virtual-thread
          (fn []
            (let [ch (if (even? p) ch1 ch2)]
              (dotimes [_ n-per]
                (put! ch :v)))
            (.countDown prod-done)
            (.countDown all-done))))
      ;; Seal after all producers finish
      (start-virtual-thread
        (fn []
          (.await prod-done)
          (seal! ch1)
          (seal! ch2)))
      ;; Consumers take through alt until sealed+drained
      (dotimes [_ n-cons]
        (start-virtual-thread
          (fn []
            (loop []
              (poll [_ a]
                (do (.incrementAndGet counter) (recur))
                nil))
            (.countDown all-done))))
      (is (.await all-done 30 TimeUnit/SECONDS)
        "All threads should complete within 30 seconds")
      (is (= total (.get counter))
        "All values should be consumed"))))


;; # New M1 tests
;; ################################################################################

(deftest prio-take-many-channels-test
  (testing "VT cascade reaches the last of many channels"
    (let [chs (vec (repeatedly 5 #(chan 4)))
          a   (alt :prio (seq chs))]
      (put! (nth chs 4) :found)
      (is (= :found (take! a))))))


(deftest prio-take-concurrent-alts-test
  (testing "Multiple concurrent alts — exactly-once delivery"
    (let [ch1     (chan 128)
          ch2     (chan 128)
          n       50
          results (java.util.concurrent.ConcurrentLinkedQueue.)
          latch   (CountDownLatch. n)]
      (dotimes [i n] (put! (if (even? i) ch1 ch2) i))
      (dotimes [_ n]
        (let [a (alt :prio ch1 ch2)]
          (start-virtual-thread
            (fn []
              (.add results (take! a))
              (.countDown latch)))))
      (is (.await latch 5 TimeUnit/SECONDS))
      (is (= n (.size results)) "Each alt got exactly one value")
      (is (= (set (range n)) (set (seq results))) "No duplicates or lost values"))))


(deftest prio-take-late-arrival-test
  (testing "Alt parks on all-empty channels, wakes when value arrives on ch3"
    (let [ch1    (chan 4)
          ch2    (chan 4)
          ch3    (chan 4)
          a      (alt :prio ch1 ch2 ch3)
          result (promise)]
      (start-virtual-thread
        (fn [] (deliver result (take! a))))
      (Thread/sleep 50)
      (is (not (realized? result)) "Should be parked")
      (put! ch3 :late)
      (is (= :late (deref result 5000 :timeout))))))


(deftest prio-take-single-channel-test
  (testing "Degenerate case: Alt with one channel"
    (let [ch (chan 4)
          a  (alt :prio ch)]
      (put! ch :solo)
      (is (= :solo (take! a))))))


(deftest prio-take-interleaved-with-regular-takers-test
  (testing "Alt VTs and regular take! calls coexist on the same channel"
    (let [ch      (chan 128)
          a       (alt :prio ch)
          n       100
          alt-results (java.util.concurrent.ConcurrentLinkedQueue.)
          reg-results (java.util.concurrent.ConcurrentLinkedQueue.)
          latch   (CountDownLatch. n)]
      (dotimes [i n] (put! ch i))
      (dotimes [k n]
        (start-virtual-thread
          (fn []
            (if (even? k)
              (.add alt-results (take! a))
              (.add reg-results (take! ch)))
            (.countDown latch))))
      (is (.await latch 5 TimeUnit/SECONDS))
      (let [all (into (set (seq alt-results)) (seq reg-results))]
        (is (= n (count all)) "All values consumed, no duplicates")))))


(deftest prio-take-all-cancelled-test
  (testing "All channels cancelled before alt — returns CANCELLED"
    (let [ch1 (chan 4)
          ch2 (chan 4)
          a   (alt :prio ch1 ch2)]
      (cancel! ch1)
      (cancel! ch2)
      (is (thrown? CancellationException (take! a))))))


(deftest prio-take-stress-test
  (testing "High throughput alt take — no lost values"
    (let [n-ch      4
          chs       (vec (repeatedly n-ch #(chan 64)))
          a         (alt :prio (seq chs))
          n-prod    4
          n-cons    4
          n-per     1000
          total     (* n-prod n-per)
          counter   (AtomicLong. 0)
          prod-done (CountDownLatch. n-prod)
          all-done  (CountDownLatch. (+ n-prod n-cons))]
      (dotimes [p n-prod]
        (start-virtual-thread
          (fn []
            (let [ch (nth chs (mod p n-ch))]
              (dotimes [_ n-per] (put! ch :v)))
            (.countDown prod-done)
            (.countDown all-done))))
      (start-virtual-thread
        (fn []
          (.await prod-done)
          (doseq [ch chs] (seal! ch))))
      (dotimes [_ n-cons]
        (start-virtual-thread
          (fn []
            (loop []
              (poll [_ a]
                (do (.incrementAndGet counter) (recur))
                nil))
            (.countDown all-done))))
      (is (.await all-done 10 TimeUnit/SECONDS)
        "All threads should complete")
      (is (= total (.get counter))
        "All values consumed"))))
