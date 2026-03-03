(ns co.multiply.quiescent.bounded-channel-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.channel :refer [buf-count cancel! cancelled? capacity chan pipe poll put! seal! sealed? saturation take!]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park]])
  (:import
    [co.multiply.quiescent.impl.channel IChannel]
    [java.util.concurrent CancellationException]))


(allow-platform-park)


;; # Construction
;; ################################################################################

(deftest capacity-rounds-to-power-of-2-test
  (testing "Rounds up to next power of 2"
    (is (= 1 (capacity (chan 1))))
    (is (= 2 (capacity (chan 2))))
    (is (= 4 (capacity (chan 3))))
    (is (= 8 (capacity (chan 5))))
    (is (= 8 (capacity (chan 7))))
    (is (= 16 (capacity (chan 16))))
    (is (= 32 (capacity (chan 31))))
    (is (= 32 (capacity (chan 32))))
    (is (= 1024 (capacity (chan 1000))))))

(deftest capacity-at-least-n-test
  (testing "Capacity is always >= requested size"
    (doseq [n [1 2 3 7 15 16 17 31 32 100 255 256 257]]
      (is (>= (capacity (chan n)) n)
        (str "capacity >= " n)))))

(deftest invalid-size-throws-test
  (testing "Zero or negative size throws IllegalArgumentException"
    (is (thrown? IllegalArgumentException (chan 0)))
    (is (thrown? IllegalArgumentException (chan -1)))
    (is (thrown? IllegalArgumentException (chan -100)))))


;; # Basic put/take
;; ################################################################################

(deftest put-take-round-trip-test
  (let [ch (chan 4)]
    (put! ch :hello)
    (is (= :hello (take! ch)))))

(deftest ring-buffer-wrap-around-test
  (testing "Generational stamp blocks producers on ring wrap until consumer frees slot"
    (let [ch      (chan 2)
          parked  (promise)
          woken   (promise)]
      ;; Fill the buffer (Gen 1, slots 0 and 1)
      (put! ch :a)
      (put! ch :b)
      ;; Attempt 3rd put (Gen 2, slot 0) — must park because slot 0 is occupied
      (q/task
        (deliver parked true)
        (put! ch :c)
        (deliver woken true))
      @parked
      (Thread/sleep 10)
      (is (not (realized? woken)) "Producer should be parked on generation wrap")
      ;; Consumer frees slot 0
      (is (= :a (take! ch)))
      ;; Producer should now unpark and write :c to slot 0
      (is (deref woken 1000 :timeout) "Producer should unpark after slot freed")
      (is (= :b (take! ch)))
      (is (= :c (take! ch))))))


(deftest fifo-ordering-test
  (let [ch (chan 8)]
    (doseq [i (range 5)]
      (put! ch i))
    (is (= [0 1 2 3 4] (mapv (fn [_] (take! ch)) (range 5))))))


(deftest put-returns-true-test
  (let [ch (chan 4)]
    (is (true? (put! ch :x)))))


(deftest nil-is-valid-value-test
  (let [ch (chan 4)]
    (put! ch nil)
    (is (nil? (take! ch)))))


;; # Query: buf-count / saturation
;; ################################################################################

(deftest empty-channel-query-test
  (let [ch (chan 4)]
    (is (= 0 (buf-count ch)))
    (is (= 0.0 (saturation ch)))))


(deftest full-channel-query-test
  (let [ch (chan 4)]
    (dotimes [i 4]
      (put! ch i))
    (is (= 4 (buf-count ch)))
    (is (= 1.0 (saturation ch)))))


(deftest buf-count-in-bounds-test
  (let [ch (chan 8)]
    (dotimes [i 5]
      (put! ch i)
      (is (<= 0 (buf-count ch) (capacity ch))))))


;; # Backpressure
;; ################################################################################

(deftest producer-parks-when-full-test
  (let [ch      (chan 1)
        started (promise)
        done    (promise)]
    (put! ch :first)
    ;; Second put should park — buffer is full
    (q/task
      (deliver started true)
      (put! ch :second)
      (deliver done true))
    @started
    (Thread/sleep 10)
    (is (not (realized? done)) "Producer should be parked")
    (is (= :first (take! ch)))
    (is (= true (deref done 1000 :timeout)) "Producer should unpark after take")
    (is (= :second (take! ch)))))


(deftest consumer-parks-when-empty-test
  (let [ch     (chan 4)
        result (promise)]
    (q/task (deliver result (take! ch)))
    (Thread/sleep 10)
    (is (not (realized? result)) "Consumer should be parked")
    (put! ch :value)
    (is (= :value (deref result 1000 :timeout)))))


;; # MPMC concurrency
;; ################################################################################

(defn- mpmc-test
  "Run n-producers × n-consumers, each handling per-thread items.
   Verify total count of all taken values equals total put count."
  [n-producers n-consumers per-thread buf-size]
  (let [ch      (chan buf-size)
        total   (* n-producers per-thread)
        results (java.util.concurrent.ConcurrentLinkedQueue.)]
    @(q/task
       (let [cs (mapv (fn [_]
                        (q/task
                          (dotimes [_ (quot total n-consumers)]
                            (.add results (take! ch)))))
                  (range n-consumers))
             ps (mapv (fn [p]
                        (q/task
                          (dotimes [i per-thread]
                            (put! ch (+ (* p per-thread) i)))))
                  (range n-producers))]
         (run! deref ps)
         (run! deref cs)))
    (is (= total (.size results))
      (str n-producers "P" n-consumers "C: expected " total " values"))))


(deftest mpmc-1p1c-test (mpmc-test 1 1 10000 64))
(deftest mpmc-4p1c-test (mpmc-test 4 1 2500 64))
(deftest mpmc-1p4c-test (mpmc-test 1 4 10000 64))
(deftest mpmc-4p4c-test (mpmc-test 4 4 2500 64))


;; # Cancellation
;; ################################################################################

(deftest cancel-stops-new-puts-test
  (let [ch (chan 4)]
    (put! ch :before)
    (is (true? (IChannel/.cancel ch nil)))
    (testing "put! throws CancellationException by default"
      (is (thrown? CancellationException (put! ch :after))))
    (testing "put! with cancel? false returns false"
      (is (false? (put! ch :after false))))))


(deftest cancel-stops-new-takes-test
  (let [ch (chan 4)]
    (put! ch :v)
    (IChannel/.cancel ch nil)
    (testing "raw take returns CANCELLED sentinel"
      (is (identical? IChannel/CANCELLED (IChannel/.take ch))))
    (testing "take! throws CancellationException"
      (is (thrown? CancellationException (take! ch))))))


(deftest cancel-wakes-parked-consumer-test
  (let [ch     (chan 4)
        result (promise)]
    (q/task (deliver result (IChannel/.take ch)))
    (Thread/sleep 10)
    (is (not (realized? result)) "Consumer should be parked")
    (IChannel/.cancel ch nil)
    (is (identical? IChannel/CANCELLED (deref result 1000 :timeout)))))


(deftest cancel-wakes-parked-producer-test
  (let [ch     (chan 1)
        result (promise)]
    (put! ch :fill)
    (q/task (deliver result (put! ch :blocked false)))
    (Thread/sleep 10)
    (is (not (realized? result)) "Producer should be parked")
    (IChannel/.cancel ch nil)
    (is (false? (deref result 1000 :timeout)))))


(deftest cancel-idempotent-test
  (let [ch (chan 4)]
    (is (true? (IChannel/.cancel ch nil)))
    (is (false? (IChannel/.cancel ch nil)))))


(deftest cancel-query-test
  (let [ch (chan 4)]
    (is (false? (IChannel/.isCancelled ch)))
    (is (false? (IChannel/.isSealed ch)))
    (IChannel/.cancel ch nil)
    (is (true? (IChannel/.isCancelled ch)))
    (is (true? (IChannel/.isSealed ch)))))


;; # Sealing
;; ################################################################################

(deftest seal-stops-new-puts-test
  (let [ch (chan 4)]
    (is (true? (IChannel/.seal ch)))
    (testing "put! throws CancellationException by default"
      (is (thrown? CancellationException (put! ch :after))))
    (testing "put! with cancel? false returns false"
      (is (false? (put! ch :after false))))))


(deftest seal-drains-buffered-values-test
  (let [ch (chan 8)]
    (put! ch :a)
    (put! ch :b)
    (put! ch :c)
    (IChannel/.seal ch)
    (is (= :a (take! ch)))
    (is (= :b (take! ch)))
    (is (= :c (take! ch)))
    (is (identical? IChannel/CANCELLED (IChannel/.take ch)))))


(deftest seal-wakes-parked-producer-test
  (let [ch     (chan 1)
        result (promise)]
    (put! ch :fill)
    (q/task (deliver result (put! ch :blocked false)))
    (Thread/sleep 10)
    (is (not (realized? result)) "Producer should be parked")
    (IChannel/.seal ch)
    (is (false? (deref result 1000 :timeout)))))


(deftest seal-idempotent-test
  (let [ch (chan 4)]
    (is (true? (IChannel/.seal ch)))
    (is (false? (IChannel/.seal ch)))))


(deftest seal-query-test
  (let [ch (chan 4)]
    (IChannel/.seal ch)
    (is (false? (IChannel/.isCancelled ch)))
    (is (true? (IChannel/.isSealed ch)))))


(deftest cancel-after-seal-test
  (let [ch (chan 4)]
    (IChannel/.seal ch)
    (is (true? (IChannel/.cancel ch nil)))
    (is (true? (IChannel/.isCancelled ch)))))


;; # poll macro
;; ################################################################################

(deftest put-cancel-false-returns-false-test
  (let [ch (chan 4)]
    (cancel! ch)
    (is (false? (put! ch :v false)) "Explicit false behaves like 2-arity")))


(deftest poll-drains-values-test
  (let [ch (chan 8)]
    (put! ch 1)
    (put! ch 2)
    (put! ch 3)
    (seal! ch)
    (is (= 6 (loop [acc 0]
               (poll [v ch]
                 (recur (+ acc v))
                 acc))))))


(deftest poll-returns-else-on-cancel-test
  (let [ch (chan 4)]
    (cancel! ch)
    (is (= :done (poll [v ch] v :done)))))


;; # pipe
;; ################################################################################

(deftest pipe-transfers-values-test
  (let [src  (chan 8)
        sink (chan 8)
        p    (pipe src sink)]
    (put! src 1 false)
    (put! src 2 false)
    (put! src 3 false)
    (seal! src)
    @p
    (is (= 1 (take! sink)))
    (is (= 2 (take! sink)))
    (is (= 3 (take! sink)))))


(deftest pipe-propagates-seal-test
  (let [src  (chan 8)
        sink (chan 8)
        p    (pipe src sink)]
    (put! src :a false)
    (seal! src)
    @p
    (is (true? (sealed? sink)))
    (is (false? (cancelled? sink)))))


(deftest pipe-propagates-cancel-test
  (let [src  (chan 8)
        sink (chan 8)
        p    (pipe src sink)]
    (put! src :a false)
    (cancel! src)
    @p
    (is (true? (cancelled? sink)))))


(deftest pipe-no-close-test
  (let [src  (chan 8)
        sink (chan 8)
        p    (pipe src sink false)]
    (put! src :a false)
    (seal! src)
    @p
    (is (false? (sealed? sink)))
    (is (false? (cancelled? sink)))))


(deftest pipe-returns-task-test
  (let [src (chan 4)
        sink (chan 4)
        p   (pipe src sink)]
    (seal! src)
    (is (deref p 1000 :timeout) "Pipe task should complete")))


(deftest pipe-with-xf-test
  (let [src  (chan 8 (map inc))
        sink (chan 8)
        p    (pipe src sink)]
    (put! src 1 false)
    (put! src 2 false)
    (put! src 3 false)
    (seal! src)
    @p
    (is (= 2 (take! sink)))
    (is (= 3 (take! sink)))
    (is (= 4 (take! sink)))))
