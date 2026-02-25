(ns co.multiply.quiescent.bounded-channel-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [co.multiply.quiescent.channel :refer [buf-count capacity chan put! saturation take!]])
  (:import
    [co.multiply.quiescent.impl.channel IChannel]))


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
      (Thread/startVirtualThread
        #(do
           (deliver parked true)
           (put! ch :c)
           (deliver woken true)))
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
    (Thread/startVirtualThread
      #(do (deliver started true)
         (put! ch :second)
         (deliver done true)))
    @started
    (Thread/sleep 10)
    (is (not (realized? done)) "Producer should be parked")
    (is (= :first (take! ch)))
    (is (= true (deref done 1000 :timeout)) "Producer should unpark after take")
    (is (= :second (take! ch)))))


(deftest consumer-parks-when-empty-test
  (let [ch     (chan 4)
        result (promise)]
    (Thread/startVirtualThread
      #(deliver result (take! ch)))
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
        results (java.util.concurrent.ConcurrentLinkedQueue.)
        cs      (mapv (fn [_]
                        (Thread/startVirtualThread
                          #(dotimes [_ (quot total n-consumers)]
                             (.add results (take! ch)))))
                  (range n-consumers))
        ps      (mapv (fn [p]
                        (Thread/startVirtualThread
                          #(dotimes [i per-thread]
                             (put! ch (+ (* p per-thread) i)))))
                  (range n-producers))]
    (run! #(.join % 10000) ps)
    (run! #(.join % 10000) cs)
    (is (= total (.size results))
      (str n-producers "P" n-consumers "C: expected " total " values"))))


(deftest mpmc-1p1c-test (mpmc-test 1 1 10000 64))
(deftest mpmc-4p1c-test (mpmc-test 4 1 2500 64))
(deftest mpmc-1p4c-test (mpmc-test 1 4 10000 64))
(deftest mpmc-4p4c-test (mpmc-test 4 4 2500 64))


;; # Padded channels
;; ################################################################################

(deftest padded-channel-round-trip-test
  (let [ch (chan 8 {:padded true})]
    (is (= 8 (capacity ch)))
    (doseq [i (range 5)]
      (put! ch i))
    (is (= [0 1 2 3 4] (mapv (fn [_] (take! ch)) (range 5))))))


(deftest padded-channel-backpressure-test
  (let [ch      (chan 2 {:padded true})
        started (promise)
        done    (promise)]
    (put! ch :a)
    (put! ch :b)
    (Thread/startVirtualThread
      #(do (deliver started true)
         (put! ch :c)
         (deliver done true)))
    @started
    (Thread/sleep 10)
    (is (not (realized? done)) "Producer should be parked")
    (is (= :a (take! ch)))
    (is (= true (deref done 1000 :timeout)))
    (is (= :b (take! ch)))
    (is (= :c (take! ch)))))


;; # Lifecycle stubs
;; ################################################################################

(deftest lifecycle-stubs-test
  (let [ch (chan 4)]
    (testing "cancel throws"
      (is (thrown? UnsupportedOperationException
            (IChannel/.cancel ch "msg"))))
    (testing "seal throws"
      (is (thrown? UnsupportedOperationException
            (IChannel/.seal ch))))
    (testing "isCancelled returns false"
      (is (false? (IChannel/.isCancelled ch))))
    (testing "isSealed returns false"
      (is (false? (IChannel/.isSealed ch))))))
