(ns co.multiply.quiescent.bounded-channel-xf-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.channel :refer [buf-count capacity chan put! saturation take!]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park]])
  (:import
    [co.multiply.quiescent.impl.channel IChannel]))


(allow-platform-park)


;; # map
;; ################################################################################

(deftest map-xform-test
  (let [ch (chan 16 (map inc))]
    (put! ch 1)
    (put! ch 2)
    (put! ch 3)
    (is (= 2 (take! ch)))
    (is (= 3 (take! ch)))
    (is (= 4 (take! ch)))))


;; # filter
;; ################################################################################

(deftest filter-xform-test
  (let [ch   (chan 16 (filter even?))
        done (promise)]
    ;; Consumer waiting for one value
    (q/task (deliver done (take! ch)))
    ;; Odd values are filtered out
    (is (true? (put! ch 1)) "put! returns true even for filtered values")
    (is (true? (put! ch 3)))
    ;; Even value passes through
    (put! ch 4)
    (is (= 4 (deref done 1000 :timeout)))))


(deftest filter-does-not-buffer-filtered-values-test
  (let [ch (chan 16 (filter even?))]
    (put! ch 1)
    (put! ch 3)
    (put! ch 5)
    (is (= 0 (buf-count ch)) "Filtered values should not be in the buffer")
    (put! ch 2)
    (is (= 1 (buf-count ch)))))


;; # mapcat
;; ################################################################################

(deftest mapcat-xform-test
  (let [ch (chan 16 (mapcat range))]
    (put! ch 3)
    (is (= 0 (take! ch)))
    (is (= 1 (take! ch)))
    (is (= 2 (take! ch)))))


(deftest mapcat-backpressure-test
  (testing "mapcat that expands beyond buffer size parks mid-batch"
    (let [ch   (chan 2 (mapcat #(repeat 4 %)))
          done (promise)]
      ;; Consumer drains all 4 values
      (q/task (deliver done (mapv (fn [_] (take! ch)) (range 4))))
      ;; Single put produces 4 values into a buffer of 2
      (put! ch :x)
      (is (= [:x :x :x :x] (deref done 1000 :timeout))))))


;; # partition-all (stateful)
;; ################################################################################

(deftest partition-all-xform-test
  (let [ch (chan 16 (partition-all 2))]
    (put! ch 1)
    (put! ch 2)
    (put! ch 3)
    (put! ch 4)
    (is (= [1 2] (take! ch)))
    (is (= [3 4] (take! ch)))))


;; # take (reduced)
;; ################################################################################

(deftest take-xform-test
  (let [ch (chan 16 (take 3))]
    (put! ch :a)
    (put! ch :b)
    ;; 3rd put writes value then throws (seal stub)
    (is (thrown? UnsupportedOperationException (put! ch :c)))
    ;; All 3 values are in the buffer
    (is (= :a (take! ch)))
    (is (= :b (take! ch)))
    (is (= :c (take! ch)))))


(deftest transducer-completion-flush-test
  (testing "Stateful transducers flush remaining items on reduced state"
    (let [xf (comp (map identity)
                   (take 3)
                   (partition-all 2))
          ch (chan 16 xf)]
      (put! ch 1)
      (put! ch 2)
      ;; 3rd put triggers `Reduced`, which invokes `rf.invoke(this)`
      ;; `partition-all` should flush its buffered `[3]` before the seal stub throws.
      (is (thrown? UnsupportedOperationException (put! ch 3)))
      (is (= [1 2] (take! ch)))
      (is (= [3] (take! ch))))))


;; # MPMC with xform
;; ################################################################################

(deftest mpmc-xform-test
  (testing "Multiple producers with map xform — all values transformed"
    (let [n       10000
          ch      (chan 64 (map inc))
          results (java.util.concurrent.ConcurrentLinkedQueue.)]
      @(q/task
         (let [c  (q/task (dotimes [_ n] (.add results (take! ch))))
               ps (mapv (fn [_]
                          (q/task (dotimes [i (quot n 4)] (put! ch i))))
                    (range 4))]
           (run! deref ps)
           @c))
      (is (= n (.size results)))
      ;; Every value should be incremented (no raw values)
      (is (every? pos-int? (iterator-seq (.iterator results)))))))


;; # Padded xf channels
;; ################################################################################

(deftest padded-xf-round-trip-test
  (let [ch (chan 8 {:xf (map inc) :padded true})]
    (is (= 8 (capacity ch)))
    (put! ch 1)
    (put! ch 2)
    (put! ch 3)
    (is (= 2 (take! ch)))
    (is (= 3 (take! ch)))
    (is (= 4 (take! ch)))))


(deftest padded-xf-filter-test
  (let [ch   (chan 16 {:xf (filter even?) :padded true})
        done (promise)]
    (q/task (deliver done (take! ch)))
    (put! ch 1)
    (put! ch 3)
    (put! ch 4)
    (is (= 4 (deref done 1000 :timeout)))))


;; # Construction and query
;; ################################################################################

(deftest xf-channel-capacity-test
  (is (= 16 (capacity (chan 16 (map identity)))))
  (is (= 8 (capacity (chan 5 (map identity))))))


(deftest xf-channel-buf-count-test
  (let [ch (chan 8 (map inc))]
    (is (= 0 (buf-count ch)))
    (put! ch 1)
    (is (= 1 (buf-count ch)))
    (is (= 2 (take! ch)))
    (is (= 0 (buf-count ch)))))


(deftest xf-channel-saturation-test
  (let [ch (chan 4 (map identity))]
    (is (= 0.0 (saturation ch)))
    (dotimes [i 4] (put! ch i))
    (is (= 1.0 (saturation ch)))))


;; # Cancellation and sealing
;; ################################################################################

(deftest cancel-xf-channel-test
  (let [ch (chan 16 (map inc))]
    (put! ch 1)
    (put! ch 2)
    (IChannel/.cancel ch nil)
    (is (false? (put! ch 3)))
    (is (identical? IChannel/CANCELLED (take! ch)))))


(deftest seal-xf-drains-test
  (let [ch (chan 16 (map inc))]
    (put! ch 1)
    (put! ch 2)
    (IChannel/.seal ch)
    (is (false? (put! ch 3)))
    (is (= 2 (take! ch)))
    (is (= 3 (take! ch)))
    (is (identical? IChannel/CANCELLED (take! ch)))))
