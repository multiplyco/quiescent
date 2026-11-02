(ns co.multiply.quiescent.bounded-channel-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.channel :refer [buf-count capacity chan if-take! pipe put! seal! sealed? saturation take!]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park timeout-fixture]])
  (:import [java.util NoSuchElementException]))


(allow-platform-park)
(use-fixtures :each (timeout-fixture))


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
      (is @woken "Producer should unpark after slot freed")
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


(deftest take-with-default-test
  (let [ch (chan 4)
        values [nil false ::default :value]]
    (doseq [v values] (put! ch v))
    (seal! ch)
    (is (= values (mapv (fn [_] (take! ch ::default)) values))
      "Buffered values, including one equal to the default, drain normally")
    (doseq [not-found [nil false ::default (Object.)
                      (fn [] (throw (AssertionError. "Default must not be invoked")))
                      (Exception. "Default must not be thrown")]]
      (is (identical? not-found (take! ch not-found))
        "The supplied default is returned as-is on exhaustion"))))


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
    (is (true? @done) "Producer should unpark after take")
    (is (= :second (take! ch)))))


(deftest consumer-parks-when-empty-test
  (let [ch     (chan 4)
        result (promise)]
    (q/task (deliver result (take! ch)))
    (Thread/sleep 10)
    (is (not (realized? result)) "Consumer should be parked")
    (put! ch :value)
    (is (= :value @result))))


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


(deftest mpmc-no-lost-or-double-dispensed-values-test
  (testing "8 producers × 5000 values: every value taken exactly once"
    (let [n-producers 8
          per-thread  5000
          n-consumers 4
          total       (* n-producers per-thread)
          ch          (chan 16)
          results     (java.util.concurrent.ConcurrentLinkedQueue.)]
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
      (let [taken   (vec results)
            freqs   (frequencies taken)
            dupes   (into {} (filter #(> (val %) 1)) freqs)
            missing (remove freqs (range total))]
        (is (= total (count taken))
          "Total taken count must equal total put count")
        (is (empty? dupes)
          (str "Values dispensed more than once: " (take 10 dupes)))
        (is (empty? (seq missing))
          (str "Values never received: " (take 10 missing)))))))


;; # Sealing
;; ################################################################################

(deftest seal-stops-new-puts-test
  (let [ch (chan 4)]
    (is (true? (seal! ch)))
    (is (false? (put! ch :after)))))


(deftest seal-drains-buffered-values-test
  (let [ch (chan 8)
        values [:a nil false :b]]
    (doseq [v values] (put! ch v))
    (seal! ch)
    (is (true? (sealed? ch)) "Sealing does not imply exhaustion")
    (is (= values (mapv (fn [_] (take! ch)) values)))
    (is (thrown? NoSuchElementException (take! ch)))
    (is (thrown? NoSuchElementException (take! ch)) "Exhaustion persists")))


(deftest seal-waits-for-earlier-producer-test
  (let [ch     (chan 1)
        result (promise)
        sealed (promise)]
    (put! ch :fill)
    (q/task (deliver result (put! ch :blocked)))
    (Thread/sleep 10)
    (is (not (realized? result)) "Producer should be parked")
    (q/task (deliver sealed (seal! ch)))
    (Thread/sleep 10)
    (is (not (realized? sealed)) "Seal waits behind the earlier put")
    (is (= :fill (take! ch)))
    (is (true? @result))
    (is (true? @sealed))
    (is (= :blocked (take! ch)))
    (is (thrown? NoSuchElementException (take! ch)))))


(deftest seal-wakes-parked-consumer-test
  (let [ch     (chan 4)
        result (promise)]
    (q/task
      (deliver result
        (try (take! ch)
          (catch NoSuchElementException e e))))
    (Thread/sleep 10)
    (is (not (realized? result)) "Consumer should be parked")
    (seal! ch)
    (is (instance? NoSuchElementException @result))))


(deftest seal-idempotent-test
  (let [ch (chan 4)]
    (is (true? (seal! ch)))
    (is (false? (seal! ch)))))


(deftest seal-query-test
  (let [ch (chan 4)]
    (is (false? (sealed? ch)))
    (seal! ch)
    (is (true? (sealed? ch)))))


(deftest handled-exhaustion-does-not-cancel-task-test
  (let [ch (chan 4)
        _ (seal! ch)
        task (q/task
               [(put! ch :rejected)
                (try (take! ch)
                  (catch NoSuchElementException _ :caught))
                (take! ch ::default)
                (if-take! [v ch] [:unexpected v] :exhausted)
                :continued])]
    (is (= [false :caught ::default :exhausted :continued] @task))
    (is (not (q/cancelled? task)))
    (is (= :success @(q/then task (fn [_] :success))))))


(deftest uncaught-exhaustion-is-task-failure-test
  (let [ch (chan 1)
        _ (seal! ch)
        task (q/task (take! ch))]
    @(q/await task)
    (is (instance? NoSuchElementException (q/get-ex task)))
    (is (not (q/cancelled? task)))
    (is (= :caught @(q/catch task NoSuchElementException (fn [_] :caught))))))


;; # if-take! macro
;; ################################################################################

(deftest if-take!-drains-values-test
  (let [ch (chan 8)]
    (put! ch 1)
    (put! ch 2)
    (put! ch 3)
    (seal! ch)
    (is (= 6 (loop [acc 0]
               (if-take! [v ch]
                 (recur (+ acc v))
                 acc))))))


(deftest if-take!-distinguishes-nil-and-false-from-exhaustion-test
  (let [ch (chan 4)]
    (put! ch nil)
    (put! ch false)
    (seal! ch)
    (is (= [nil false] (loop [values []]
                        (if-take! [v ch]
                          (recur (conj values v))
                          values))))))


(deftest if-take!-returns-else-on-exhaustion-test
  (let [ch (chan 4)]
    (seal! ch)
    (is (= :done (if-take! [v ch] v :done)))))


(deftest if-take!-evaluates-channel-once-and-only-selected-branch-test
  (let [ch (chan 1)
        events (atom [])
        v :outer]
    (put! ch :value)
    (seal! ch)
    (is (= :value
          (if-take! [v (do (swap! events conj :channel) ch)]
            (do (swap! events conj :then) v)
            (do (swap! events conj :else) v))))
    (is (= [:channel :then] @events))
    (reset! events [])
    (is (= :outer
          (if-take! [v (do (swap! events conj :channel) ch)]
            (do (swap! events conj :then) v)
            (do (swap! events conj :else) v)))
      "The take binding is scoped to the then branch")
    (is (= [:channel :else] @events))))


;; # pipe
;; ################################################################################

(deftest pipe-transfers-values-test
  (let [src  (chan 8)
        sink (chan 8)
        p    (pipe src sink)]
    (put! src 1)
    (put! src 2)
    (put! src 3)
    (seal! src)
    @p
    (is (= 1 (take! sink)))
    (is (= 2 (take! sink)))
    (is (= 3 (take! sink)))))


(deftest pipe-propagates-seal-test
  (let [src  (chan 8)
        sink (chan 8)
        p    (pipe src sink)]
    (put! src :a)
    (seal! src)
    @p
    (is (true? (sealed? sink)))
    (is (= :a (take! sink)))
    (is (thrown? NoSuchElementException (take! sink)))
    (is (not (q/cancelled? p)))))


(deftest pipe-no-close-test
  (let [src  (chan 8)
        sink (chan 8)
        p    (pipe src sink false)]
    (put! src :a)
    (seal! src)
    @p
    (is (false? (sealed? sink)))
    (is (= :a (take! sink)))))


(deftest pipe-stops-on-sealed-sink-test
  (doseq [close? [false true]]
    (let [src (chan 4)
          sink (chan 4)]
      (put! src :in-flight)
      (put! src :remaining)
      (seal! sink)
      (let [p (pipe src sink close?)]
        (try
          @p
          (is (not (q/cancelled? p)))
          (is (false? (sealed? src)) "The pipe does not own its source")
          (is (= 1 (buf-count src)) "A rejected put stops further consumption")
          (is (= :remaining (take! src)))
          (is (thrown? NoSuchElementException (take! sink)))
          (finally (seal! src)))))))


(deftest pipe-returns-task-test
  (let [src (chan 4)
        sink (chan 4)
        p   (pipe src sink)]
    (seal! src)
    (is @p "Pipe task should complete")))


;; # Interruption
;; ################################################################################

(deftest put-throws-on-pre-interrupted-thread-test
  (testing "put throws InterruptedException if thread is already interrupted"
    (let [ch     (chan 8)
          result (promise)
          worker (Thread.
                   (fn []
                     (.interrupt (Thread/currentThread))
                     (try
                       (.put ch :v)
                       (deliver result :should-not-reach)
                       (catch InterruptedException _
                         (deliver result :interrupted)))))]
      (.start worker)
      (.join worker 1000)
      (is (= :interrupted @result))
      (is (= 0 (buf-count ch)) "Value should not have been written"))))

(deftest take-throws-on-pre-interrupted-thread-test
  (testing "take throws InterruptedException if thread is already interrupted"
    (let [ch     (chan 8)
          result (promise)
          _      (put! ch :v)
          worker (Thread.
                   (fn []
                     (.interrupt (Thread/currentThread))
                     (try
                       (.take ch)
                       (deliver result :should-not-reach)
                       (catch InterruptedException _
                         (deliver result :interrupted)))))]
      (.start worker)
      (.join worker 1000)
      (is (= :interrupted @result))
      (is (= 1 (buf-count ch)) "Value should still be in buffer"))))


