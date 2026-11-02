(ns co.multiply.quiescent.channel.sealing-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent.channel :as ch]
    [co.multiply.quiescent.test-support :refer [timeout-fixture]])
  (:import [co.multiply.quiescent.impl.channel BoundedChannel RelayLock]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.locks LockSupport]))


(use-fixtures :each (timeout-fixture 20000))


(defn- field
  [klass name]
  (doto (.getDeclaredField ^Class klass name) (.setAccessible true)))


(def ^:private put-lock-field (field BoundedChannel "putLock"))
(def ^:private slot-field (field RelayLock "slot"))
(def ^:private head-field (field BoundedChannel "head"))
(def ^:private tail-field (field BoundedChannel "tail"))


(defn- spawn
  [f]
  (let [result (promise)]
    {:result result
     :thread (Thread/startVirtualThread
               (fn []
                 (deliver result (try (f) (catch Throwable e e)))))}))


(defn- await-result
  [{:keys [result]}]
  (let [v (deref result 5000 ::timeout)]
    (when (= v ::timeout)
      (throw (ex-info "Channel operation timed out" {})))
    (when (instance? Throwable v) (throw v))
    v))


(defn- queued-call
  "Enroll behind a held put lock, verifying queue order without sleeps."
  [channel f]
  (let [lock (.get ^java.lang.reflect.Field put-lock-field channel)
        previous (.get ^java.lang.reflect.Field slot-field lock)
        {:keys [thread result] :as call} (spawn f)
        deadline (+ (System/nanoTime) 3000000000)]
    (loop []
      (when (or (identical? previous (.get ^java.lang.reflect.Field slot-field lock))
              (not= Thread$State/WAITING (.getState ^Thread thread)))
        (when (or (realized? result) (> (System/nanoTime) deadline))
          (throw (ex-info "Operation did not join the held put queue"
                   {:result (deref result 0 ::pending)})))
        (LockSupport/parkNanos 100000)
        (recur)))
    call))


(defn- drain
  [channel]
  (loop [values []]
    (ch/if-take! [v channel]
      (recur (conj values v))
      values)))


(deftest seal-honours-the-producer-queue-boundary
  (doseq [xf? [false true]]
    (testing (str "transducer=" xf?)
      (let [channel (if xf? (ch/chan 1 (map identity)) (ch/chan 1))
            values [:before nil false]
            _ (ch/put! channel :buffered)
            ;; The first put owns the put lock but is blocked on capacity.
            before (mapv #(queued-call channel (fn [] (ch/put! channel %))) values)
            seal (queued-call channel #(ch/seal! channel))
            after (mapv #(queued-call channel (fn [] (ch/put! channel %))) (range 8))
            seals (mapv (fn [_] (queued-call channel #(ch/seal! channel))) (range 3))]
        (is (not (ch/sealed? channel)) "A queued seal has not taken effect")
        (is (not (realized? (:result seal))) "Sealing waits for earlier puts")
        (let [consumer (spawn #(drain channel))]
          (is (= (into [:buffered] values) (await-result consumer)))
          (is (every? true? (mapv await-result before)))
          (is (true? (await-result seal)))
          (is (every? false? (mapv await-result after)) "No put crosses the seal node")
          (is (every? false? (mapv await-result seals)) "Only one seal succeeds")
          (is (ch/sealed? channel))
          (is (= ::end (ch/take! channel ::end))))))))


(deftest seal-waits-for-an-acknowledged-combined-put-to-be-published
  (let [owner-entered (CountDownLatch. 1)
        owner-proceed (CountDownLatch. 1)
        third-entered (CountDownLatch. 1)
        third-proceed (CountDownLatch. 1)
        channel (ch/chan 8
                  (map (fn [v]
                         (case v
                           :owner (do (.countDown owner-entered) (.await owner-proceed))
                           :third (do (.countDown third-entered) (.await third-proceed))
                           nil)
                         v)))
        owner (spawn #(ch/put! channel :owner))]
    (try
      (is (.await owner-entered 3 TimeUnit/SECONDS))
      (let [second (queued-call channel #(ch/put! channel :second))
            third (queued-call channel #(ch/put! channel :third))
            seal (queued-call channel #(ch/seal! channel))
            rejected (queued-call channel #(ch/put! channel :after))]
        (.countDown owner-proceed)
        (is (.await third-entered 3 TimeUnit/SECONDS))
        (is (true? (await-result second)) "The combiner has acknowledged the second put")
        (is (zero? (ch/buf-count channel)) "The acknowledged output is still unflushed")
        (is (not (ch/sealed? channel)) "Sealing cannot overtake the pending publication")
        (is (not (realized? (:result seal))))
        (.countDown third-proceed)
        (is (true? (await-result owner)))
        (is (true? (await-result third)))
        (is (true? (await-result seal)))
        (is (false? (await-result rejected)))
        (is (= [:owner :second :third] (drain channel)))
        (is (= ::end (ch/take! channel ::end))))
      (finally
        (.countDown owner-proceed)
        (.countDown third-proceed)))))


(deftest terminal-sequence-needs-no-buffer-slot-or-reserved-counter-value
  (doseq [start [0 (- Long/MAX_VALUE 1) -2] xf? [false true]]
    (testing (str "sequence=" start " transducer=" xf?)
      (let [channel (if xf? (ch/chan 4 (map identity)) (ch/chan 4))
            values [:a nil false :b]]
        ;; Set an empty channel near wrap before publishing it to another thread.
        (.setLong ^java.lang.reflect.Field head-field channel start)
        (.setLong ^java.lang.reflect.Field tail-field channel start)
        (doseq [v values] (is (ch/put! channel v)))
        (is (= 4 (ch/buf-count channel)))
        (is (true? (await-result (spawn #(ch/seal! channel))))
          "A full buffer does not obstruct sealing once the put lock is free")
        (is (false? (ch/put! channel :after)))
        (is (= values (drain channel)))
        (dotimes [_ 3] (is (= ::end (ch/take! channel ::end))))))))


(deftest reduced-transducer-seals-at-its-current-position
  (let [entered (CountDownLatch. 1)
        proceed (CountDownLatch. 1)
        channel (ch/chan 1
                  (comp (map (fn [v] (.countDown entered) (.await proceed) v))
                    (take 1)))
        owner (spawn #(ch/put! channel :last))]
    (try
      (is (.await entered 3 TimeUnit/SECONDS))
      (let [later (queued-call channel #(ch/put! channel :after))
            seal (queued-call channel #(ch/seal! channel))]
        (.countDown proceed)
        (is (true? (await-result owner)) "Reduced seals without waiting behind itself")
        (is (false? (await-result later)))
        (is (false? (await-result seal)))
        (is (= [:last] (drain channel))))
      (finally (.countDown proceed)))))


(deftest concurrent-empty-observation-and-final-publication
  ;; Exercise the empty / publish / seal boundary without adding synchronization
  ;; to the channel's reads. This is stress coverage, not a forced interleaving.
  (doseq [xf? [false true]]
    (dotimes [_ 500]
      (let [channel (if xf? (ch/chan 1 (map identity)) (ch/chan 1))
            start (CountDownLatch. 1)
            consumer (spawn #(do (.await start) (drain channel)))
            producer (spawn #(do (.await start)
                               (ch/put! channel :last)
                               (ch/seal! channel)))]
        (.countDown start)
        (is (true? (await-result producer)))
        (is (= [:last] (await-result consumer)))
        (is (= ::end (ch/take! channel ::end)))))))
