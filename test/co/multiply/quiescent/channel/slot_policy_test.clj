(ns co.multiply.quiescent.channel.slot-policy-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent.test-support :refer [timeout-fixture]])
  (:import [co.multiply.quiescent.impl.channel Alt BoundedChannel IChannel RelayLock]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.locks LockSupport]))


(use-fixtures :each (timeout-fixture 10000))


(defn- field
  [klass name]
  (doto (.getDeclaredField ^Class klass name) (.setAccessible true)))


(def ^:private buffer-field (field BoundedChannel "buffer"))
(def ^:private take-lock-field (field BoundedChannel "takeLock"))
(def ^:private slot-field (field RelayLock "slot"))


(def policies
  [{:label "plain default" :make #(BoundedChannel. 8) :clear? true}
   {:label "transducer default" :make #(BoundedChannel. 8 (map identity)) :clear? true}])


(defn- slots
  [ch]
  (vec (.get ^java.lang.reflect.Field buffer-field ch)))


(defn- expect-slots
  [ch values clear?]
  (is (= (if clear? (vec (repeat (count values) nil)) values)
         (subvec (slots ch) 0 (count values)))))


(defn- await-enqueued!
  [^RelayLock lock previous ^Thread thread]
  (let [deadline (+ (System/nanoTime) 2000000000)]
    (loop []
      (when (or (identical? previous (.get ^java.lang.reflect.Field slot-field lock))
              (not= Thread$State/WAITING (.getState thread)))
        (when (> (System/nanoTime) deadline)
          (throw (ex-info "Taker did not join the blocked lock chain" {})))
        (LockSupport/parkNanos 100000)
        (recur)))))


(deftest plain-take-slot-policy
  (doseq [{:keys [label make clear?]} policies]
    (testing label
      (let [^BoundedChannel ch (make)
            values (vec (repeatedly 8 #(Object.)))]
        (try
          (doseq [v values] (.put ch v))
          (is (= values (mapv (fn [_] (.take ch)) values)))
          (expect-slots ch values clear?)
          (is (zero? (.count ch)))
          ;; Wrapping and exhausting the ring must not redispatch retained values.
          (dotimes [i 16] (.put ch i) (is (= i (.take ch))))
          (.seal ch)
          (is (identical? IChannel/CANCELLED (.take ch)))
          (finally (.cancel ch nil)))))))


(deftest alt-take-slot-policy
  (doseq [{:keys [label make clear?]} policies]
    (testing label
      (let [^BoundedChannel ch (make)
            a (Alt. false false (into-array IChannel [ch]))
            value (Object.)]
        (try
          (.put ch value)
          (is (identical? value (.take a)))
          (expect-slots ch [value] clear?)
          (is (zero? (.count ch)))
          (finally (.cancel ch nil)))))))


(deftest queued-takers-slot-policy
  (doseq [{:keys [label make clear?]} policies]
    (testing label
      (let [^BoundedChannel ch (make)
            ^RelayLock lock (.get ^java.lang.reflect.Field take-lock-field ch)
            blocker (.acquire lock)
            released? (atom false)
            values (vec (repeatedly 8 #(Object.)))
            results (object-array 8)
            done (CountDownLatch. 8)]
        (try
          ;; Fill before releasing an ordered chain of takers. This exercises
          ;; owner and combined reads without relying on scheduler arrival order.
          (doseq [v values] (.put ch v))
          (dotimes [i 8]
            (let [previous (.get ^java.lang.reflect.Field slot-field lock)
                  t (Thread/startVirtualThread
                      (fn []
                        (try (aset results i (.take ch))
                          (catch Throwable e (aset results i e))
                          (finally (.countDown done)))))]
              (await-enqueued! lock previous t)))
          (.release blocker)
          (reset! released? true)
          (is (.await done 5 TimeUnit/SECONDS))
          (is (= values (vec results)) "Queued takers preserve FIFO order")
          (expect-slots ch values clear?)
          (is (zero? (.count ch)))
          (finally
            (when-not @released? (.release blocker))
            (.cancel ch nil)
            (.await done 1 TimeUnit/SECONDS)))))))
