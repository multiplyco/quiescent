(ns co.multiply.quiescent.channel.handoff-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
    [co.multiply.quiescent.channel :as ch]
    [co.multiply.quiescent.test-support :refer [timeout-fixture]])
  (:import [co.multiply.quiescent.impl.channel BoundedChannel RelayLock RelayLock$Node]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.locks LockSupport]))


(use-fixtures :each (timeout-fixture 20000))


(defn- field
  [klass name]
  (doto (.getDeclaredField ^Class klass name) (.setAccessible true)))


(def ^:private put-lock-field (field BoundedChannel "putLock"))
(def ^:private owner-field (field RelayLock "owner"))
(def ^:private state-field (field RelayLock$Node "state"))
(def ^:private value-field (field RelayLock$Node "value"))
(def ^:private combined-field (field RelayLock$Node "combined"))


(defn- spawn
  [workers f]
  (let [result (promise)
        thread (Thread/startVirtualThread
                 (fn [] (deliver result (try (f) (catch Throwable e e)))))]
    (swap! workers conj thread)
    {:thread thread :result result}))


(defn- await-result
  [{:keys [result]}]
  (let [v (deref result 5000 ::timeout)]
    (when (= v ::timeout) (throw (ex-info "Channel operation timed out" {})))
    (when (instance? Throwable v) (throw v))
    v))


(defn- await-latch!
  [^CountDownLatch latch]
  (when-not (.await latch 5 TimeUnit/SECONDS)
    (throw (ex-info "Handoff checkpoint timed out" {}))))


(defn- await-condition!
  [pred]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (> (System/nanoTime) deadline)
          (throw (ex-info "Handoff condition timed out" {})))
        (LockSupport/parkNanos 100000)
        (recur)))))


(defn- parked-on?
  [^Thread thread blocker]
  (and (= Thread$State/WAITING (.getState thread))
    (identical? blocker (LockSupport/getBlocker thread))))


(defn- interrupt-parked!
  [^Thread thread lock]
  (await-condition! #(parked-on? thread lock))
  (.interrupt thread)
  ;; RelayLock records and clears the interruption, then parks again until
  ;; capacity is available. Do not free capacity before it has recorded it.
  (await-condition! #(and (parked-on? thread lock) (not (.isInterrupted thread)))))


(defn- put-result
  [channel value]
  {:accepted (ch/put! channel value)
   :interrupted (.isInterrupted (Thread/currentThread))})


(deftest outgoing-producer-keeps-its-own-interruption-result
  (doseq [interrupt-owner? [true false]]
    (testing (str "interrupted owner=" interrupt-owner?)
      (let [released (CountDownLatch. 1)
            finish-owner (CountDownLatch. 1)
            workers (atom [])
            channel (ch/chan 2)
            ^RelayLock lock (.get ^java.lang.reflect.Field put-lock-field channel)]
        (try
          (ch/put! channel :fill-1)
          (ch/put! channel :fill-2)
          (let [owner (spawn workers #(put-result channel :owner))]
            ;; Full-buffer backpressure holds the owner until its successor
            ;; is linked. No transducer or production test hook is needed.
            (await-condition! #(parked-on? (:thread owner) lock))
            (let [owner-node (.get ^java.lang.reflect.Field owner-field lock)
                  node-ready (promise)
                  combined (spawn workers
                             (fn []
                               ;; A normal queued payload node, with a test-only
                               ;; release override. The combiner releases this
                               ;; last node, then pauses after the real handoff.
                               (let [node (proxy [RelayLock$Node] [(Thread/currentThread)]
                                            (release
                                              []
                                              (proxy-super release)
                                              (.countDown released)
                                              (.await finish-owner)))]
                                 (.set ^java.lang.reflect.Field value-field node :combined)
                                 (deliver node-ready node)
                                 (.acquire lock node)
                                 (when-not (.getBoolean ^java.lang.reflect.Field combined-field node)
                                   (.release node)
                                   (throw (ex-info "Expected the queued put to be combined" {})))
                                 true)))
                  node (deref node-ready 5000 ::timeout)]
              (await-condition! #(identical? node (.get ^java.lang.reflect.Field state-field owner-node)))
              (when interrupt-owner?
                (interrupt-parked! (:thread owner) lock))
              (is (= :fill-1 (ch/take! channel)))
              (is (= :fill-2 (ch/take! channel)))
              (await-latch! released)
              (is (true? (await-result combined)))
              (is (not (realized? (:result owner))) "Owner has handed off but not returned")
              ;; Both queued outputs fill the buffer. The next producer owns
              ;; the scratch fields while the former owner is paused.
              (let [successor (spawn workers #(put-result channel :successor))]
                (await-condition! #(parked-on? (:thread successor) lock))
                (when-not interrupt-owner?
                  (interrupt-parked! (:thread successor) lock))
                (is (= :owner (ch/take! channel)))
                (is (= {:accepted true :interrupted (not interrupt-owner?)}
                       (await-result successor)))
                (.countDown finish-owner)
                (is (= {:accepted true :interrupted interrupt-owner?}
                       (await-result owner))
                  "A successor must neither erase nor supply the former owner's interruption")
                (is (= [:combined :successor] [(ch/take! channel) (ch/take! channel)])))))
          (finally
            (.countDown finish-owner)
            ;; Also unblock workers when a checkpoint/assertion fails.
            (spawn workers #(loop [] (ch/if-take! [_ channel] (recur) nil)))
            (spawn workers #(ch/seal! channel))
            (doseq [^Thread thread @workers] (.join thread 1000))))))))
