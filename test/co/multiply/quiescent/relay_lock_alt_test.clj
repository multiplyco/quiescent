(ns co.multiply.quiescent.relay-lock-alt-test
  (:require
    [clojure.test :refer [deftest is testing]])
  (:import
    [co.multiply.quiescent.impl.channel
     RelayLock RelayLock$Node RelayLock$AltNode
     Signal ChannelRef IChannel]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.atomic AtomicLong AtomicBoolean AtomicReference]))


(defn- start-virtual-thread
  ^Thread [^Runnable f]
  (.start (Thread/ofVirtual) f))


;; Minimal IChannel stub for claim testing
(defn- stub-channel ^IChannel []
  (reify IChannel
    (take [_] nil)
    (put [_ _] false)
    (altTake [_ _] nil)
    (altPut [_ _ _] false)
    (cancel [_ _] false)
    (seal [_] false)
    (isCancelled [_] false)
    (isSealed [_] false)))


;; ================================================================
;;  AltNode in lock chain — claim succeeds
;; ================================================================

(deftest alt-acquire-uncontended-test
  (testing "AltNode acquires uncontended lock"
    (let [lock    (RelayLock.)
          ref     (ChannelRef.)
          ch      (stub-channel)
          alt     (RelayLock$AltNode. (Thread/currentThread) ref)
          node    (.acquireAlt lock alt)]
      (is (identical? alt node))
      (.release lock node ch)
      (is true "Completed without error"))))


(deftest alt-then-regular-handoff-test
  (testing "AltNode holds lock, regular thread waits, handoff works"
    (let [lock    (RelayLock.)
          ref     (ChannelRef.)
          ch      (stub-channel)
          alt     (RelayLock$AltNode. (Thread/currentThread) ref)
          _       (.acquireAlt lock alt)
          result  (AtomicBoolean. false)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (let [node (.acquire lock)]
            (try
              (.set result true)
              (finally
                (.release lock node))))
          (.countDown done)))
      (Thread/sleep 10)
      ;; Release with channel — successor is a regular Thread
      (.release lock alt ch)
      (is (.await done 5 TimeUnit/SECONDS))
      (is (.get result)))))


(deftest regular-then-alt-handoff-claim-succeeds-test
  (testing "Regular thread holds lock, AltNode successor, claim succeeds on release"
    (let [lock    (RelayLock.)
          ref     (ChannelRef.)
          ch      (stub-channel)
          node    (.acquire lock)
          result  (AtomicBoolean. false)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (let [alt  (RelayLock$AltNode. (Thread/currentThread) ref)
                node (.acquireAlt lock alt)]
            (try
              (.set result true)
              (finally
                (.release lock node ch))))
          (.countDown done)))
      (Thread/sleep 10)
      ;; Release with channel — successor is an AltNode, claim should succeed
      (.release lock node ch)
      (is (.await done 5 TimeUnit/SECONDS))
      (is (.get result))
      (is (identical? ch (.get ref))
        "ChannelRef should be claimed by ch"))))


;; ================================================================
;;  AltNode in lock chain — claim fails (dead alt skipping)
;; ================================================================

(deftest dead-alt-skipped-to-regular-thread-test
  (testing "Dead AltNode in chain is skipped, regular thread behind it gets woken"
    (let [lock    (RelayLock.)
          ref     (ChannelRef.)
          ch1     (stub-channel)
          ch2     (stub-channel)
          ;; Pre-claim the ref so the alt is dead
          _       (.claim ref ch1)
          node    (.acquire lock)
          regular-done (CountDownLatch. 1)
          regular-ran  (AtomicBoolean. false)]
      ;; Alt thread — acquires lock, holds it (simulating visiting
      ;; other channels). The alt thread is "elsewhere" — not parked
      ;; in this lock chain.
      (start-virtual-thread
        (fn []
          (let [alt  (RelayLock$AltNode. (Thread/currentThread) ref)
                _    (.acquireAlt lock alt)]
            ;; Simulate alt holding lock while visiting other channels.
            ;; In real usage, the alt thread would eventually release
            ;; or park on a signal. Here we just hold indefinitely —
            ;; dispatch will skip this dead alt and hand off to the
            ;; regular thread behind it.
            (Thread/sleep 60000))))
      (Thread/sleep 10)
      ;; Regular thread behind the alt
      (start-virtual-thread
        (fn []
          (let [node (.acquire lock)]
            (try
              (.set regular-ran true)
              (finally
                (.release lock node))))
          (.countDown regular-done)))
      (Thread/sleep 10)
      ;; Release — successor is the dead AltNode. dispatch should
      ;; skip it (claim fails), write DONE to altNode.state, and
      ;; wake the regular thread directly.
      (.release lock node ch2)
      (is (.await regular-done 5 TimeUnit/SECONDS)
        "Regular thread should complete")
      (is (.get regular-ran)
        "Regular thread behind dead alt should have run"))))


(deftest chained-dead-alts-skipped-test
  (testing "Multiple dead AltNodes in chain are all skipped"
    (let [lock    (RelayLock.)
          ref1    (ChannelRef.)
          ref2    (ChannelRef.)
          ch      (stub-channel)
          ;; Pre-claim both refs
          _       (.claim ref1 ch)
          _       (.claim ref2 ch)
          node    (.acquire lock)
          regular-done (CountDownLatch. 1)
          regular-ran  (AtomicBoolean. false)]
      ;; Two dead alt threads — hold lock, simulate visiting other channels
      (start-virtual-thread
        (fn []
          (let [alt  (RelayLock$AltNode. (Thread/currentThread) ref1)
                _    (.acquireAlt lock alt)]
            (Thread/sleep 60000))))
      (Thread/sleep 10)
      (start-virtual-thread
        (fn []
          (let [alt  (RelayLock$AltNode. (Thread/currentThread) ref2)
                _    (.acquireAlt lock alt)]
            (Thread/sleep 60000))))
      (Thread/sleep 10)
      ;; Regular thread at the end
      (start-virtual-thread
        (fn []
          (let [node (.acquire lock)]
            (try
              (.set regular-ran true)
              (finally
                (.release lock node))))
          (.countDown regular-done)))
      (Thread/sleep 10)
      ;; Release — should skip both dead alts, reach regular thread
      (.release lock node ch)
      (is (.await regular-done 10 TimeUnit/SECONDS)
        "Regular thread should complete")
      (is (.get regular-ran)
        "Regular thread at end of chain should have run"))))


;; ================================================================
;;  Signal with AltNode
;; ================================================================

(deftest signal-wakes-alt-claim-succeeds-test
  (testing "Signal with AltNode — claim succeeds, alt thread woken"
    (let [sig     (Signal.)
          ref     (ChannelRef.)
          ch      (stub-channel)
          result  (AtomicBoolean. false)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (let [alt (RelayLock$AltNode. (Thread/currentThread) ref)]
            (.await sig alt)
            (.set result true))
          (.countDown done)))
      (Thread/sleep 10)
      (.signal sig ch)
      (is (.await done 5 TimeUnit/SECONDS))
      (is (.get result)
        "Alt thread should have been woken")
      (is (identical? ch (.get ref))
        "Ref should be claimed by ch"))))


(deftest signal-skips-dead-alt-wakes-thread-test
  (testing "Signal with dead AltNode — skips to lock chain successor"
    (let [lock    (RelayLock.)
          sig     (Signal.)
          ref     (ChannelRef.)
          ch1     (stub-channel)
          ch2     (stub-channel)
          ;; Pre-claim ref
          _       (.claim ref ch1)
          result  (AtomicBoolean. false)
          alt-parked (CountDownLatch. 1)
          successor-done (CountDownLatch. 1)]
      ;; Alt thread: acquires lock, awaits on signal (parks)
      (start-virtual-thread
        (fn []
          (let [alt-node (RelayLock$AltNode. (Thread/currentThread) ref)
                _        (.acquireAlt lock alt-node)]
            ;; Alt holds the lock, buffer "empty", await on signal
            (.countDown alt-parked)
            (.await sig alt-node)
            ;; Alt wakes (from signal skip writing DONE, or spurious).
            ;; Release the lock — may be redundant if signal already
            ;; did the DONE handoff, but idempotent.
            (.release lock alt-node ch2))))
      (.await alt-parked 5 TimeUnit/SECONDS)
      (Thread/sleep 10)
      ;; A successor arrives in the lock chain behind the alt
      (start-virtual-thread
        (fn []
          (let [node (.acquire lock)]
            (try
              (.set result true)
              (finally
                (.release lock node))))
          (.countDown successor-done)))
      (Thread/sleep 10)
      ;; Signal from producer side — should skip dead alt,
      ;; write DONE to altNode.state (waking successor).
      (.signal sig ch2)
      (is (.await successor-done 5 TimeUnit/SECONDS)
        "Successor should be woken after dead alt skipped")
      (is (.get result)
        "Successor should have acquired lock and run"))))


(deftest signal-no-alt-still-works-test
  (testing "Signal with regular thread still works after Alt support added"
    (let [sig     (Signal.)
          result  (AtomicBoolean. false)
          done    (CountDownLatch. 1)]
      (start-virtual-thread
        (fn []
          (.await sig)
          (.set result true)
          (.countDown done)))
      (Thread/sleep 10)
      (.signal sig (stub-channel))
      (is (.await done 5 TimeUnit/SECONDS))
      (is (.get result)))))
