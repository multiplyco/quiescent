(ns quiescent.allocation
  "JDK 21+ whole-JVM allocated-byte accounting, including carrier execution."
  (:refer-clojure :exclude [bean])
  (:import [com.sun.management ThreadMXBean]
    [java.lang.management ManagementFactory]
    [java.util.concurrent CountDownLatch TimeUnit]))


(defn bean
  []
  (let [b (ManagementFactory/getThreadMXBean)]
    (when (and (instance? ThreadMXBean b) (.isThreadAllocatedMemorySupported ^ThreadMXBean b))
      (.setThreadAllocatedMemoryEnabled ^ThreadMXBean b true)
      (try
        (when (<= 0 (.getTotalThreadAllocatedBytes ^ThreadMXBean b)) b)
        (catch UnsupportedOperationException _ nil)))))


(defn total
  [^ThreadMXBean b]
  (when b
    (let [n (.getTotalThreadAllocatedBytes b)]
      (when (neg? n) (throw (ex-info "Whole-JVM allocation counter became unavailable" {})))
      n)))


(defn probe!
  "Verify the actual runtime counts escaping allocations from a virtual thread,
   including after termination. Read-pair controls are retained, not subtracted."
  [b]
  (let [sink (object-array 32)
        ready (CountDownLatch. 1)
        gate (CountDownLatch. 1)
        worker (Thread/startVirtualThread
                 (fn []
                   (.countDown ready)
                   (.await gate)
                   (dotimes [i 32] (aset sink i (byte-array 32768)))))
        _ (when-not (.await ready 10 TimeUnit/SECONDS)
            (throw (ex-info "Allocation probe did not start" {})))
        before (total b)
        _ (.countDown gate)
        _ (.join worker 10000)
        after (total b)
        bytes (- after before)
        controls (mapv (fn [_] (let [a (total b) z (total b)] (- z a))) (range 5))]
    (when (or (.isAlive worker) (some nil? (vec sink)) (< bytes (* 32 32768)))
      (throw (ex-info "Runtime did not account for virtual-thread allocations" {:bytes bytes})))
    {:status :passed :virtual-thread-terminated? (not (.isAlive worker))
     :expected-payload-bytes (* 32 32768) :observed-bytes bytes
     :read-pair-control-bytes controls :control-subtracted? false}))
