(ns co.multiply.quiescent.channel
  "Channels — multi-value coordination primitives.

   Channels are bounded ring buffers for passing values between
   virtual threads. `put` writes a value, `take` reads one. Both
   park the virtual thread when the buffer is full or empty."
  (:require
    [co.multiply.quiescent.impl :as impl :refer [do-runner]]
    [co.multiply.quiescent.impl.executor :refer [delegate-virtual]]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [ask]])
  (:import
    [co.multiply.quiescent.impl.channel BoundedChannel IBuffered IChannel]
    [java.util.concurrent CancellationException]))


(def n-cpus
  "Number of available processors. Useful as a buffer size heuristic
   when sizing channels to concurrency rather than throughput."
  (.availableProcessors (Runtime/getRuntime)))


;; # Construction
;; ################################################################################
(defn chan
  "Create a bounded channel with the given buffer size.

   Buffer size is rounded up to the next power of 2. The channel
   supports multiple concurrent producers and consumers.

   (chan 8)          ; plain buffer
   (chan 16 (map inc)) ; with transducer"
  ([n]
   (BoundedChannel. (int n)))
  ([n xf]
   (BoundedChannel. (int n) xf)))


;; # Core operations
;; ################################################################################
(defn put!
  "Put a value on the channel. Parks if the buffer is full.

   If the channel is sealed/cancelled, cancels the current task (if any)
   and throws CancellationException. Pass `false` to suppress cancellation
   and return false instead."
  ([ch v]
   (put! ch v true))
  ([ch v cancel?]
   (let [ok (IChannel/.put ch v)]
     (if (and (not ok) cancel?)
       (do
         (when-let [this (ask impl/*this* nil)]
           (call/doCancel this "Channel sealed."))
         (throw (CancellationException. "Channel sealed.")))
       ok))))


(defn take!
  "Take a value from the channel. Parks if the buffer is empty.
   Returns the value. If the channel is cancelled or sealed+drained,
   cancels the current task (if any) and throws CancellationException."
  [ch]
  (let [v (IChannel/.take ch)]
    (if (identical? v IChannel/CANCELLED)
      (do
        (when-let [this (ask impl/*this* nil)]
          (call/doCancel this "Channel cancelled."))
        (throw (CancellationException. "Channel cancelled.")))
      v)))


(defmacro poll
  "Take from channel with exhaustion handling. If a value is available,
   binds it and evaluates the then branch. If the channel is cancelled
   or sealed+drained, evaluates the else branch (binding is not in scope).

   (poll [v ch]
     (recur (+ acc v))
     acc)"
  [[sym ch] then else]
  `(let [v# (IChannel/.take ~ch)]
     (if (identical? v# IChannel/CANCELLED)
       ~else
       (let [~sym v#]
         ~then))))


;; # Lifecycle
;; ################################################################################
(defn cancel!
  "Cancel the channel. Discards buffered values, wakes all parked
   threads. Returns true if this call performed the cancellation."
  ([ch] (IChannel/.cancel ch nil))
  ([ch msg] (IChannel/.cancel ch msg)))


(defn seal!
  "Seal the channel. No more puts accepted; existing values drain
   normally. Returns true if this call performed the seal."
  [ch]
  (IChannel/.seal ch))


(defn cancelled?
  "True if the channel has been cancelled."
  [ch]
  (IChannel/.isCancelled ch))


(defn sealed?
  "True if the channel has been sealed (includes cancelled channels)."
  [ch]
  (IChannel/.isSealed ch))


;; # Composition
;; ################################################################################
(defn pipe
  "Transfer values from source to sink. Spawns a virtual thread task
   that drains source via poll and forwards each value with put!.

   When `close?` is true (default), propagates lifecycle: seals sink
   when source is sealed+drained, cancels sink when source is cancelled.
   When false, the pipe completes without affecting sink lifecycle.

   Returns the pipe task. Deref to wait for completion."
  ([source sink]
   (pipe source sink true))
  ([source sink close?]
   (do-runner delegate-virtual
     (loop []
       (poll [v source]
         (do (put! sink v) (recur))
         (when close?
           (if (cancelled? source)
             (cancel! sink)
             (seal! sink))))))))


;; # Query
;; ################################################################################
(defn capacity
  "Buffer capacity (always a power of 2)."
  [ch]
  (IBuffered/.capacity ch))


(defn buf-count
  "Approximate number of values currently in the buffer."
  [ch]
  (IBuffered/.count ch))


(defn saturation
  "Buffer saturation from 0.0 (empty) to 1.0 (full)."
  [ch]
  (IBuffered/.saturation ch))
