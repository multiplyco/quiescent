(ns co.multiply.quiescent.channel
  "Channels — multi-value coordination primitives.

   Channels are bounded ring buffers for passing values between
   virtual threads. `put!` writes a value, `take!` reads one. Both
   park the virtual thread when the buffer is full or empty.

   `seal!` waits its turn in the producer queue, ends production, and allows
   buffered values to drain.
   Channels do not participate in task cancellation. To seal a channel
   when an owning task settles, explicitly attach a `q/finally` handler:

   (q/finally task (fn [_ _ _] (seal! ch)))"
  (:require
    [co.multiply.quiescent.impl :refer [do-runner]]
    [co.multiply.quiescent.impl.executor :refer [delegate-virtual]])
  (:import
    [co.multiply.quiescent.impl.channel BoundedChannel IBuffered IChannel]
    [java.util NoSuchElementException]))


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

   Returns true if accepted, false if the channel is sealed.
   Rejection does not cancel the calling task."
  [ch v]
  (IChannel/.put ch v))


(defn take!
  "Take a value from the channel. Parks while the channel is open and empty.
   Drains buffered values after sealing. Once sealed and drained, throws
   NoSuchElementException, or returns `not-found` when supplied.
   `nil` and `false` are valid channel values and valid defaults.

   `not-found` is an eagerly evaluated value, returned as-is on exhaustion.
   Use `if-take!` for branching without a caller-supplied exhaustion value.
   An uncaught exception follows normal task failure propagation."
  ([ch]
   (let [v (IChannel/.take ch)]
     (if (identical? v IChannel/SEALED)
       (throw (NoSuchElementException. "Channel is sealed and drained."))
       v)))
  ([ch not-found]
   (let [v (IChannel/.take ch)]
     (if (identical? v IChannel/SEALED)
       not-found
       v))))


(defmacro if-take!
  "Take from channel with exhaustion handling. On a successful take, binds
   the value and evaluates the then branch, including for nil or false.
   If the channel is sealed and drained, evaluates the else branch
   (binding is not in scope). Only the selected branch is evaluated.
   Parks while the channel is open and empty. Does not cancel the calling task.

   (if-take! [v ch]
     (recur (+ acc v))
     acc)"
  [[sym ch] then else]
  `(let [v# (IChannel/.take ~ch)]
     (if (identical? v# IChannel/SEALED)
       ~else
       (let [~sym v#]
         ~then))))


;; # Lifecycle
;; ################################################################################
(defn seal!
  "Seal the channel in producer queue order. Puts ahead of the seal finish;
   puts behind it return false. May block waiting for consumers to make room
   for earlier puts. Sealing itself needs no spare buffer capacity.
   Buffered values drain normally; waiting takes consume them or report
   exhaustion once the final producer sequence has been reached.
   Returns true if this call performed the seal. Does not cancel tasks."
  [ch]
  (IChannel/.seal ch))


(defn sealed?
  "True if the channel has been sealed, whether or not it has drained."
  [ch]
  (IChannel/.isSealed ch))


;; # Composition
;; ################################################################################
(defn pipe
  "Transfer values from source to sink. Spawns a virtual thread task
   that drains source via if-take! and forwards each value with put!.

   When `close?` is true (default), seals sink when source is sealed and
   drained. When false, leaves sink open on source exhaustion.

   If sink rejects a put, stops normally without sealing source. The value
   already taken from source is not restored. Task cancellation does not
   seal either channel; attach `q/finally` for explicit lifecycle wiring.

   Returns the pipe task. Deref to wait for completion."
  ([source sink]
   (pipe source sink true))
  ([source sink close?]
   (do-runner delegate-virtual
     (loop []
       (if-take! [v source]
         (when (put! sink v) (recur))
         (when close?
           (seal! sink)))))))


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
