(ns co.multiply.quiescent.channel
  "Channels — multi-value coordination primitives.

   Channels are bounded ring buffers for passing values between
   virtual threads. `put` writes a value, `take` reads one. Both
   park the virtual thread when the buffer is full or empty."
  (:import
    [co.multiply.quiescent.impl.channel BoundedChannel BoundedChannelXf IBuffered IChannel]))


;; # Construction
;; ################################################################################
(defn chan
  "Create a bounded channel with the given buffer size.

   Buffer size is rounded up to the next power of 2. The channel
   supports multiple concurrent producers and consumers.

   Second arg is either a transducer or an opts map:

   (chan 8)                              ; plain buffer
   (chan 16 (map inc))                   ; with transducer
   (chan 16 {:padded true})              ; cache-line padded arrays (~8× memory)
   (chan 16 {:xf (map inc) :padded true}) ; both"
  ([n]
   (BoundedChannel. (int n)))
  ([n xf-or-opts]
   (if (map? xf-or-opts)
     (let [{:keys [xf padded]} xf-or-opts]
       (if xf
         (BoundedChannelXf. (int n) xf (boolean padded))
         (BoundedChannel. (int n) (boolean padded))))
     (BoundedChannelXf. (int n) xf-or-opts))))


;; # Core operations
;; ################################################################################
(defn put!
  "Put a value on the channel. Parks if the buffer is full.
   Returns true."
  [ch v]
  (IChannel/.put ch v))


(defn take!
  "Take a value from the channel. Parks if the buffer is empty.
   Returns the value."
  [ch]
  (IChannel/.take ch))


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
