package co.multiply.quiescent.impl.channel;

/**
 * Core channel interface. Implemented by each channel type
 * (bounded, sliding, dropping, memo, rendezvous).
 *
 * A channel is a multi-value coordination primitive.
 */
public interface IChannel {

    /** Sentinel returned by {@link #take()} when the channel is cancelled or sealed+drained. */
    Object CANCELLED = new Object();

    /** Sentinel returned by {@link #altTake} when no value is immediately available. */
    Object UNAVAILABLE = new Object();

    // Core operations
    Object take();
    boolean put(Object value);

    // Alt operations — non-blocking attempts that enqueue an AltNode if not ready.
    // Returns value (including null) if taken, CANCELLED if sealed+drained, UNAVAILABLE if enqueued.
    Object altTake(ChannelRef ref);
    // Returns true if put succeeded (and claims ref), false if full (enqueued) or closed.
    boolean altPut(Object value, ChannelRef ref);

    // Lifecycle
    boolean cancel(String msg);
    boolean seal();

    // Query
    boolean isCancelled();
    boolean isSealed();
}
