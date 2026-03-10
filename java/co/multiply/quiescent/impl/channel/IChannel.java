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

    // Core operations
    Object take();
    boolean put(Object value);

    // Lifecycle
    boolean cancel(String msg);
    boolean seal();

    // Query
    boolean isCancelled();
    boolean isSealed();
}
