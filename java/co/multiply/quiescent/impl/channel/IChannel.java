package co.multiply.quiescent.impl.channel;

/**
 * Core channel interface. Implemented by each channel type
 * (bounded, sliding, dropping, memo, rendezvous).
 *
 * A channel is a multi-value coordination primitive.
 */
public interface IChannel {

    /** Sentinel returned by {@link #take()} when the channel is sealed and drained. */
    Object SEALED = new Object();

    // Core operations
    /** Wait for a value, or return {@link #SEALED} on exhaustion. Null is a valid value. */
    Object take() throws InterruptedException;
    /** Wait for capacity. Return false if the channel is sealed. */
    boolean put(Object value) throws InterruptedException;

    // Lifecycle
    /** Stop accepting puts, release waiters, and allow buffered values to drain. */
    boolean seal();

    // Query
    boolean isSealed();
}
