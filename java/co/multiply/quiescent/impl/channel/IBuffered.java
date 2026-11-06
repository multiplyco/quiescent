package co.multiply.quiescent.impl.channel;

/**
 * Interface for channels with a buffer. Provides buffer inspection
 * for monitoring and diagnostics. Not all channels are buffered —
 * memo and rendezvous channels do not implement this.
 */
public interface IBuffered {
    /** Buffer capacity (always a power of 2 for bounded channels). */
    int capacity();

    /**
     * Approximate buffered count, bounded by zero and {@link #capacity()}.
     * For diagnostics; concurrent observations need not form an atomic snapshot.
     */
    int count();

    /** Approximate buffer saturation, bounded by 0.0 and 1.0. */
    double saturation();
}
