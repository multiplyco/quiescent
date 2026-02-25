package co.multiply.quiescent.impl.channel;

/**
 * Interface for channels with a buffer. Provides buffer inspection
 * for monitoring and diagnostics. Not all channels are buffered —
 * memo and rendezvous channels do not implement this.
 */
public interface IBuffered {
    /** Buffer capacity (always a power of 2 for bounded channels). */
    int capacity();

    /** Approximate number of values currently in the buffer. */
    int count();

    /** Buffer saturation as a ratio from 0.0 (empty) to 1.0 (full). */
    double saturation();
}
