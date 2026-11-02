package co.multiply.quiescent.impl.channel;

/**
 * Bounded channel with lock-free MPMC put.
 * <p>
 * Uses XADD on both sides for slot claiming — wait-free, one atomic
 * instruction per operation. Three-tier parking for backpressure.
 * <p>
 * See {@link AbstractBoundedChannel} for ring buffer internals and
 * channels-ringbuffer.md for the full design.
 */
public class BoundedChannel extends AbstractBoundedChannel {

    public BoundedChannel(int requestedSize) {
        super(requestedSize, false);
    }

    public BoundedChannel(int requestedSize, boolean padded) {
        super(requestedSize, padded);
    }

    @Override
    public boolean put(Object value) throws InterruptedException {
        long slot = (long) PRODUCER_SEQ.getAndAdd(this, 1L);
        if (slot < 0) return false; // sealed or cancelled

        int idx = (int) (slot & mask);
        int pIdx = idx << ashft;
        long gen = slot >>> sizeShift;

        // Check if slot is free for our generation
        if ((long) AVAIL.getVolatile(avail, pIdx) != -gen) {
            if (!park(producerThreads, pIdx, -gen, gen - 1,
                      producerGateLock, producerGate, PRODUCER_GATE_WAITERS)) {
                return false;
            }
        }

        // Write value and publish
        OBJ_ARRAY.setVolatile(values, pIdx, value);
        AVAIL.setVolatile(avail, pIdx, gen);

        // Dekker: wake consumer parked on this slot
        tryWake(consumerThreads, pIdx);

        // Signal gated consumers — a generation transition happened
        signalGate(consumerGateLock, consumerGate, CONSUMER_GATE_WAITERS);

        return true;
    }
}
