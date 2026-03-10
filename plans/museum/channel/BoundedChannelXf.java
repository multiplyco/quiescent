package co.multiply.quiescent.impl.channel;

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;

/**
 * Bounded channel with transducer support.
 * <p>
 * Producers are serialized through a {@link ReentrantLock} to protect
 * stateful transducer state. The consumer side remains lock-free,
 * inheriting the same ring buffer and three-tier parking from
 * {@link AbstractBoundedChannel}.
 * <p>
 * The transducer wraps a base reducing function whose step writes
 * directly into the ring buffer via {@link #putDirect(Object)}.
 * Transducers like {@code (mapcat f)} may call putDirect multiple
 * times per input — parking mid-batch while holding the xfLock is
 * safe because only consumers free slots and they don't need the lock.
 */
public class BoundedChannelXf extends AbstractBoundedChannel {

    private final ReentrantLock xfLock = new ReentrantLock();
    private final IFn rf;

    public BoundedChannelXf(int requestedSize, IFn xf) {
        this(requestedSize, xf, false);
    }

    public BoundedChannelXf(int requestedSize, IFn xf, boolean padded) {
        super(requestedSize, padded);

        AFn baseRf = new AFn() {
            // complete — no-op (seal deferred)
            public Object invoke(Object acc) {
                return acc;
            }
            // step — direct ring buffer put
            public Object invoke(Object acc, Object val) {
                putDirect(val);
                return acc;
            }
        };
        this.rf = (IFn) xf.invoke(baseRf);
    }

    @Override
    public boolean put(Object value) throws InterruptedException {
        if ((long) PRODUCER_SEQ.getVolatile(this) < 0) return false;
        xfLock.lock();
        try {
            if ((long) PRODUCER_SEQ.getVolatile(this) < 0) return false;
            Object result = rf.invoke(this, value);
            if (RT.isReduced(result)) {
                rf.invoke(this); // xform complete (flush)
                throw new UnsupportedOperationException("Seal not yet implemented");
            }
        } finally {
            xfLock.unlock();
        }
        return true;
    }

    /**
     * Single-value ring buffer put with simplified producer parking.
     * <p>
     * Since xfLock guarantees single-producer, we skip the Dekker CAS
     * protocol and write directly into the producer thread registry.
     */
    private void putDirect(Object value) {
        // xfLock guarantees single-threaded access, so we can use opaque read/write
        // instead of the more expensive volatile/XADD operations.
        long slot = (long) PRODUCER_SEQ.getOpaque(this);
        PRODUCER_SEQ.setOpaque(this, slot + 1);

        int idx = (int) (slot & mask);
        int pIdx = idx << ashft;
        long gen = slot >>> sizeShift;

        // Spin-wait for slot to become free
        long a = (long) AVAIL.getVolatile(avail, pIdx);
        if (a != -gen) {
            if (a == CANCELLED_AVAIL) return; // cancelled

            boolean ready = false;
            // Brief spin first
            for (int i = 0; i < SPIN_LIMIT; i++) {
                a = (long) AVAIL.getVolatile(avail, pIdx);
                if (a == -gen) { ready = true; break; }
                if (a == CANCELLED_AVAIL) return;
                Thread.onSpinWait();
            }

            // Park if still blocked — single producer, no CAS needed
            if (!ready) {
                Thread self = Thread.currentThread();
                OBJ_ARRAY.setVolatile(producerThreads, pIdx, self);

                // Spin a bit after registration before parking
                for (int i = 0; i < SPIN_LIMIT; i++) {
                    a = (long) AVAIL.getVolatile(avail, pIdx);
                    if (a == -gen) { ready = true; break; }
                    if (a == CANCELLED_AVAIL) {
                        OBJ_ARRAY.setVolatile(producerThreads, pIdx, CLEARED);
                        return;
                    }
                    Thread.onSpinWait();
                }

                if (!ready) {
                    while (true) {
                        a = (long) AVAIL.getVolatile(avail, pIdx);
                        if (a == -gen) break;
                        if (a == CANCELLED_AVAIL) {
                            OBJ_ARRAY.setVolatile(producerThreads, pIdx, CLEARED);
                            return;
                        }
                        LockSupport.park(this);
                        // Check if our registration was replaced (cancel)
                        if (OBJ_ARRAY.getVolatile(producerThreads, pIdx) != self) return;
                    }
                }
                OBJ_ARRAY.setVolatile(producerThreads, pIdx, CLEARED);
            }
        }

        // Write value and publish
        OBJ_ARRAY.setVolatile(values, pIdx, value);
        AVAIL.setVolatile(avail, pIdx, gen);

        // Wake consumer parked on this slot
        tryWake(consumerThreads, pIdx);

        // Signal gated consumers
        signalGate(consumerGateLock, consumerGate, CONSUMER_GATE_WAITERS);
    }
}
