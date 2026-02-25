package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base for bounded channels backed by a Disruptor-inspired ring buffer.
 * <p>
 * Buffer size is always a power of 2 (rounded up from requested size).
 * Parallel arrays: values[], producerThreads[], consumerThreads[], avail[].
 * <p>
 * Subclasses provide the {@link #put(Object)} implementation:
 * <ul>
 *   <li>{@link BoundedChannel} — lock-free XADD put (MPMC).</li>
 *   <li>{@link BoundedChannelXf} — locked put with transducer support.</li>
 * </ul>
 * <p>
 * See channels-ringbuffer.md for the full design.
 */
public abstract class AbstractBoundedChannel implements IChannel, IBuffered {

    // Sentinel: slot is clear, ready for the next generation's waiter.
    protected static final Object CLEARED = new Object();

    // -- Ring buffer structure --
    protected static final int ASHFT_PADDED = 3; // 8× stride = 64-byte cache lines
    protected static final int SPIN_LIMIT = 256;

    protected final Object[] values;
    protected final Object[] producerThreads;
    protected final Object[] consumerThreads;
    protected final long[] avail;
    protected final int ashft;
    protected final int size;
    protected final int mask;
    protected final int sizeShift;

    // -- Sequence counters with cache-line padding --
    @SuppressWarnings("unused")
    private long p01, p02, p03, p04, p05, p06, p07;
    protected volatile long producerSeq;
    @SuppressWarnings("unused")
    private long p11, p12, p13, p14, p15, p16, p17;
    protected volatile long consumerSeq;
    @SuppressWarnings("unused")
    private long p21, p22, p23, p24, p25, p26, p27;

    // -- Gate: shared per-side condition for generationally-ahead threads --
    protected final ReentrantLock producerGateLock = new ReentrantLock();
    protected final Condition producerGate = producerGateLock.newCondition();
    protected volatile int producerGateWaiters;
    @SuppressWarnings("unused")
    private long p31, p32, p33, p34, p35, p36, p37;

    protected final ReentrantLock consumerGateLock = new ReentrantLock();
    protected final Condition consumerGate = consumerGateLock.newCondition();
    protected volatile int consumerGateWaiters;
    @SuppressWarnings("unused")
    private long p41, p42, p43, p44, p45, p46, p47;

    // -- VarHandles for atomic operations --
    protected static final VarHandle PRODUCER_SEQ;
    protected static final VarHandle CONSUMER_SEQ;
    protected static final VarHandle AVAIL;
    protected static final VarHandle OBJ_ARRAY;
    protected static final VarHandle PRODUCER_GATE_WAITERS;
    protected static final VarHandle CONSUMER_GATE_WAITERS;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            PRODUCER_SEQ = l.findVarHandle(AbstractBoundedChannel.class, "producerSeq", long.class);
            CONSUMER_SEQ = l.findVarHandle(AbstractBoundedChannel.class, "consumerSeq", long.class);
            AVAIL = MethodHandles.arrayElementVarHandle(long[].class);
            OBJ_ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
            PRODUCER_GATE_WAITERS = l.findVarHandle(AbstractBoundedChannel.class, "producerGateWaiters", int.class);
            CONSUMER_GATE_WAITERS = l.findVarHandle(AbstractBoundedChannel.class, "consumerGateWaiters", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Construction ----

    protected AbstractBoundedChannel(int requestedSize, boolean padded) {
        if (requestedSize < 1) {
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        }
        this.ashft = padded ? ASHFT_PADDED : 0;
        this.size = nextPowerOf2(requestedSize);
        this.mask = size - 1;
        this.sizeShift = Integer.numberOfTrailingZeros(size);

        int paddedSize = size << ashft;
        this.values = new Object[paddedSize];
        this.producerThreads = new Object[paddedSize];
        this.consumerThreads = new Object[paddedSize];
        this.avail = new long[paddedSize];
        Arrays.fill(this.avail, -1L);

        // Initialize registry slots to CLEARED
        Arrays.fill(this.producerThreads, CLEARED);
        Arrays.fill(this.consumerThreads, CLEARED);

        // Counters start at size so first generation = 1, avoiding -0 == 0
        this.producerSeq = size;
        this.consumerSeq = size;
    }

    protected static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ---- IChannel: Core operations ----

    @Override
    public Object take() {
        long slot = (long) CONSUMER_SEQ.getAndAdd(this, 1L);
        int idx = (int) (slot & mask);
        int pIdx = idx << ashft;
        long gen = slot >>> sizeShift;

        // Check if value is published for our generation
        if ((long) AVAIL.getVolatile(avail, pIdx) != gen) {
            park(consumerThreads, pIdx, gen, -gen,
                 consumerGateLock, consumerGate, CONSUMER_GATE_WAITERS);
        }

        // Read value
        Object value = OBJ_ARRAY.getVolatile(values, pIdx);

        // Consume: mark slot as free for next generation's producer
        AVAIL.setVolatile(avail, pIdx, -(gen + 1));

        // Dekker: wake producer parked on this slot
        tryWake(producerThreads, pIdx);

        // Signal gated producers — a generation transition happened
        signalGate(producerGateLock, producerGate, PRODUCER_GATE_WAITERS);

        return value;
    }

    // ---- Three-tier parking ----

    protected void park(Object[] registry, int pIdx, long expectedAvail, long prevStep,
                        ReentrantLock gateLock, Condition gate,
                        VarHandle gateWaiters) {
        Thread self = Thread.currentThread();
        int spins = 0;

        // Tier 2/3: am I next in line, or should I gate?
        while (true) {
            long currentAvail = (long) AVAIL.getVolatile(avail, pIdx);
            if (currentAvail == expectedAvail) return;

            if (currentAvail == prevStep) {
                // Tier 2: I'm next — try to register for Dekker park.
                if (OBJ_ARRAY.compareAndSet(registry, pIdx, CLEARED, self)) {
                    break; // registered, proceed to Dekker park below
                }
                // Previous gen hasn't written CLEARED yet. Brief spin.
                Thread.onSpinWait();
            } else {
                if (spins < SPIN_LIMIT) {
                    spins++;
                    Thread.onSpinWait();
                    continue;
                }

                // Tier 3: too far ahead — gate until channel progresses.
                gateLock.lock();
                try {
                    gateWaiters.getAndAdd(this, 1);
                    currentAvail = (long) AVAIL.getVolatile(avail, pIdx);
                    if (currentAvail != expectedAvail && currentAvail != prevStep) {
                        gate.await();
                    }
                    gateWaiters.getAndAdd(this, -1);
                } catch (InterruptedException e) {
                    gateWaiters.getAndAdd(this, -1);
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    gateLock.unlock();
                }
                spins = 0; // Reset spins after waking from gate
            }
        }

        // Tier 2: Registered — Dekker park.
        // Spin a bit before parking for real
        for (int i = 0; i < SPIN_LIMIT; i++) {
            if ((long) AVAIL.getVolatile(avail, pIdx) == expectedAvail) {
                OBJ_ARRAY.setVolatile(registry, pIdx, CLEARED);
                return;
            }
            Thread.onSpinWait();
        }

        while ((long) AVAIL.getVolatile(avail, pIdx) != expectedAvail) {
            LockSupport.park(this);
        }
        // Clear: write CLEARED so next generation can CAS in.
        OBJ_ARRAY.setVolatile(registry, pIdx, CLEARED);
    }

    protected void tryWake(Object[] registry, int pIdx) {
        Object occupant = OBJ_ARRAY.getVolatile(registry, pIdx);
        if (occupant != CLEARED) {
            LockSupport.unpark((Thread) occupant);
        }
    }

    protected void signalGate(ReentrantLock gateLock, Condition gate,
                              VarHandle gateWaiters) {
        // Volatile read: skip lock acquisition if nobody is gated
        if ((int) gateWaiters.getVolatile(this) > 0) {
            gateLock.lock();
            try {
                gate.signalAll();
            } finally {
                gateLock.unlock();
            }
        }
    }

    // ---- IChannel: Lifecycle (stubs — cancellation deferred) ----

    @Override
    public boolean cancel(String msg) {
        throw new UnsupportedOperationException("Cancellation not yet implemented");
    }

    @Override
    public boolean seal() {
        throw new UnsupportedOperationException("Seal not yet implemented");
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isSealed() {
        return false;
    }

    // ---- IBuffered ----

    @Override
    public int capacity() {
        return size;
    }

    @Override
    public int count() {
        // Approximate: claimed-but-not-yet-published slots are counted
        long p = (long) PRODUCER_SEQ.getVolatile(this);
        long c = (long) CONSUMER_SEQ.getVolatile(this);
        long diff = p - c;
        if (diff < 0) return 0;
        if (diff > size) return size;
        return (int) diff;
    }

    @Override
    public double saturation() {
        return (double) count() / size;
    }
}
