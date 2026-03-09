package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Bounded channel backed by a ring buffer with separate put/take locks.
 * <p>
 * Uses {@link RelayLock} for FIFO mutual exclusion and {@link Signal}
 * for cross-lock coordination. The lock is held while parked on the
 * signal — this is safe because producers and consumers use separate
 * locks.
 * <p>
 * When the buffer is empty (consumer) or full (producer), the thread
 * awaits on the signal with its lock node. Signal's SIGNALED sentinel
 * handles the race between buffer check and park — no Dekker recheck
 * needed.
 */
public class BoundedChannel implements IChannel, IBuffered {

    static final int OPEN = 0;
    static final int SEALED = 1;
    static final int CANCELLED = 2;

    static final VarHandle TAIL;
    static final VarHandle HEAD;
    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            TAIL = lookup.findVarHandle(BoundedChannel.class, "tail", long.class);
            HEAD = lookup.findVarHandle(BoundedChannel.class, "head", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Locks + Signals ----
    final RelayLock putLock = new RelayLock();
    final RelayLock takeLock = new RelayLock();
    final Signal putSignal = new Signal();
    final Signal takeSignal = new Signal();

    // ---- Producer-side ----
    volatile long tail;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared coordination ----
    volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side ----
    volatile long head;

    // ---- Buffer ----
    final Object[] buffer;
    final int capacity;
    final int mask;

    // ================================================================
    //  Constructors
    // ================================================================

    public BoundedChannel(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
    }

    /**
     * Transducer constructor — not yet implemented for RelayLock.
     */
    public BoundedChannel(int requestedSize, Object xf) {
        this(requestedSize);
        if (xf != null)
            throw new UnsupportedOperationException("Transducer support not yet implemented");
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  PUT
    // ================================================================

    @Override
    public boolean put(Object value) {
        RelayLock.Node node = putLock.acquire();
        boolean signalTake = false;
        try {
            while (true) {
                if (state != OPEN) return false;
                long t = tail;
                long h = (long) HEAD.getAcquire(this);
                if (t - h < capacity) {
                    buffer[(int)(t & mask)] = value;
                    TAIL.setVolatile(this, t + 1);
                    signalTake = (t + 1 - (long) HEAD.getVolatile(this)) == 1;
                    return true;
                }
                // Buffer full — await signal from consumer
                if (putSignal.await(node)) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        } finally {
            putLock.release(node);
            if (signalTake) takeSignal.signal(this);
        }
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() {
        RelayLock.Node node = takeLock.acquire();
        boolean signalPut = false;
        try {
            while (true) {
                if (state == CANCELLED) return IChannel.CANCELLED;
                long h = head;
                long t = (long) TAIL.getAcquire(this);
                if (t - h > 0) {
                    Object value = buffer[(int)(h & mask)];
                    buffer[(int)(h & mask)] = null;
                    HEAD.setVolatile(this, h + 1);
                    signalPut = ((long) TAIL.getVolatile(this) - (h + 1)) == capacity - 1;
                    return value;
                }
                if (state >= SEALED) return IChannel.CANCELLED;
                // Buffer empty — await signal from producer
                if (takeSignal.await(node)) {
                    Thread.currentThread().interrupt();
                    return IChannel.CANCELLED;
                }
            }
        } finally {
            takeLock.release(node);
            if (signalPut) putSignal.signal(this);
        }
    }

    // ================================================================
    //  ALT (stripped — to be reintroduced)
    // ================================================================

    @Override
    public Object altTake(ChannelRef ref) {
        throw new UnsupportedOperationException("Alt not yet implemented for RelayLock");
    }

    @Override
    public boolean altPut(Object value, ChannelRef ref) {
        throw new UnsupportedOperationException("Alt not yet implemented for RelayLock");
    }

    // ================================================================
    //  Lifecycle
    // ================================================================

    @Override
    public boolean cancel(String msg) {
        if (this.state == CANCELLED) return false;
        this.state = CANCELLED;
        VarHandle.fullFence();
        putSignal.signal(this);
        takeSignal.signal(this);
        return true;
    }

    @Override
    public boolean seal() {
        if (this.state != OPEN) return false;
        this.state = SEALED;
        VarHandle.fullFence();
        putSignal.signal(this);
        takeSignal.signal(this);
        return true;
    }

    @Override
    public boolean isCancelled() {
        return this.state == CANCELLED;
    }

    @Override
    public boolean isSealed() {
        return this.state >= SEALED;
    }

    // ================================================================
    //  IBuffered
    // ================================================================

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int count() {
        return (int) ((long) TAIL.getVolatile(this) - (long) HEAD.getVolatile(this));
    }

    @Override
    public double saturation() {
        return (double) count() / capacity;
    }
}
