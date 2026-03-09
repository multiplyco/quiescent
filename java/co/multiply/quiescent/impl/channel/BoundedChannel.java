package co.multiply.quiescent.impl.channel;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;
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
 * <p>
 * When a transducer is supplied, the reducing function wraps
 * {@link #putDirect}, which writes one value at a time to the ring
 * buffer with backpressure. Stateful transducers like {@code mapcat}
 * that expand a single input into multiple outputs will park mid-batch
 * when the buffer is full, resuming as consumers drain.
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
    final Signal putSignal;
    final Signal takeSignal;
    final RelayLock putLock;
    final RelayLock takeLock;

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

    // ---- Transducer ----
    final IFn rf;
    RelayLock.Node putNode;     // current lock node, written/read under putLock only
    boolean putInterrupted;     // interrupt captured during putDirect, under putLock only

    // ================================================================
    //  Constructors
    // ================================================================

    public BoundedChannel(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
        this.rf = null;
        this.putLock = new RelayLock(this);
        this.takeLock = new RelayLock(this);
        this.putSignal = new Signal(putLock);
        this.takeSignal = new Signal(takeLock);
    }

    public BoundedChannel(int requestedSize, Object xf) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
        this.putLock = new RelayLock(this);
        this.takeLock = new RelayLock(this);
        this.putSignal = new Signal(putLock);
        this.takeSignal = new Signal(takeLock);
        if (xf != null) {
            IFn baseRf = new AFn() {
                public Object invoke(Object acc) {
                    return acc;
                }
                public Object invoke(Object acc, Object val) {
                    putDirect(val, putNode);
                    return acc;
                }
            };
            this.rf = (IFn) ((IFn) xf).invoke(baseRf);
        } else {
            this.rf = null;
        }
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  PUT
    // ================================================================

    /**
     * Write one value to the ring buffer, parking on putSignal if full.
     * Signals takeSignal when the buffer transitions from empty to non-empty.
     * <p>
     * Called under putLock. On interrupt, sets {@code putInterrupted} and
     * returns false without restoring the flag — the caller decides how
     * to handle it.
     */
    private boolean putDirect(Object value, RelayLock.Node node) {
        while (true) {
            if (state != OPEN) return false;
            long t = tail;
            long h = (long) HEAD.getAcquire(this);
            if (t - h < capacity) {
                buffer[(int)(t & mask)] = value;
                TAIL.setVolatile(this, t + 1);
                if (t + 1 - (long) HEAD.getVolatile(this) == 1) {
                    takeSignal.signal();
                }
                return true;
            }
            if (putSignal.await(node)) {
                putInterrupted = true;
                return false;
            }
        }
    }

    /**
     * Transducer put path. Runs the value through the reducing function,
     * which calls putDirect via the baseRf for each output value.
     */
    private boolean putXf(Object value, RelayLock.Node node) {
        if (state != OPEN) return false;
        this.putNode = node;
        Object result = rf.invoke(this, value);
        if (RT.isReduced(result)) {
            rf.invoke(this);
            seal();
            return true;
        }
        if (putInterrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    /**
     * Direct put path. Takes an object and puts it on the channel, possibly
     * parking if the buffer is full.
     */
    @Override
    public boolean put(Object value) {
        RelayLock.Node node = putLock.acquire();
        try {
            putInterrupted = false;
            if (rf != null) return putXf(value, node);
            boolean ok = putDirect(value, node);
            if (putInterrupted) Thread.currentThread().interrupt();
            return ok;
        } finally {
            putLock.release(node);
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
            if (signalPut) putSignal.signal();
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
        putSignal.signal();
        takeSignal.signal();
        return true;
    }

    @Override
    public boolean seal() {
        if (this.state != OPEN) return false;
        this.state = SEALED;
        VarHandle.fullFence();
        putSignal.signal();
        takeSignal.signal();
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
