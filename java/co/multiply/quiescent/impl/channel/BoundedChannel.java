package co.multiply.quiescent.impl.channel;

import clojure.lang.AFn;
import clojure.lang.IFn;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Bounded channel backed by a ring buffer with separate put/take locks.
 * <p>
 * Uses {@link RelayLock} for FIFO mutual exclusion and cross-lock
 * coordination (suspend/resume). The lock is held while suspended —
 * this is safe because producers and consumers use separate locks.
 * <p>
 * Cross-lock signaling uses a Dekker-style protocol: the producer
 * writes TAIL then reads HEAD; the consumer writes HEAD then reads
 * TAIL. Sequential consistency guarantees at least one side sees the
 * other's update, so the consumer only signals the producer when the
 * buffer was observably full. The SIGNALED sentinel in RelayLock
 * handles the residual race between the Dekker check and the park.
 * <p>
 * Buffer writes are accumulated via {@link PutNode#putAccumulate} without
 * bumping the committed tail. A single volatile TAIL write flushes
 * all accumulated values at the end of {@link #put}, or earlier if
 * backpressure forces a mid-batch flush (e.g. mapcat expansion filling
 * the buffer). This batches volatile writes and gives consumers a
 * larger batch to drain per wake-up.
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

    // ---- Locks ----
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

    // ---- Put-side state (putLock only) ----
    final IFn rf;
    boolean putInterrupted;
    long putTail;
    boolean putSealed;

    // ---- Take-side state (takeLock only) ----
    long takeHead;
    long takeTail;

    // ================================================================
    //  Constructors
    // ================================================================

    public BoundedChannel(int requestedSize) {
        this(requestedSize, null);
    }

    public BoundedChannel(int requestedSize, Object xf) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
        this.putLock = new RelayLock();
        this.takeLock = new RelayLock();
        if (xf != null) {
            IFn baseRf = new AFn() {
                public Object invoke(Object acc) {
                    return acc;
                }
                public Object invoke(Object acc, Object val) {
                    ((PutNode) putLock.owner).putAccumulate(val);
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
    //  Buffer operations
    // ================================================================

    Object readSlot(long i) {
        return buffer[(int)(i & mask)];
    }

    void writeSlot(long i, Object v) {
        buffer[(int)(i & mask)] = v;
    }

    void clearSlot(long i) {
        buffer[(int)(i & mask)] = null;
    }

    Object readAndClearSlot(long i) {
        int idx = (int)(i & mask);
        Object val = buffer[idx];
        buffer[idx] = null;
        return val;
    }

    boolean hasCapacity(long putTail) {
        long h = (long) HEAD.getAcquire(this);
        return putTail - h < capacity;
    }

    boolean isOpen() {
        return this.state == OPEN;
    }

    /**
     * Advance HEAD, release the node, and resume producers if the
     * buffer was observably full before this drain.
     * <p>
     * Preserve the baseline volatile HEAD publication before the handoff
     * and subsequent TAIL check. Callers must not release the node separately.
     */
    void commitHeadAndSignal(long newHead, long origHead, Node node) {
        HEAD.setVolatile(this, newHead);
        node.release();
        long freshT = (long) TAIL.getAcquire(this);
        if (freshT - origHead >= capacity) {
            putLock.resume();
        }
    }

    /**
     * Take a single item: read and clear the slot, commit HEAD,
     * release the node, and signal producers. Convenience for
     * non-combining take paths (e.g. Alt).
     */
    Object takeOneItem(long head, Node node) {
        Object val = readAndClearSlot(head);
        commitHeadAndSignal(head + 1, head, node);
        return val;
    }

    // ================================================================
    //  PUT
    // ================================================================

    @Override
    public boolean put(Object value) throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();
        PutNode node = new PutNode(Thread.currentThread(), this);
        node.value = value;
        putLock.acquire(node);
        if (node.combined) return true;
        return node.execute();
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();
        TakeNode node = new TakeNode(Thread.currentThread(), this);
        takeLock.acquire(node);
        if (node.combined) return node.value;
        return node.execute();
    }

    // ================================================================
    //  Lifecycle
    // ================================================================

    @Override
    public boolean cancel(String msg) {
        if (this.state == CANCELLED) return false;
        this.state = CANCELLED;
        VarHandle.fullFence();
        putLock.resume();
        takeLock.resume();
        return true;
    }

    @Override
    public boolean seal() {
        if (this.state != OPEN) return false;
        this.state = SEALED;
        VarHandle.fullFence();
        putLock.resume();
        takeLock.resume();
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
