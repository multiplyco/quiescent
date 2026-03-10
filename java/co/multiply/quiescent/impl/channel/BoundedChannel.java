package co.multiply.quiescent.impl.channel;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Bounded channel backed by a ring buffer with separate put/take locks.
 * <p>
 * Uses {@link RelayLock} for FIFO mutual exclusion and cross-lock
 * coordination (suspend/resume). The lock is held while suspended —
 * this is safe because producers and consumers use separate locks.
 * <p>
 * When the buffer is empty (consumer) or full (producer), the thread
 * suspends with its lock node. The SIGNALED sentinel handles the race
 * between buffer check and park — no Dekker recheck needed.
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
        this.putLock = new RelayLock();
        this.takeLock = new RelayLock();
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
     * Write one value to the ring buffer, suspending on putLock if full.
     * Resumes takeLock when the buffer transitions from empty to non-empty.
     * <p>
     * Called under putLock. Interrupts are deferred: if interrupted during
     * suspend, sets {@code putInterrupted} and continues until the value
     * is written or the channel closes. Returns true if written, false
     * only when {@code state != OPEN}.
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
                    takeLock.resume();
                }
                return true;
            }
            if (putLock.suspend(node)) {
                putInterrupted = true;
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
        return state == OPEN;
    }

    /**
     * Direct put path. Takes an object and puts it on the channel, possibly
     * parking if the buffer is full.
     */
    @Override
    public boolean put(Object value) throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();
        if (rf != null) {
            // Transducer path — no combining (xf may expand values)
            RelayLock.Node node = putLock.acquire();
            putInterrupted = false;
            try {
                return putXf(value, node);
            } finally {
                node.release();
                if (putInterrupted) Thread.currentThread().interrupt();
            }
        }

        // Direct path with combining
        RelayLock.Node node = new RelayLock.Node(Thread.currentThread());
        node.value = value;
        putLock.acquire(node);

        if (node.combined) return true;

        putInterrupted = false;
        boolean ok = putDirect(value, node);

        // Walk successors if own value was written
        RelayLock.Node last = node;
        if (ok) {
            last = putCombine(node);
        }
        last.release();
        if (putInterrupted) Thread.currentThread().interrupt();
        return ok;
    }

    private RelayLock.Node putCombine(RelayLock.Node node) {
        long t = tail;
        long h = (long) HEAD.getAcquire(this);
        RelayLock.Node current = node;
        long newTail = t;

        while (newTail - h < capacity && state == OPEN) {
            Object succObj = current.state;
            if (!(succObj instanceof RelayLock.Node succ)) break;
            buffer[(int)(newTail & mask)] = succ.value;
            newTail++;
            succ.combined = true;
            succ.wake();
            current = succ;
        }

        if (newTail > t) {
            TAIL.setVolatile(this, newTail);
            takeLock.resume();
        }

        return current;
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();
        RelayLock.Node node = new RelayLock.Node(Thread.currentThread());
        takeLock.acquire(node);

        if (node.combined) return node.value;

        boolean interrupted = false;
        while (true) {
            if (state == CANCELLED) {
                node.release();
                if (interrupted) Thread.currentThread().interrupt();
                return IChannel.CANCELLED;
            }
            long h = head;
            long t = (long) TAIL.getAcquire(this);
            if (t - h > 0) {
                Object ownValue = buffer[(int)(h & mask)];
                buffer[(int)(h & mask)] = null;

                RelayLock.Node last = takeCombine(node, h + 1, t);

                if (last == node) HEAD.setVolatile(this, h + 1);

                boolean signalPut = ((long) TAIL.getVolatile(this) - h) >= capacity;
                last.release();
                if (signalPut) putLock.resume();
                if (interrupted) Thread.currentThread().interrupt();
                return ownValue;
            }
            if (state >= SEALED) {
                node.release();
                if (interrupted) Thread.currentThread().interrupt();
                return IChannel.CANCELLED;
            }
            if (takeLock.suspend(node)) {
                interrupted = true;
            }
        }
    }

    private RelayLock.Node takeCombine(RelayLock.Node node, long startHead, long t) {
        long newHead = startHead;
        RelayLock.Node current = node;

        while (newHead < t && state != CANCELLED) {
            Object succObj = current.state;
            if (!(succObj instanceof RelayLock.Node succ)) break;
            succ.value = buffer[(int)(newHead & mask)];
            buffer[(int)(newHead & mask)] = null;
            newHead++;
            succ.combined = true;
            succ.wake();
            current = succ;
        }

        if (newHead > startHead) {
            HEAD.setVolatile(this, newHead);
        }

        return current;
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
