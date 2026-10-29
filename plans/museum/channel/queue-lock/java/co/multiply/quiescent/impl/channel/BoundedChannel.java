package co.multiply.quiescent.impl.channel;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * Bounded MPMC channel backed by a power-of-2 ring buffer and a
 * queue lock (no ReentrantLock, no AQS, no condition variables).
 *
 * <p>Optionally wraps a transducer. When {@code rf} is non-null,
 * puts run through the transducer's step function; when null, puts
 * write directly to the buffer. The JIT profiles the branch on the
 * final field and eliminates the dead path.
 */
public class BoundedChannel implements IChannel, IBuffered {

    static final int OPEN = 0;
    static final int SEALED = 1;
    static final int CANCELLED = 2;

    // ---- VarHandles ----

    static final VarHandle SHARED_TAIL;
    static final VarHandle SHARED_HEAD;
    static final VarHandle STATE;
    static final VarHandle PRODUCER_OWNER;
    static final VarHandle PRODUCER_NEXT;
    static final VarHandle PRODUCER_TAIL;
    static final VarHandle CONSUMER_OWNER;
    static final VarHandle CONSUMER_NEXT;
    static final VarHandle CONSUMER_TAIL;
    static final VarHandle QNODE_NEXT;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            SHARED_TAIL = l.findVarHandle(BoundedChannel.class, "sharedTail", long.class);
            SHARED_HEAD = l.findVarHandle(BoundedChannel.class, "sharedHead", long.class);
            STATE = l.findVarHandle(BoundedChannel.class, "state", int.class);
            PRODUCER_OWNER = l.findVarHandle(BoundedChannel.class, "producerOwner", Thread.class);
            PRODUCER_NEXT = l.findVarHandle(BoundedChannel.class, "producerNext", QNode.class);
            PRODUCER_TAIL = l.findVarHandle(BoundedChannel.class, "producerTail", QNode.class);
            CONSUMER_OWNER = l.findVarHandle(BoundedChannel.class, "consumerOwner", Thread.class);
            CONSUMER_NEXT = l.findVarHandle(BoundedChannel.class, "consumerNext", QNode.class);
            CONSUMER_TAIL = l.findVarHandle(BoundedChannel.class, "consumerTail", QNode.class);
            QNODE_NEXT = l.findVarHandle(QNode.class, "next", QNode.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Producer-side fields ----
    @SuppressWarnings("unused") volatile Thread producerOwner;
    @SuppressWarnings("unused") volatile QNode producerNext;
    @SuppressWarnings("unused") volatile QNode producerTail;
    long tail;
    long headCache;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared coordination ----
    @SuppressWarnings("unused") volatile long sharedTail;

    // ---- Padding between shared variables ----
    @SuppressWarnings("unused") private long pt01, pt02, pt03, pt04, pt05, pt06, pt07, pt08;

    @SuppressWarnings("unused") volatile long sharedHead;
    @SuppressWarnings("unused") volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side fields ----
    @SuppressWarnings("unused") volatile Thread consumerOwner;
    @SuppressWarnings("unused") volatile QNode consumerNext;
    @SuppressWarnings("unused") volatile QNode consumerTail;
    long head;
    long tailCache;

    // ---- Buffer ----
    final Object[] buffer;
    final int capacity;
    final int mask;
    final IFn rf; // null for plain channels

    private static final class PutFailedException extends RuntimeException {
        static final PutFailedException INSTANCE = new PutFailedException();
        private PutFailedException() { super(null, null, false, false); }
    }

    // ---- Tunables (via -D properties) ----
    static final int SPINS_BEFORE_PARK = Integer.getInteger(
            "quiescent.channel.spinsBeforePark", 64);
    static final int SPINS_RELEASE_LINK = Integer.getInteger(
            "quiescent.channel.spinsReleaseLink", 1024);

    // ---- Constructors ----

    public BoundedChannel(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
        this.rf = null;
        QNode pHead = new QNode(null);
        QNode cHead = new QNode(null);
        PRODUCER_NEXT.setVolatile(this, pHead);
        CONSUMER_NEXT.setVolatile(this, cHead);
        PRODUCER_TAIL.setVolatile(this, pHead);
        CONSUMER_TAIL.setVolatile(this, cHead);
    }

    public BoundedChannel(int requestedSize, IFn xf) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];

        AFn baseRf = new AFn() {
            public Object invoke(Object acc) {
                return acc;
            }
            public Object invoke(Object acc, Object val) {
                int c = putDirect(val);
                if (c < 0) throw PutFailedException.INSTANCE;
                // Ensure the write to sharedTail and buffer element is
                // visible to a consumer before we unpark it on empty->nonempty.
                if (c == 0) {
                    VarHandle.fullFence();
                    wakeConsumer();
                }
                return acc;
            }
        };
        this.rf = (IFn) xf.invoke(baseRf);
        QNode pHead2 = new QNode(null);
        QNode cHead2 = new QNode(null);
        PRODUCER_NEXT.setVolatile(this, pHead2);
        CONSUMER_NEXT.setVolatile(this, cHead2);
        PRODUCER_TAIL.setVolatile(this, pHead2);
        CONSUMER_TAIL.setVolatile(this, cHead2);
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  Queue lock — producer side helpers
    // ================================================================

    final void producerRelease() {
        QNode head = (QNode) PRODUCER_NEXT.getVolatile(this);
        QNode succ = head.next;
        if (succ != null) {
            producerHandover(succ);
            return;
        }
        // No successor visible yet. Is one pending?
        if ((QNode) PRODUCER_TAIL.getVolatile(this) != head) {
            // Spin with a bounded limit for the link. If the enqueuer is
            // preempted between getAndSet(tail) and setRelease(link), we
            // must not spin forever while holding the lock — that blocks
            // every other producer.
            for (int spins = SPINS_RELEASE_LINK; spins > 0; spins--) {
                succ = head.next;
                if (succ != null) {
                    producerHandover(succ);
                    return;
                }
                Thread.onSpinWait();
            }
            // Link still not visible (enqueuer preempted). Fall through
            // to release the lock so other producers can make progress.
            // The stalled enqueuer will self-promote when it resumes.
        }

        // No successor observed or link not visible. Clear owner.
        PRODUCER_OWNER.setVolatile(this, null);

        // Final check for race: a successor might have enqueued between
        // the tail check above and the owner clear. We must spin for
        // the link here — if head.next is null but tail != head, the
        // enqueuer WILL complete the link, and the waiter may already
        // be parked and unable to self-promote.
        if ((QNode) PRODUCER_TAIL.getVolatile(this) != head) {
            while ((succ = head.next) == null) {
                // Must yield the carrier on virtual threads so the
                // enqueuer (which may share this carrier) can complete
                // its setRelease on pred.next.
                LockSupport.parkNanos(this, 1L);
            }
            Thread t = succ.thread;
            if (t != null) LockSupport.unpark(t);
        }
    }

    private void producerHandover(QNode succ) {
        Thread t = succ.thread;
        succ.thread = null;
        PRODUCER_NEXT.setRelease(this, succ);
        PRODUCER_OWNER.setRelease(this, t);
        LockSupport.unpark(t);
    }

    final boolean producerSelfPromote(Thread self, QNode node) {
        QNode head = (QNode) PRODUCER_NEXT.getVolatile(this);
        if (head.next != node) return false;
        if (!PRODUCER_OWNER.compareAndSet(this, null, self)) return false;
        node.thread = null;
        PRODUCER_NEXT.setVolatile(this, node);
        return true;
    }

    final void producerEnqueueNode(QNode node) {
        node.next = null;
        QNode pred = (QNode) PRODUCER_TAIL.getAndSet(this, node);
        QNODE_NEXT.setRelease(pred, node);
    }

    // ================================================================
    //  Queue lock — consumer side helpers
    // ================================================================

    final void consumerRelease() {
        QNode head = (QNode) CONSUMER_NEXT.getVolatile(this);
        QNode succ = head.next;
        if (succ != null) {
            consumerHandover(succ);
            return;
        }
        // No successor visible yet. Is one pending?
        if ((QNode) CONSUMER_TAIL.getVolatile(this) != head) {
            for (int spins = SPINS_RELEASE_LINK; spins > 0; spins--) {
                succ = head.next;
                if (succ != null) {
                    consumerHandover(succ);
                    return;
                }
                Thread.onSpinWait();
            }
        }

        // No successor observed or link not visible. Clear owner.
        CONSUMER_OWNER.setVolatile(this, null);

        // Final check — spin for the link if a successor is pending.
        if ((QNode) CONSUMER_TAIL.getVolatile(this) != head) {
            while ((succ = head.next) == null) {
                LockSupport.parkNanos(this, 1L);
            }
            Thread t = succ.thread;
            if (t != null) LockSupport.unpark(t);
        }
    }

    private void consumerHandover(QNode succ) {
        Thread t = succ.thread;
        succ.thread = null;
        CONSUMER_NEXT.setRelease(this, succ);
        CONSUMER_OWNER.setRelease(this, t);
        LockSupport.unpark(t);
    }

    final boolean consumerSelfPromote(Thread self, QNode node) {
        QNode head = (QNode) CONSUMER_NEXT.getVolatile(this);
        if (head.next != node) return false;
        if (!CONSUMER_OWNER.compareAndSet(this, null, self)) return false;
        node.thread = null;
        CONSUMER_NEXT.setVolatile(this, node);
        return true;
    }

    final void consumerEnqueueNode(QNode node) {
        node.next = null;
        QNode pred = (QNode) CONSUMER_TAIL.getAndSet(this, node);
        QNODE_NEXT.setRelease(pred, node);
    }

    // ================================================================
    //  Cross-side signaling
    // ================================================================

    void wakeConsumer() {
        Thread t = (Thread) CONSUMER_OWNER.getAcquire(this);
        if (t != null) {
            LockSupport.unpark(t);
            return;
        }
        // No owner; try waking the head of the wait queue to self-promote
        QNode head = (QNode) CONSUMER_NEXT.getAcquire(this);
        QNode succ = head.next;
        if (succ != null && succ.thread != null) {
            LockSupport.unpark(succ.thread);
        }
    }

    void wakeProducer() {
        Thread t = (Thread) PRODUCER_OWNER.getAcquire(this);
        if (t != null) {
            LockSupport.unpark(t);
            return;
        }
        // No owner; try waking the head of the wait queue to self-promote
        QNode head = (QNode) PRODUCER_NEXT.getAcquire(this);
        QNode succ = head.next;
        if (succ != null && succ.thread != null) {
            LockSupport.unpark(succ.thread);
        }
    }

    // ================================================================
    //  PUT
    // ================================================================

    @Override
    public boolean put(Object value) {
        Thread self = Thread.currentThread();

        if (!PRODUCER_OWNER.compareAndSet(this, null, self)) {
            producerWait(self);
        }

        if (rf == null) {
            return putPlain(value);
        } else {
            return putXf(value);
        }
    }

    private void producerWait(Thread self) {
        QNode node = new QNode(self);
        producerEnqueueNode(node);
        boolean wasInterrupted = false;
        while (true) {
            Thread owner = (Thread) PRODUCER_OWNER.getVolatile(this);
            if (owner == self) break;
            if (owner == null) {
                if (producerSelfPromote(self, node)) break;
            }
            // Short bounded spin to avoid immediate park/unpark churn
            boolean acquired = false;
            for (int spins = SPINS_BEFORE_PARK; spins > 0; spins--) {
                owner = (Thread) PRODUCER_OWNER.getVolatile(this);
                if (owner == self) { acquired = true; break; }
                if (owner == null && producerSelfPromote(self, node)) { acquired = true; break; }
                Thread.onSpinWait();
            }
            if (acquired) break;
            LockSupport.park(this);
            if (Thread.interrupted()) {
                wasInterrupted = true;
            }
        }
        if (wasInterrupted) {
            self.interrupt();
        }
    }

    private boolean putPlain(Object value) {
        int c = putDirect(value);
        producerRelease();
        if (c == 0) {
            VarHandle.fullFence();
            wakeConsumer();
        }
        return c >= 0;
    }

    private boolean putXf(Object value) {
        try {
            if (state >= SEALED) return false;
            Object result = rf.invoke(this, value);
            if (RT.isReduced(result)) {
                rf.invoke(this); // flush stateful xforms
                throw new UnsupportedOperationException("Seal not yet implemented");
            }
            return true;
        } catch (PutFailedException e) {
            return false;
        } finally {
            producerRelease();
        }
    }

    int putDirect(Object value) {
        long t = tail;
        long hc = headCache;
        if (t - hc >= capacity) {
            hc = (long) SHARED_HEAD.getAcquire(this);
            headCache = hc;
            int spins = SPINS_BEFORE_PARK;
            while (t - hc >= capacity) {
                if ((int) STATE.getAcquire(this) != OPEN) return -1;
                if (spins-- > 0) {
                    Thread.onSpinWait();
                } else {
                    LockSupport.park(this);
                }
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    return -1;
                }
                hc = (long) SHARED_HEAD.getAcquire(this);
                headCache = hc;
            }
        }
        if ((int) STATE.getAcquire(this) != OPEN) return -1;

        buffer[(int)(t & mask)] = value;
        SHARED_TAIL.setRelease(this, t + 1);
        tail = t + 1;

        long h = (long) SHARED_HEAD.getAcquire(this);
        return (t == h) ? 0 : 1;
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public final Object take() {
        Thread self = Thread.currentThread();

        if (!CONSUMER_OWNER.compareAndSet(this, null, self)) {
            consumerWait(self);
        }

        return takeWork();
    }

    private void consumerWait(Thread self) {
        QNode node = new QNode(self);
        consumerEnqueueNode(node);
        boolean wasInterrupted = false;
        while (true) {
            Thread owner = (Thread) CONSUMER_OWNER.getVolatile(this);
            if (owner == self) break;
            if (owner == null) {
                if (consumerSelfPromote(self, node)) break;
            }
            boolean acquired = false;
            for (int spins = SPINS_BEFORE_PARK; spins > 0; spins--) {
                owner = (Thread) CONSUMER_OWNER.getVolatile(this);
                if (owner == self) { acquired = true; break; }
                if (owner == null && consumerSelfPromote(self, node)) { acquired = true; break; }
                Thread.onSpinWait();
            }
            if (acquired) break;
            LockSupport.park(this);
            if (Thread.interrupted()) {
                wasInterrupted = true;
            }
        }
        if (wasInterrupted) {
            self.interrupt();
        }
    }

    private Object takeWork() {
        long h = head;
        long tc = tailCache;
        if (h == tc) {
            tc = (long) SHARED_TAIL.getAcquire(this);
            tailCache = tc;
            int spins = SPINS_BEFORE_PARK;
            while (h == tc) {
                if ((int) STATE.getAcquire(this) >= SEALED) {
                    consumerRelease();
                    return IChannel.CANCELLED;
                }
                if (spins-- > 0) {
                    Thread.onSpinWait();
                } else {
                    LockSupport.park(this);
                }
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    consumerRelease();
                    return IChannel.CANCELLED;
                }
                tc = (long) SHARED_TAIL.getAcquire(this);
                tailCache = tc;
            }
        }
        if ((int) STATE.getAcquire(this) == CANCELLED) {
            consumerRelease();
            return IChannel.CANCELLED;
        }

        Object value = buffer[(int)(h & mask)];
        buffer[(int)(h & mask)] = null;
        SHARED_HEAD.setRelease(this, h + 1);
        head = h + 1;

        long t = (long) SHARED_TAIL.getAcquire(this);

        consumerRelease();

        if (t - h >= capacity) {
            VarHandle.fullFence();
            wakeProducer();
        }
        return value;
    }

    // ================================================================
    //  Lifecycle
    // ================================================================

    @Override
    public boolean cancel(String msg) {
        if (!STATE.compareAndSet(this, OPEN, CANCELLED)
                && !STATE.compareAndSet(this, SEALED, CANCELLED)) {
            return false;
        }
        wakeProducer();
        wakeConsumer();
        return true;
    }

    @Override
    public boolean seal() {
        if (!STATE.compareAndSet(this, OPEN, SEALED)) {
            return false;
        }
        wakeProducer();
        wakeConsumer();
        return true;
    }

    @Override
    public boolean isCancelled() {
        return (int) STATE.getAcquire(this) == CANCELLED;
    }

    @Override
    public boolean isSealed() {
        return (int) STATE.getAcquire(this) >= SEALED;
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
        long t = (long) SHARED_TAIL.getVolatile(this);
        long h = (long) SHARED_HEAD.getVolatile(this);
        int c = (int) (t - h);
        return c < 0 ? 0 : c;
    }

    @Override
    public double saturation() {
        return (double) count() / capacity;
    }
}
