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

    static final VarHandle COUNT;
    static final VarHandle STATE;
    static final VarHandle PRODUCER_OWNER;
    static final VarHandle PRODUCER_NEXT;
    static final VarHandle CONSUMER_OWNER;
    static final VarHandle CONSUMER_NEXT;
    static final VarHandle QNODE_NEXT;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            COUNT = l.findVarHandle(BoundedChannel.class, "count", int.class);
            STATE = l.findVarHandle(BoundedChannel.class, "state", int.class);
            PRODUCER_OWNER = l.findVarHandle(BoundedChannel.class, "producerOwner", Thread.class);
            PRODUCER_NEXT = l.findVarHandle(BoundedChannel.class, "producerNext", QNode.class);
            CONSUMER_OWNER = l.findVarHandle(BoundedChannel.class, "consumerOwner", Thread.class);
            CONSUMER_NEXT = l.findVarHandle(BoundedChannel.class, "consumerNext", QNode.class);
            QNODE_NEXT = l.findVarHandle(QNode.class, "next", QNode.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Producer-side fields ----
    @SuppressWarnings("unused") volatile Thread producerOwner;
    @SuppressWarnings("unused") volatile QNode producerNext;
    QNode producerTailHint;
    long tail;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared coordination ----
    @SuppressWarnings("unused") volatile int count;
    @SuppressWarnings("unused") volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side fields ----
    @SuppressWarnings("unused") volatile Thread consumerOwner;
    @SuppressWarnings("unused") volatile QNode consumerNext;
    QNode consumerTailHint;
    private long head;

    // ---- Buffer ----
    final Object[] buffer;
    final int capacity;
    final int mask;
    final IFn rf; // null for plain channels

    // ---- Constructors ----

    public BoundedChannel(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
        this.rf = null;
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
                if (c == 0) wakeConsumer();
                return acc;
            }
        };
        this.rf = (IFn) xf.invoke(baseRf);
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  Queue lock — producer side helpers
    // ================================================================

    final void producerRelease() {
        QNode succ = (QNode) PRODUCER_NEXT.getVolatile(this);
        if (succ != null) {
            producerHandover(succ);
            return;
        }
        // No successor — clear owner (volatile for Dekker)
        PRODUCER_OWNER.setVolatile(this, null);
        // Dekker re-check: someone may have queued after our read
        succ = (QNode) PRODUCER_NEXT.getVolatile(this);
        if (succ != null) {
            LockSupport.unpark(succ.thread); // let them self-promote
        }
    }

    private void producerHandover(QNode succ) {
        // Unlink successor: advance producerNext to successor's next
        PRODUCER_NEXT.setRelease(this, succ.next);
        // Write successor's thread onto owner — plain store, single writer
        PRODUCER_OWNER.setRelease(this, succ.thread);
        LockSupport.unpark(succ.thread);
    }

    final boolean producerSelfPromote(Thread self, QNode node) {
        QNode head = (QNode) PRODUCER_NEXT.getVolatile(this);
        if (head != node) return false;
        if (!PRODUCER_OWNER.compareAndSet(this, null, self)) return false;
        PRODUCER_NEXT.setVolatile(this, node.next);
        return true;
    }

    final void producerEnqueueNode(QNode node) {
        while (true) {
            QNode head = (QNode) PRODUCER_NEXT.getAcquire(this);
            if (head == null) {
                if (PRODUCER_NEXT.compareAndSet(this, null, node)) {
                    producerTailHint = node;
                    return;
                }
                continue;
            }
            QNode tail = findTail(head);
            if (QNODE_NEXT.compareAndSet(tail, null, node)) {
                producerTailHint = node;
                return;
            }
        }
    }

    static QNode findTail(QNode from) {
        QNode n = from;
        while (n.next != null) {
            n = n.next;
        }
        return n;
    }

    // ================================================================
    //  Queue lock — consumer side helpers
    // ================================================================

    final void consumerRelease() {
        QNode succ = (QNode) CONSUMER_NEXT.getVolatile(this);
        if (succ != null) {
            consumerHandover(succ);
            return;
        }
        CONSUMER_OWNER.setVolatile(this, null);
        // Dekker re-check: someone may have queued after our read
        succ = (QNode) CONSUMER_NEXT.getVolatile(this);
        if (succ != null) {
            LockSupport.unpark(succ.thread); // let them self-promote
        }
    }

    private void consumerHandover(QNode succ) {
        CONSUMER_NEXT.setRelease(this, succ.next);
        CONSUMER_OWNER.setRelease(this, succ.thread);
        LockSupport.unpark(succ.thread);
    }

    final boolean consumerSelfPromote(Thread self, QNode node) {
        QNode head = (QNode) CONSUMER_NEXT.getVolatile(this);
        if (head != node) return false;
        if (!CONSUMER_OWNER.compareAndSet(this, null, self)) return false;
        CONSUMER_NEXT.setVolatile(this, node.next);
        return true;
    }

    final void consumerEnqueueNode(QNode node) {
        while (true) {
            QNode head = (QNode) CONSUMER_NEXT.getAcquire(this);
            if (head == null) {
                if (CONSUMER_NEXT.compareAndSet(this, null, node)) {
                    consumerTailHint = node;
                    return;
                }
                continue;
            }
            QNode tail = findTail(head);
            if (QNODE_NEXT.compareAndSet(tail, null, node)) {
                consumerTailHint = node;
                return;
            }
        }
    }

    // ================================================================
    //  Cross-side signaling
    // ================================================================

    void wakeConsumer() {
        Thread t = (Thread) CONSUMER_OWNER.getAcquire(this);
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    void wakeProducer() {
        Thread t = (Thread) PRODUCER_OWNER.getAcquire(this);
        if (t != null) {
            LockSupport.unpark(t);
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
        QNode node = new QNode(self, null);
        producerEnqueueNode(node);
        while (true) {
            Thread owner = (Thread) PRODUCER_OWNER.getVolatile(this);
            if (owner == self) return;
            if (owner == null) {
                if (producerSelfPromote(self, node)) return;
            }
            LockSupport.park(this);
        }
    }

    private boolean putPlain(Object value) {
        int c = putDirect(value);
        producerRelease();
        if (c == 0) wakeConsumer();
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
        } finally {
            producerRelease();
        }
    }

    int putDirect(Object value) {
        while ((int) COUNT.getAcquire(this) >= capacity) {
            if ((int) STATE.getAcquire(this) != OPEN) return -1;
            LockSupport.park(this);
            if (Thread.interrupted()) return -1;
        }
        if ((int) STATE.getAcquire(this) != OPEN) return -1;
        buffer[(int)(tail++ & mask)] = value;
        return (int) COUNT.getAndAddRelease(this, 1);
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
        QNode node = new QNode(self, null);
        consumerEnqueueNode(node);
        while (true) {
            Thread owner = (Thread) CONSUMER_OWNER.getVolatile(this);
            if (owner == self) return;
            if (owner == null) {
                if (consumerSelfPromote(self, node)) return;
            }
            LockSupport.park(this);
        }
    }

    private Object takeWork() {
        while ((int) COUNT.getAcquire(this) == 0) {
            if ((int) STATE.getAcquire(this) >= SEALED) {
                consumerRelease();
                return IChannel.CANCELLED;
            }
            LockSupport.park(this);
            if (Thread.interrupted()) {
                consumerRelease();
                return IChannel.CANCELLED;
            }
        }
        if ((int) STATE.getAcquire(this) == CANCELLED) {
            consumerRelease();
            return IChannel.CANCELLED;
        }

        Object value = buffer[(int)(head & mask)];
        buffer[(int)(head++ & mask)] = null;
        int c = (int) COUNT.getAndAddRelease(this, -1);

        consumerRelease();

        if (c == capacity) wakeProducer();
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
        return (int) COUNT.getVolatile(this);
    }

    @Override
    public double saturation() {
        return (double) (int) COUNT.getVolatile(this) / capacity;
    }
}
