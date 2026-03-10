package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * CLH-style queue lock with no-sleep-in-place semantics.
 * <p>
 * The {@code owner} field means "actively working" — no thread ever parks
 * while holding the lock. When a thread discovers the buffer is empty/full,
 * it nulls owner ({@link #unlock()}), enqueues itself, does a Dekker
 * recheck, and parks.
 * <p>
 * {@link #wake()} is the only way to serve queued nodes. It walks the queue,
 * CAS-transfers ownership, and unparks the next waiter. Callers only call
 * wake() when the condition is actually met (buffer has items / has space).
 * <p>
 * Both regular waiters and alt waiters share the same FIFO queue.
 */
public class ChannelLock {

    static final VarHandle OWNER;
    static final VarHandle TAIL;
    static final VarHandle NODE_NEXT;
    static final VarHandle NODE_THREAD;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            OWNER       = lookup.findVarHandle(ChannelLock.class, "owner", Thread.class);
            TAIL        = lookup.findVarHandle(ChannelLock.class, "tail", Node.class);
            NODE_NEXT   = lookup.findVarHandle(Node.class, "next", Node.class);
            NODE_THREAD = lookup.findVarHandle(Node.class, "thread", Thread.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static class Node {
        volatile Thread thread;
        volatile Node next;

        Node(Thread thread) {
            this.thread = thread;
        }
    }

    /**
     * Queue node carrying a ChannelRef for alt operations.
     * Created by {@link #enqueueAlt(ChannelRef)}.
     */
    static class AltNode extends Node {
        final ChannelRef ref;

        AltNode(Thread thread, ChannelRef ref) {
            super(thread);
            this.ref = ref;
        }
    }

    @SuppressWarnings("unused") private long p01, p02, p03, p04, p05, p06, p07, p08;

    volatile Thread owner;
    volatile Node head;
    volatile Node tail;

    @SuppressWarnings("unused") private long p11, p12, p13, p14, p15, p16, p17, p18;

    final IChannel channel;

    public ChannelLock() {
        this(null);
    }

    public ChannelLock(IChannel channel) {
        this.channel = channel;
        Node sentinel = new Node(null);
        this.head = sentinel;
        this.tail = sentinel;
    }

    // ================================================================
    //  tryAcquire / unlock
    // ================================================================

    /**
     * Try to acquire the lock. Returns true if CAS succeeded.
     * Never blocks. Caller enqueues on failure.
     */
    public boolean tryAcquire() {
        return OWNER.compareAndSet(this, null, Thread.currentThread());
    }

    /**
     * Spin briefly trying to acquire the lock. The lock is never held
     * while parked, so max hold time is a few buffer operations (~10ns).
     * Returns true if acquired within maxSpins attempts.
     */
    public boolean spinAcquire(int maxSpins) {
        Thread self = Thread.currentThread();
        for (int i = 0; i < maxSpins; i++) {
            if (OWNER.compareAndSet(this, null, self))
                return true;
            if (i < 32) Thread.onSpinWait(); else Thread.yield();
        }
        return false;
    }

    /**
     * Release the lock. Does not walk the queue — callers decide
     * when to call {@link #wake()} based on buffer state.
     */
    public void unlock() {
        OWNER.setVolatile(this, null);
    }

    // ================================================================
    //  Enqueue
    // ================================================================

    /**
     * Enqueue a regular waiter node. Returns the node so the caller
     * can clear its thread field if abandoning (e.g., channel sealed
     * while parked).
     */
    public Node enqueue() {
        Node node = new Node(Thread.currentThread());
        Node prev = (Node) TAIL.getAndSet(this, node);
        NODE_NEXT.setVolatile(prev, node);
        return node;
    }

    /**
     * Enqueue an AltNode for alt operations.
     *
     * @param ref the shared ChannelRef for this alt operation
     */
    public void enqueueAlt(ChannelRef ref) {
        AltNode node = new AltNode(Thread.currentThread(), ref);
        Node prev = (Node) TAIL.getAndSet(this, node);
        NODE_NEXT.setVolatile(prev, node);
    }

    // ================================================================
    //  Wake — unified queue serving
    // ================================================================

    /**
     * Serve the next queued waiter.
     * <p>
     * Uses {@code getAndSet} on {@code NODE_THREAD} to atomically claim
     * each node, preventing two concurrent wake() calls from re-serving
     * the same node (ABA on the owner field). If the owner CAS fails
     * (a barger took the lock via tryAcquire), the claimed thread is
     * re-enqueued at the tail so it gets another chance.
     * <p>
     * AltNodes are handled differently: no lock ownership is transferred.
     * The ref is claimed and the alt thread is unparked so it can compete
     * for the lock independently via put()/take(). After serving an
     * AltNode, wake() continues to look for a regular waiter.
     */
    public void wake() {
        Node h = this.head;
        while (true) {
            if (h == (Node) TAIL.getVolatile(this)) return;

            Node next = (Node) NODE_NEXT.getVolatile(h);
            if (next == null) {
                // Late enqueuer — spin until link visible.
                while ((next = (Node) NODE_NEXT.getVolatile(h)) == null)
                    Thread.yield();
            }

            if (next instanceof AltNode alt && alt.ref.get() != null) {
                // AltNode already claimed by another channel — dead. Skip.
                this.head = next;
                h = next;
                continue;
            }

            // Atomically claim the node's thread (one-shot per node).
            Thread t = (Thread) NODE_THREAD.getAndSet(next, null);
            if (t == null) {
                // Dead node (already claimed or abandoned) — skip.
                this.head = next;
                h = next;
                continue;
            }

            // Advance head past this node.
            this.head = next;

            // AltNode — no lock transfer. Claim ref, unpark, keep going.
            if (next instanceof AltNode alt) {
                if (!alt.ref.claim(this.channel)) {
                    // Lost claim race — skip, continue to next node.
                    h = next;
                    continue;
                }
                LockSupport.unpark(t);
                h = next;
                continue; // keep serving — condition still valid
            }

            // Regular node — transfer lock ownership.
            if (!OWNER.compareAndSet(this, null, t)) {
                // Barger took lock — re-enqueue the thread at the tail
                // and unpark so it can re-check state (handles race where
                // wakeAll finishes walking before this node is linked).
                Node reNode = new Node(t);
                Node prev = (Node) TAIL.getAndSet(this, reNode);
                NODE_NEXT.setVolatile(prev, reNode);
                LockSupport.unpark(t);
                return;
            }

            LockSupport.unpark(t);
            return;
        }
    }

    /**
     * Wake all waiters. Used for lifecycle events (seal, cancel).
     * <p>
     * Walks the entire queue: claims all unclaimed AltNodes, unparks
     * all threads. Regular nodes wake, see state changed, and bail out.
     * Calls {@link #wake()} at the end to properly serve the first node.
     */
    public void wakeAll() {
        Node n = this.head;
        Node next;
        while ((next = (Node) NODE_NEXT.getVolatile(n)) != null) {
            if (next instanceof AltNode alt) {
                alt.ref.claim(this.channel);
            }
            Thread t = (Thread) NODE_THREAD.getVolatile(next);
            if (t != null) LockSupport.unpark(t);
            n = next;
        }
        // Serve the first node properly if no one holds the lock.
        wake();
    }
}
