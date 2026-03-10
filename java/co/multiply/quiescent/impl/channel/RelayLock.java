package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * A FIFO handoff lock with integrated cross-lock signaling.
 * <p>
 * <b>Lock (acquire/release)</b> — three exchange (getAndSet) operations
 * per lock cycle. Threads form an implicit chain by swapping nodes into
 * a shared slot. Each node is a one-shot rendezvous point between
 * consecutive threads: the owner writes DONE when finished, the
 * successor writes its node reference when it arrives. Both use
 * getAndSet; the return value tells each side whether the other has
 * already acted. No spinning, no CAS, no queue structure. FIFO
 * ordering falls out of arrival order at the slot.
 * <p>
 * <b>Signal (suspend/resume)</b> — a single atomic field transitions
 * between SIGNALED (sentinel, no waiter) and a caller-provided Node
 * (waiter registered). The SIGNALED sentinel eliminates the need for
 * Dekker-style rechecks — if a resume fires between the buffer check
 * and the suspend, the suspending thread sees SIGNALED and returns
 * without parking. Used for cross-lock coordination: a thread holds
 * its lock while suspended, and the other side resumes it.
 * <p>
 * Interruption is deferred: a thread cannot bail mid-chain. It must
 * wait for ownership, then hand off immediately.
 */
public class RelayLock {

    static final Object DONE = new Object();

    static final Node SIGNALED;

    static final VarHandle SLOT;
    static final VarHandle STATE;
    static final VarHandle REF;

    static {
        SIGNALED = new Node(null);
        SIGNALED.state = DONE;
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            SLOT = lookup.findVarHandle(RelayLock.class, "slot", Node.class);
            STATE = lookup.findVarHandle(Node.class, "state", Object.class);
            REF = lookup.findVarHandle(RelayLock.class, "ref", Node.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static class Node {
        final Thread thread;
        volatile Object state; // null | Node | DONE
        Object value;          // payload for combining (put value or take result)
        boolean combined;      // set by combiner before waking; visibility
                               // guaranteed by happens-before through the
                               // volatile write of DONE to predecessor's state

        public Node(Thread thread) {
            this.thread = thread;
        }
    }

    volatile Node slot;
    volatile Node ref = SIGNALED;

    public RelayLock() {
        Node initial = new Node(null);
        initial.state = DONE;
        this.slot = initial;
    }

    /**
     * Acquire the lock. Returns the node that must be passed to
     * {@link #release(Node)} when the critical section is done.
     * <p>
     * Blocks (parks) until this thread is the owner. Interruption
     * is deferred: the thread cannot bail mid-chain. If interrupted
     * while waiting, the interrupt flag is restored after acquiring.
     */
    public Node acquire() {
        Node myNode = new Node(Thread.currentThread());
        Node prev = (Node) SLOT.getAndSet(this, myNode);
        Object prevState = STATE.getAndSet(prev, myNode);
        if (prevState == DONE) return myNode;
        awaitPredecessor(prev, myNode);
        return myNode;
    }

    private void awaitPredecessor(Node prev, Node myNode) {
        boolean interrupted = false;
        while (prev.state != DONE && !myNode.combined) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    /**
     * Release the lock. Writes DONE to this node, then dispatches
     * on the successor: unpark the successor's thread if present.
     *
     * @param node the node returned by acquire()
     */
    public void release(Node node) {
        Object successor = STATE.getAndSet(node, DONE);
        dispatch(successor);
    }

    // ================================================================
    //  Suspend / Resume (cross-lock signaling)
    // ================================================================

    /**
     * Register the given node and park until resumed.
     * <p>
     * If a SIGNALED sentinel is found, the thread returns immediately
     * without parking. The caller must re-check the buffer condition
     * after returning, as wakeups may be spurious.
     *
     * @return true if the thread was interrupted while parked
     */
    public boolean suspend(Node node) {
        Node prev = (Node) REF.getAndSet(this, node);
        if (prev == SIGNALED) return false;
        if (prev != node && prev.state != DONE) {
            LockSupport.unpark(prev.thread);
        }
        boolean interrupted = false;
        while (ref == node) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }
        return interrupted;
    }

    /**
     * Wake the waiting thread, if any.
     * <p>
     * If no waiter is registered (SIGNALED), this is a no-op.
     * If a waiter is found, wakes it directly.
     */
    public void resume() {
        Node prev = (Node) REF.getAndSet(this, SIGNALED);
        if (prev == SIGNALED) return;
        if (prev.state == DONE) return; // stale
        LockSupport.unpark(prev.thread);
    }

    /**
     * Dispatch on a waiter: null or Node.
     */
    static void dispatch(Object waiter) {
        if (waiter instanceof Node n) {
            LockSupport.unpark(n.thread);
        }
    }
}
