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
 * between SIGNALED (sentinel, no waiter) and the owner's Node
 * (waiter registered). The SIGNALED sentinel handles the race between
 * condition check and park — if a resume fires between the two, the
 * suspending thread sees SIGNALED and returns without parking.
 * Used for cross-lock coordination: a thread holds its lock while
 * suspended, and the other side resumes it.
 * <p>
 * Interruption is deferred: a thread cannot bail mid-chain. It must
 * wait for ownership, then hand off immediately.
 */
public class RelayLock {

    static final Node SIGNALED;

    static final VarHandle SLOT;
    static final VarHandle REF;

    static {
        SIGNALED = new Node(null);
        SIGNALED.state = Node.DONE;
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            SLOT = lookup.findVarHandle(RelayLock.class, "slot", Node.class);
            REF = lookup.findVarHandle(RelayLock.class, "ref", Node.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    volatile Node slot;
    volatile Node ref = SIGNALED;
    Node owner; // plain field — only the owner thread reads/writes it

    public RelayLock() {
        Node initial = new Node(null);
        initial.state = Node.DONE;
        this.slot = initial;
    }

    /**
     * Acquire the lock. Returns the node that must be passed to
     * {@link Node#release()} when the critical section is done.
     * <p>
     * Blocks (parks) until this thread is the owner. Interruption
     * is deferred: the thread cannot bail mid-chain. If interrupted
     * while waiting, the interrupt flag is restored after acquiring.
     */
    public Node acquire() {
        return acquire(new Node(Thread.currentThread()));
    }

    public Node acquire(Node myNode) {
        Node prev = (Node) SLOT.getAndSet(this, myNode);
        Object prevState = Node.STATE.getAndSet(prev, myNode);
        if (prevState != Node.DONE) {
            awaitPredecessor(prev, myNode);
        }
        if (!myNode.combined) {
            this.owner = myNode;
        }
        return myNode;
    }

    private void awaitPredecessor(Node prev, Node myNode) {
        myNode.onContention();
        boolean interrupted = false;
        while (prev.state != Node.DONE && !myNode.combined) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    // ================================================================
    //  Suspend / Resume (cross-lock signaling)
    // ================================================================

    /**
     * Park the current lock owner until resumed.
     * <p>
     * If a SIGNALED sentinel is found, the thread returns immediately
     * without parking. The caller must re-check the buffer condition
     * after returning, as wakeups may be spurious.
     *
     * @return true if the thread was interrupted while parked
     */
    public boolean suspend() {
        Node node = this.owner;
        Node prev = (Node) REF.getAndSet(this, node);
        if (prev == SIGNALED) return false;
        if (prev != node && prev.state != Node.DONE) {
            prev.wake();
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
        if (prev.state == Node.DONE) return; // stale
        prev.wake();
    }
}
