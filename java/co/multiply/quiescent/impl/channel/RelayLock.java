package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * A FIFO handoff lock using three exchange (getAndSet) operations per
 * lock cycle.
 * <p>
 * Threads form an implicit chain by swapping nodes into a shared slot.
 * Each node is a one-shot rendezvous point between consecutive threads:
 * the owner writes DONE when finished, the successor writes its thread
 * reference when it arrives. Both use getAndSet; the return value tells
 * each side whether the other has already acted.
 * <p>
 * No spinning, no CAS, no queue structure. The chain lives on the stack
 * of the participating threads. FIFO ordering falls out of arrival order
 * at the slot.
 * <p>
 * Interruption is deferred: a thread cannot bail mid-chain. It must wait
 * for ownership, then hand off immediately.
 */
public class RelayLock {

    static final Object DONE = new Object();

    static final VarHandle SLOT;
    static final VarHandle STATE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            SLOT  = lookup.findVarHandle(RelayLock.class, "slot", Node.class);
            STATE = lookup.findVarHandle(Node.class, "state", Object.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static class Node {
        volatile Object state; // null | Thread | AltNode | DONE
    }

    public static class AltNode extends Node {
        final Thread thread;
        final ChannelRef ref;

        public AltNode(Thread thread, ChannelRef ref) {
            this.thread = thread;
            this.ref = ref;
        }
    }

    volatile Node slot;

    public RelayLock() {
        Node initial = new Node();
        initial.state = DONE;
        this.slot = initial;
    }

    /**
     * Acquire the lock. Returns the node that must be passed to
     * {@link #release(Node, IChannel)} when the critical section is done.
     * <p>
     * Blocks (parks) until this thread is the owner. Interruption
     * is deferred: the thread cannot bail mid-chain. If interrupted
     * while waiting, the interrupt flag is restored after acquiring.
     */
    public Node acquire() {
        Node myNode = new Node();
        Node prev = (Node) SLOT.getAndSet(this, myNode);

        // Register on predecessor's node.
        Object prevState = STATE.getAndSet(prev, Thread.currentThread());

        if (prevState == DONE) {
            // Predecessor already finished — we are the owner.
            return myNode;
        }

        // Predecessor still working. Park until it writes DONE.
        boolean interrupted = false;
        while (prev.state != DONE) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }
        if (interrupted) Thread.currentThread().interrupt();
        return myNode;
    }

    /**
     * Acquire the lock with an AltNode. The AltNode is used as this
     * thread's node in the chain, and is written onto the predecessor's
     * state (instead of a Thread reference).
     * <p>
     * Returns the AltNode for passing to {@link #release(Node, IChannel)}.
     */
    public AltNode acquireAlt(AltNode altNode) {
        Node prev = (Node) SLOT.getAndSet(this, altNode);

        // Register AltNode on predecessor (not Thread).
        Object prevState = STATE.getAndSet(prev, altNode);

        if (prevState == DONE) {
            return altNode;
        }

        // Park until predecessor writes DONE.
        boolean interrupted = false;
        while (prev.state != DONE) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }
        if (interrupted) Thread.currentThread().interrupt();
        return altNode;
    }

    /**
     * Release the lock. Writes DONE to this node, then dispatches
     * on the successor: unpark Thread, try-claim AltNode, or skip
     * dead AltNodes by writing DONE and following the chain.
     *
     * @param node    the node returned by acquire() or acquireAlt()
     * @param channel the channel identity for Alt claim, or null
     */
    public void release(Node node, IChannel channel) {
        Object successor = STATE.getAndSet(node, DONE);
        dispatch(successor, channel);
    }

    /**
     * Release the lock (non-Alt path, no claim).
     */
    public void release(Node node) {
        release(node, null);
    }

    /**
     * Dispatch on a waiter: null, Thread, or AltNode.
     * Shared by release() and Signal.signal().
     */
    static void dispatch(Object waiter, IChannel channel) {
        while (true) {
            if (waiter == null) return;
            if (waiter instanceof Thread t) {
                LockSupport.unpark(t);
                return;
            }
            // AltNode — try to claim
            AltNode alt = (AltNode) waiter;
            if (channel != null && alt.ref.claim(channel)) {
                LockSupport.unpark(alt.thread);
                return;
            }
            // Dead alt (or no channel to claim with) — release its
            // lock node and follow the successor.
            waiter = STATE.getAndSet(alt, DONE);
        }
    }
}
