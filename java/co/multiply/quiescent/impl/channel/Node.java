package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * Base participation unit for {@link RelayLock}.
 * <p>
 * A node is a one-shot rendezvous point between consecutive threads
 * in the lock chain. It carries the thread reference, the state
 * handoff field, and an optional payload for combining.
 * <p>
 * Subclasses define operation-specific behavior via {@link #tryCombine()}
 * (what to do when combined by a predecessor) and {@link #onContention()}
 * (what to do when blocked waiting for a predecessor).
 */
public class Node {

    static final Object DONE = new Object();

    static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup()
                .findVarHandle(Node.class, "state", Object.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    final Thread thread;
    volatile Object state; // null | Node | DONE
    Object value;          // payload for combining (put value or take result)
    volatile boolean combined; // set by combiner before waking; the volatile
                           // write provides the happens-before edge

    public Node(Thread thread) {
        this.thread = thread;
    }

    /** Unpark this node's thread. */
    public void wake() {
        LockSupport.unpark(thread);
    }

    /**
     * Try to combine this node's operation. Called by combineChain on
     * each successor. Does operation-specific work (e.g. buffer write/read).
     * Returns true on success. Does NOT set combined/wake — the caller does that.
     */
    boolean tryCombine() {
        return false;
    }

    /** Called when this node must wait for its predecessor. */
    void onContention() {}

    /**
     * Combine waiting successors. Each node's tryCombine does the
     * operation-specific work; this loop handles combined/wake.
     */
    static Node combineChain(Node from) {
        Node current = from;
        while (true) {
            Object succObj = current.state;
            if (!(succObj instanceof Node succ)) break;
            if (!succ.tryCombine()) break;
            succ.combined = true;
            succ.wake();
            current = succ;
        }
        return current;
    }

    /**
     * Release this node: write DONE to state and dispatch
     * on the successor (unpark it if present).
     */
    public void release() {
        Object successor = STATE.getAndSet(this, DONE);
        dispatch(successor);
    }

    /**
     * Dispatch on a waiter: null or Node.
     */
    static void dispatch(Object waiter) {
        if (waiter instanceof Node n) {
            n.wake();
        }
    }
}
