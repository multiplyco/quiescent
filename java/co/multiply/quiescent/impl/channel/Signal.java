package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * Cross-lock signaling between producers and consumers.
 * <p>
 * When a thread holds its lock but can't proceed (buffer empty for
 * consumer, buffer full for producer), it calls {@link #await()} to
 * register and park. The other side calls {@link #signal(IChannel)}
 * after changing the buffer state to wake the waiting thread.
 * <p>
 * Internally, a single atomic field holds null (nobody waiting),
 * a Thread reference, or an AltNode. Both await and signal are a
 * single getAndSet.
 */
public class Signal {

    static final VarHandle REF;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            REF = lookup.findVarHandle(Signal.class, "ref", Object.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    volatile Object ref; // null | Thread | AltNode

    /**
     * Register the current thread and park until signaled.
     * <p>
     * The caller must re-check the buffer condition after waking,
     * as wakeups may be spurious or due to cancellation.
     */
    public void await() {
        Thread self = Thread.currentThread();
        Object prev = REF.getAndSet(this, self);
        if (prev instanceof Thread t) {
            LockSupport.unpark(t);
        }
        LockSupport.park(this);
    }

    /**
     * Register an AltNode and park until signaled.
     * <p>
     * The AltNode sits in both the signal field and the lock chain.
     * When signaled, the dispatcher will try to claim the Alt or
     * skip past it to the lock chain successor.
     */
    public void await(RelayLock.AltNode altNode) {
        Object prev = REF.getAndSet(this, altNode);
        if (prev instanceof Thread t) {
            LockSupport.unpark(t);
        }
        LockSupport.park(this);
    }

    /**
     * Wake the waiting thread or AltNode, if any.
     * <p>
     * Uses the shared dispatch loop: handles Thread (unpark),
     * AltNode (try claim or skip), and null (no-op).
     *
     * @param channel the channel identity for Alt claim, or null
     */
    public void signal(IChannel channel) {
        Object prev = REF.getAndSet(this, null);
        RelayLock.dispatch(prev, channel);
    }

    /**
     * Wake the waiting thread, if any (non-Alt path).
     */
    public void signal() {
        signal(null);
    }
}
