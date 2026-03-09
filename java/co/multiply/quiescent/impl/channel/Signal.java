package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * Cross-lock signaling between producers and consumers.
 * <p>
 * When a thread holds its lock but can't proceed (buffer empty for
 * consumer, buffer full for producer), it calls {@link #await(RelayLock.Node)}
 * to register and park. The other side calls {@link #signal(IChannel)}
 * after changing the buffer state to wake the waiting thread.
 * <p>
 * Internally, a single atomic field transitions between two states:
 * SIGNALED (a distinguished Node sentinel — signal pending, no waiter)
 * and a caller-provided Node (waiter registered). The SIGNALED sentinel
 * eliminates the need for Dekker-style rechecks — if a signal fires
 * between the buffer check and the await, the awaiting thread sees
 * SIGNALED and returns without parking.
 */
public class Signal {

    static final RelayLock.Node SIGNALED;

    static final VarHandle REF;

    static {
        SIGNALED = new RelayLock.Node(null);
        SIGNALED.state = RelayLock.DONE;
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            REF = lookup.findVarHandle(Signal.class, "ref", RelayLock.Node.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    volatile RelayLock.Node ref = SIGNALED;

    /**
     * Register the given node and park until signaled.
     * <p>
     * If a SIGNALED sentinel is found, the thread returns immediately
     * without parking. The caller must re-check the buffer condition
     * after returning, as wakeups may be spurious.
     *
     * @return true if the thread was interrupted while parked
     */
    public boolean await(RelayLock.Node node) {
        RelayLock.Node prev = (RelayLock.Node) REF.getAndSet(this, node);
        if (prev == SIGNALED) return false;
        if (prev != node && prev.state != RelayLock.DONE) {
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
     * Convenience await for standalone usage (without a RelayLock).
     * Creates a temporary Node internally.
     */
    public void await() {
        RelayLock.Node node = new RelayLock.Node(Thread.currentThread());
        await(node);
    }

    /**
     * Register an AltNode and park until signaled.
     * <p>
     * The AltNode sits in both the signal field and the lock chain.
     * When signaled, the dispatcher will try to claim the Alt or
     * skip past it to the lock chain successor.
     *
     * @return true if the thread was interrupted while parked
     */
    public boolean await(RelayLock.AltNode altNode) {
        RelayLock.Node prev = (RelayLock.Node) REF.getAndSet(this, altNode);
        if (prev == SIGNALED) return false;
        if (prev != altNode && prev.state != RelayLock.DONE) {
            LockSupport.unpark(prev.thread);
        }
        boolean interrupted = false;
        while (ref == altNode) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }
        return interrupted;
    }

    /**
     * Wake the waiting thread or AltNode, if any.
     * <p>
     * If no waiter is registered (SIGNALED), this is a no-op.
     * If a waiter is found, wakes it directly.
     *
     * @param channel the channel identity for Alt claim, or null
     */
    public void signal(IChannel channel) {
        RelayLock.Node prev = (RelayLock.Node) REF.getAndSet(this, SIGNALED);
        if (prev == SIGNALED) return;
        if (prev instanceof RelayLock.AltNode alt) {
            if (channel != null && alt.ref.claim(channel)) {
                LockSupport.unpark(alt.thread);
                return;
            }
            // Dead alt — release its lock node and follow the successor chain
            Object waiter = RelayLock.STATE.getAndSet(alt, RelayLock.DONE);
            RelayLock.dispatch(waiter, channel);
        } else {
            if (prev.state == RelayLock.DONE) return; // stale
            LockSupport.unpark(prev.thread);
        }
    }

    /**
     * Wake the waiting thread, if any (non-Alt path).
     */
    public void signal() {
        signal(null);
    }
}
