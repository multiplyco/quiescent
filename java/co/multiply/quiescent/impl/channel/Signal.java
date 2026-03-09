package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * Cross-lock signaling between producers and consumers.
 * <p>
 * When a thread holds its lock but can't proceed (buffer empty for
 * consumer, buffer full for producer), it calls {@link #await()} to
 * register and park. The other side calls {@link #signal()} after
 * changing the buffer state to wake the waiting thread.
 * <p>
 * Internally, a single atomic field holds either null (nobody waiting)
 * or a Thread reference (someone waiting). Both operations are a single
 * getAndSet.
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

    volatile Object ref; // null | Thread

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
     * Wake the waiting thread, if any.
     * <p>
     * Called by the other side after depositing or consuming a value.
     */
    public void signal() {
        Object prev = REF.getAndSet(this, null);
        if (prev instanceof Thread t) {
            LockSupport.unpark(t);
        }
    }
}
