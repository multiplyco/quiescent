package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;

/**
 * Bounded channel backed by a ring buffer with separate put/take locks.
 * <p>
 * Uses {@link ChannelLock} with no-sleep-in-place semantics: the lock
 * is never held while parked. When the buffer is empty/full, the thread
 * releases the lock, enqueues itself, does a Dekker recheck, and parks.
 * {@code wake()} serves the next queued waiter when the condition is met.
 */
public class BoundedChannel implements IChannel, IBuffered {

    static final int OPEN = 0;
    static final int SEALED = 1;
    static final int CANCELLED = 2;

    static final VarHandle TAIL;
    static final VarHandle HEAD;
    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            TAIL = lookup.findVarHandle(BoundedChannel.class, "tail", long.class);
            HEAD = lookup.findVarHandle(BoundedChannel.class, "head", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Locks ----
    final ChannelLock putLock;
    final ChannelLock takeLock;

    // ---- Producer-side ----
    volatile long tail;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared coordination ----
    volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side ----
    volatile long head;

    // ---- Buffer ----
    final Object[] buffer;
    final int capacity;
    final int mask;

    // ---- Transducer (null = plain channel) ----
    final IFn rf;

    // ================================================================
    //  Constructors
    // ================================================================

    public BoundedChannel(int requestedSize) {
        this(requestedSize, null);
    }

    public BoundedChannel(int requestedSize, IFn xf) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.putLock = new ChannelLock(this);
        this.takeLock = new ChannelLock(this);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];

        if (xf != null) {
            AFn baseRf = new AFn() {
                public Object invoke(Object acc) {
                    return acc;
                }
                public Object invoke(Object acc, Object val) {
                    // Called under putLock. If buffer full, release, wait, re-acquire.
                    while (true) {
                        if (state != OPEN) return acc;
                        long t = tail;
                        long h = (long) HEAD.getAcquire(BoundedChannel.this);
                        if (t - h < capacity) {
                            buffer[(int)(t & mask)] = val;
                            TAIL.setVolatile(BoundedChannel.this, t + 1);
                            int itemsAfter = (int)(t + 1 - (long) HEAD.getVolatile(BoundedChannel.this));
                            if (itemsAfter == 1) takeLock.wake();
                            return acc;
                        }
                        // Full — release, enqueue, Dekker, park, re-acquire
                        putLock.unlock();
                        ChannelLock.Node node = putLock.enqueue();
                        if ((long) TAIL.getAcquire(BoundedChannel.this)
                              - (long) HEAD.getAcquire(BoundedChannel.this) < capacity
                            || state != OPEN) {
                            putLock.wake();
                        }
                        parkLoop(node, putLock);
                        if ((Thread) ChannelLock.OWNER.getVolatile(putLock) != Thread.currentThread())
                            return acc;
                        // Now own putLock again — retry buffer write
                    }
                }
            };
            this.rf = (IFn) xf.invoke(baseRf);
        } else {
            this.rf = null;
        }
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  Park loop — shared helper
    // ================================================================

    /**
     * Park until wake() transfers ownership to this thread, or until
     * the channel's state changes (sealed/cancelled). On state change,
     * the node's thread field is cleared so wake() skips it.
     */
    private void parkLoop(ChannelLock.Node node, ChannelLock lock) {
        Thread self = Thread.currentThread();
        while ((Thread) ChannelLock.OWNER.getVolatile(lock) != self) {
            if (state >= SEALED) {
                ChannelLock.NODE_THREAD.setVolatile(node, null);
                return;
            }
            LockSupport.park(lock);
            if (Thread.interrupted()) {
                ChannelLock.NODE_THREAD.setVolatile(node, null);
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    // ================================================================
    //  PUT
    // ================================================================

    @Override
    public boolean put(Object value) {
        if (rf != null) return putXf(value);

        if (!putLock.spinAcquire(64)) {
            ChannelLock.Node node = putLock.enqueue();
            if ((long) TAIL.getAcquire(this) - (long) HEAD.getAcquire(this) < capacity || state != OPEN)
                putLock.wake();
            parkLoop(node, putLock);
            if ((Thread) ChannelLock.OWNER.getVolatile(putLock) != Thread.currentThread())
                return false;
        }

        while (true) {
            if (state != OPEN) {
                putLock.unlock();
                return false;
            }
            long t = tail;
            long h = (long) HEAD.getAcquire(this);
            if (t - h < capacity) {
                buffer[(int)(t & mask)] = value;
                TAIL.setVolatile(this, t + 1);
                long currentHead = (long) HEAD.getVolatile(this);
                int itemsAfter = (int)(t + 1 - currentHead);
                putLock.unlock();
                if (itemsAfter == 1) takeLock.wake();
                if (itemsAfter < capacity) putLock.wake();
                return true;
            }
            // Buffer full — release, enqueue, Dekker, park
            putLock.unlock();
            ChannelLock.Node node = putLock.enqueue();
            if ((long) TAIL.getAcquire(this) - (long) HEAD.getAcquire(this) < capacity || state != OPEN)
                putLock.wake();
            parkLoop(node, putLock);
            if ((Thread) ChannelLock.OWNER.getVolatile(putLock) != Thread.currentThread())
                return false;
        }
    }

    // ================================================================
    //  PUT — transducer path
    // ================================================================

    private boolean putXf(Object value) {
        if (state >= SEALED) return false;

        if (!putLock.spinAcquire(64)) {
            ChannelLock.Node node = putLock.enqueue();
            if ((long) TAIL.getAcquire(this) - (long) HEAD.getAcquire(this) < capacity || state != OPEN)
                putLock.wake();
            parkLoop(node, putLock);
            if ((Thread) ChannelLock.OWNER.getVolatile(putLock) != Thread.currentThread())
                return false;
        }

        if (state >= SEALED) {
            putLock.unlock();
            return false;
        }

        try {
            Object result = rf.invoke(this, value);
            if (RT.isReduced(result)) {
                rf.invoke(this); // flush
                putLock.unlock();
                throw new UnsupportedOperationException("Seal not yet implemented");
            }
        } catch (Throwable t) {
            putLock.unlock();
            throw t;
        }

        long currentTail = (long) TAIL.getVolatile(this);
        long currentHead = (long) HEAD.getVolatile(this);
        int itemsAfter = (int)(currentTail - currentHead);
        putLock.unlock();
        if (itemsAfter < capacity) putLock.wake();
        return true;
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() {
        if (!takeLock.spinAcquire(64)) {
            ChannelLock.Node node = takeLock.enqueue();
            if ((long) TAIL.getAcquire(this) - (long) HEAD.getAcquire(this) > 0 || state >= SEALED)
                takeLock.wake();
            parkLoop(node, takeLock);
            if ((Thread) ChannelLock.OWNER.getVolatile(takeLock) != Thread.currentThread())
                return IChannel.CANCELLED;
        }

        while (true) {
            if (this.state == CANCELLED) {
                takeLock.unlock();
                return IChannel.CANCELLED;
            }
            long h = head;
            long t = (long) TAIL.getAcquire(this);
            if (t - h > 0) {
                Object value = buffer[(int)(h & mask)];
                buffer[(int)(h & mask)] = null;
                HEAD.setVolatile(this, h + 1);
                long currentTail = (long) TAIL.getVolatile(this);
                int itemsAfter = (int)(currentTail - (h + 1));
                takeLock.unlock();
                if (itemsAfter > 0 && state != CANCELLED) takeLock.wake();
                if (itemsAfter == capacity - 1) putLock.wake();
                return value;
            }
            if (this.state >= SEALED) {
                takeLock.unlock();
                return IChannel.CANCELLED;
            }
            // Buffer empty, open — release, enqueue, Dekker, park
            takeLock.unlock();
            ChannelLock.Node node = takeLock.enqueue();
            if ((long) TAIL.getAcquire(this) - (long) HEAD.getAcquire(this) > 0 || state >= SEALED)
                takeLock.wake();
            parkLoop(node, takeLock);
            if ((Thread) ChannelLock.OWNER.getVolatile(takeLock) != Thread.currentThread())
                return IChannel.CANCELLED;
        }
    }

    // ================================================================
    //  GET-NOW / PUT-NOW direct (caller holds lock)
    // ================================================================

    /**
     * Non-blocking take assuming the caller holds takeLock.
     * Returns the value (including null) if available, {@code CANCELLED}
     * if sealed+drained/cancelled, or {@code UNAVAILABLE} if the buffer
     * is empty.
     * <p>
     * Does NOT release the lock or signal. Caller handles unlock + wake.
     */
    Object getNowDirect() {
        long h = head;
        long t = (long) TAIL.getAcquire(this);

        if (t - h > 0 && state != CANCELLED) {
            Object value = buffer[(int)(h & mask)];
            buffer[(int)(h & mask)] = null;
            HEAD.setVolatile(this, h + 1);
            return value;
        }

        if (state >= SEALED) return IChannel.CANCELLED;
        return IChannel.UNAVAILABLE;
    }

    /**
     * Non-blocking put assuming the caller holds putLock.
     * Returns the number of items after put if successful, {@code PUT_CLOSED}
     * if closed, or {@code PUT_FULL} if the buffer is full.
     * <p>
     * Does NOT release the lock or signal. Caller handles unlock + wake.
     */
    static final int PUT_CLOSED = -1;
    static final int PUT_FULL = -2;

    int putNowDirect(Object value) {
        if (state != OPEN) return PUT_CLOSED;

        long t = tail;
        long h = (long) HEAD.getAcquire(this);

        if (t - h < capacity) {
            buffer[(int)(t & mask)] = value;
            TAIL.setVolatile(this, t + 1);
            return (int)(t + 1 - (long) HEAD.getVolatile(this));
        }

        return PUT_FULL;
    }

    // ================================================================
    //  ALT TAKE
    // ================================================================

    @Override
    public Object altTake(ChannelRef ref) {
        if (ref.get() != null) return IChannel.UNAVAILABLE;

        // Spin briefly — lock is never held while parked.
        int spin = 0;
        while (!takeLock.tryAcquire()) {
            if (ref.get() != null) return IChannel.UNAVAILABLE;
            if (++spin > 64) Thread.yield(); else Thread.onSpinWait();
        }

        if (ref.get() != null) {
            takeLock.unlock();
            return IChannel.UNAVAILABLE;
        }

        Object result = getNowDirect();

        if (result != IChannel.UNAVAILABLE && result != IChannel.CANCELLED) {
            // Got a value — compute signals.
            long currentTail = (long) TAIL.getVolatile(this);
            long currentHead = (long) HEAD.getVolatile(this);
            int itemsAfter = (int)(currentTail - currentHead);
            takeLock.unlock();
            if (itemsAfter > 0 && state != CANCELLED) takeLock.wake();
            if (itemsAfter == capacity - 1) putLock.wake();
            ref.claim(this);
            return result;
        }

        if (result == IChannel.CANCELLED) {
            takeLock.unlock();
            return IChannel.CANCELLED;
        }

        // UNAVAILABLE — enqueue AltNode and unlock
        takeLock.enqueueAlt(ref);
        takeLock.unlock();
        return IChannel.UNAVAILABLE;
    }

    // ================================================================
    //  ALT PUT
    // ================================================================

    @Override
    public boolean altPut(Object value, ChannelRef ref) {
        if (ref.get() != null) return false;

        // Spin briefly — lock is never held while parked.
        int spin = 0;
        while (!putLock.tryAcquire()) {
            if (ref.get() != null) return false;
            if (++spin > 64) Thread.yield(); else Thread.onSpinWait();
        }

        if (ref.get() != null) {
            putLock.unlock();
            return false;
        }

        int result = putNowDirect(value);

        if (result >= 0) {
            // Put succeeded — compute signals.
            putLock.unlock();
            if (result == 1) takeLock.wake();
            if (result < capacity) putLock.wake();
            ref.claim(this);
            return true;
        }

        if (result == PUT_CLOSED) {
            putLock.unlock();
            return false;
        }

        // PUT_FULL — enqueue AltNode and unlock
        putLock.enqueueAlt(ref);
        putLock.unlock();
        return false;
    }

    // ================================================================
    //  Lifecycle
    // ================================================================

    @Override
    public boolean cancel(String msg) {
        if (this.state == CANCELLED) return false;
        this.state = CANCELLED;
        VarHandle.fullFence();
        putLock.wakeAll();
        takeLock.wakeAll();
        return true;
    }

    @Override
    public boolean seal() {
        if (this.state != OPEN) return false;
        this.state = SEALED;
        VarHandle.fullFence();
        putLock.wakeAll();
        takeLock.wakeAll();
        return true;
    }

    @Override
    public boolean isCancelled() {
        return this.state == CANCELLED;
    }

    @Override
    public boolean isSealed() {
        return this.state >= SEALED;
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
        return (int) ((long) TAIL.getVolatile(this) - (long) HEAD.getVolatile(this));
    }

    @Override
    public double saturation() {
        return (double) count() / capacity;
    }
}
