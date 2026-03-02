package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Adaptive bounded channel that biases toward a lock-free fast path
 * for single-owner scenarios and irreversibly upgrades to a two-lock
 * path when contention is detected.
 * <p>
 * Each side (producer/consumer) independently tracks an owner thread:
 * <ul>
 *   <li>{@code null} — unclaimed; first thread claims ownership under the lock.</li>
 *   <li>{@code Thread} — that thread uses the lock-free fast path.</li>
 *   <li>{@code CONTENDED} — all threads use the locked path (irreversible).</li>
 * </ul>
 * <p>
 * The fast path performs: one volatile read (owner), one volatile read
 * (count), one array write, one atomic increment (count). When the buffer
 * is full/empty, the fast-path thread clears its active flag and parks
 * via the lock's Condition.
 * <p>
 * Transition safety: a volatile {@code putFastActive} flag brackets the
 * fast-path write. The upgrading thread, under the lock, spins on this
 * flag (bounded — one array write + one atomic op). After the spin,
 * {@code tail} is visible via the happens-before chain through {@code count}.
 */
public class BoundedChannelAdaptive implements IChannel, IBuffered {

    private static final int OPEN = 0;
    private static final int SEALED = 1;
    private static final int CANCELLED = 2;

    /** Sentinel indicating the side has been upgraded to the locked path. */
    private static final Thread CONTENDED = new Thread("CONTENDED-SENTINEL");

    // ---- VarHandles for volatile field access ----

    private static final VarHandle PRODUCER_OWNER;
    private static final VarHandle PUT_FAST_ACTIVE;
    private static final VarHandle CONSUMER_OWNER;
    private static final VarHandle TAKE_FAST_ACTIVE;
    private static final VarHandle STATE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            PRODUCER_OWNER  = l.findVarHandle(BoundedChannelAdaptive.class, "producerOwner", Thread.class);
            PUT_FAST_ACTIVE = l.findVarHandle(BoundedChannelAdaptive.class, "putFastActive", int.class);
            CONSUMER_OWNER  = l.findVarHandle(BoundedChannelAdaptive.class, "consumerOwner", Thread.class);
            TAKE_FAST_ACTIVE = l.findVarHandle(BoundedChannelAdaptive.class, "takeFastActive", int.class);
            STATE           = l.findVarHandle(BoundedChannelAdaptive.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Producer-side fields ----
    // Cache-line: putLock, notFull, producerOwner, putFastActive, tail

    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();
    @SuppressWarnings("unused") private volatile Thread producerOwner; // null → Thread → CONTENDED
    @SuppressWarnings("unused") private volatile int putFastActive;    // 0 or 1
    private long tail;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared fields ----

    private final AtomicInteger count = new AtomicInteger();
    @SuppressWarnings("unused") private volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side fields ----
    // Cache-line: takeLock, notEmpty, consumerOwner, takeFastActive, head

    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    @SuppressWarnings("unused") private volatile Thread consumerOwner; // null → Thread → CONTENDED
    @SuppressWarnings("unused") private volatile int takeFastActive;   // 0 or 1
    private long head;

    // ---- Buffer ----

    private final Object[] buffer;
    private final int capacity;
    private final int mask;

    // ---- Constructor ----

    public BoundedChannelAdaptive(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
    }

    public BoundedChannelAdaptive(int requestedSize, boolean padded) {
        this(requestedSize); // padded flag accepted for API compat, ignored
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  PUT
    // ================================================================

    @Override
    public boolean put(Object value) {
        Thread owner = (Thread) PRODUCER_OWNER.getVolatile(this);
        if (owner == CONTENDED)
            return putContended(value);
        Thread self = Thread.currentThread();
        if (owner == self)
            return putFast(value);
        return putSlow(value, self);
    }

    /**
     * Lock-free fast path — only the owner thread reaches here.
     * Brackets the buffer write with putFastActive so the upgrading
     * thread can spin-wait for completion.
     */
    private boolean putFast(Object value) {
        final AtomicInteger count = this.count;

        // Set active flag before touching buffer
        PUT_FAST_ACTIVE.setVolatile(this, 1);

        // Check capacity — if full, clear flag and park
        if (count.get() >= capacity) {
            PUT_FAST_ACTIVE.setVolatile(this, 0);
            return putFastPark(value);
        }

        // Check lifecycle
        if ((int) STATE.getVolatile(this) != OPEN) {
            PUT_FAST_ACTIVE.setVolatile(this, 0);
            return false;
        }

        // Write value — single-producer, plain store is safe
        buffer[(int)(tail++ & mask)] = value;

        int c = count.getAndIncrement();

        // Clear active flag — release the write above
        PUT_FAST_ACTIVE.setVolatile(this, 0);

        // Cross-lock signal: buffer went from empty to non-empty
        if (c == 0) signalNotEmpty();

        return true;
    }

    /**
     * Fast-path thread detected buffer full. Clear putFastActive BEFORE
     * acquiring putLock (prevents deadlock: upgrading thread holds putLock
     * and spins on putFastActive). Then park via Condition.await() under
     * the lock, same as the locked path.
     */
    private boolean putFastPark(Object value) {
        final AtomicInteger count = this.count;
        int c;
        putLock.lock();
        try {
            while (count.get() >= capacity) {
                if ((int) STATE.getVolatile(this) != OPEN) return false;
                notFull.await();
            }
            if ((int) STATE.getVolatile(this) != OPEN) return false;
            buffer[(int)(tail++ & mask)] = value;
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            putLock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return true;
    }

    /**
     * Slow path: first-time or contention-detecting entry.
     * Always operates under putLock.
     */
    private boolean putSlow(Object value, Thread self) {
        final AtomicInteger count = this.count;
        int c;
        putLock.lock();
        try {
            // Re-read owner under lock to resolve races
            Thread owner = (Thread) PRODUCER_OWNER.getVolatile(this);

            if (owner == null) {
                // First thread — claim ownership
                PRODUCER_OWNER.setVolatile(this, self);
            } else if (owner != CONTENDED) {
                // Different thread — upgrade to contended
                PRODUCER_OWNER.setVolatile(this, CONTENDED);
                // Spin-wait for fast-path thread to finish its in-flight op
                spinOnFastActive(PUT_FAST_ACTIVE);
            }
            // else: already CONTENDED, proceed under lock

            // putCore: standard locked put logic
            while (count.get() >= capacity) {
                if ((int) STATE.getVolatile(this) != OPEN) return false;
                notFull.await();
            }
            if ((int) STATE.getVolatile(this) != OPEN) return false;
            buffer[(int)(tail++ & mask)] = value;
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            putLock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return true;
    }

    /**
     * Post-upgrade locked path — same as BoundedChannelLocked.put().
     */
    private boolean putContended(Object value) {
        final AtomicInteger count = this.count;
        int c;
        putLock.lock();
        try {
            while (count.get() >= capacity) {
                if ((int) STATE.getVolatile(this) != OPEN) return false;
                notFull.await();
            }
            if ((int) STATE.getVolatile(this) != OPEN) return false;
            buffer[(int)(tail++ & mask)] = value;
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            putLock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return true;
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() {
        Thread owner = (Thread) CONSUMER_OWNER.getVolatile(this);
        if (owner == CONTENDED)
            return takeContended();
        Thread self = Thread.currentThread();
        if (owner == self)
            return takeFast();
        return takeSlow(self);
    }

    /**
     * Lock-free fast path — only the owner thread reaches here.
     */
    private Object takeFast() {
        final AtomicInteger count = this.count;

        TAKE_FAST_ACTIVE.setVolatile(this, 1);

        if (count.get() == 0) {
            TAKE_FAST_ACTIVE.setVolatile(this, 0);
            return takeFastPark();
        }

        if ((int) STATE.getVolatile(this) == CANCELLED) {
            TAKE_FAST_ACTIVE.setVolatile(this, 0);
            return IChannel.CANCELLED;
        }

        Object value = buffer[(int)(head & mask)];
        buffer[(int)(head++ & mask)] = null;

        int c = count.getAndDecrement();

        TAKE_FAST_ACTIVE.setVolatile(this, 0);

        if (c == capacity) signalNotFull();

        return value;
    }

    /**
     * Fast-path thread detected buffer empty. Clear takeFastActive BEFORE
     * acquiring takeLock to prevent deadlock with upgrading thread.
     */
    private Object takeFastPark() {
        final AtomicInteger count = this.count;
        int c;
        Object value;
        takeLock.lock();
        try {
            while (count.get() == 0) {
                if ((int) STATE.getVolatile(this) >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = count.getAndDecrement();
            if (c > 1)
                notEmpty.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IChannel.CANCELLED;
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) signalNotFull();
        return value;
    }

    /**
     * Slow path: first-time or contention-detecting entry.
     */
    private Object takeSlow(Thread self) {
        final AtomicInteger count = this.count;
        int c;
        Object value;
        takeLock.lock();
        try {
            Thread owner = (Thread) CONSUMER_OWNER.getVolatile(this);

            if (owner == null) {
                CONSUMER_OWNER.setVolatile(this, self);
            } else if (owner != CONTENDED) {
                CONSUMER_OWNER.setVolatile(this, CONTENDED);
                spinOnFastActive(TAKE_FAST_ACTIVE);
            }

            while (count.get() == 0) {
                if ((int) STATE.getVolatile(this) >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = count.getAndDecrement();
            if (c > 1)
                notEmpty.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IChannel.CANCELLED;
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) signalNotFull();
        return value;
    }

    /**
     * Post-upgrade locked path — same as BoundedChannelLocked.take().
     */
    private Object takeContended() {
        final AtomicInteger count = this.count;
        int c;
        Object value;
        takeLock.lock();
        try {
            while (count.get() == 0) {
                if ((int) STATE.getVolatile(this) >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = count.getAndDecrement();
            if (c > 1)
                notEmpty.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IChannel.CANCELLED;
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) signalNotFull();
        return value;
    }

    // ================================================================
    //  Fast-active spin + Cross-lock signaling
    // ================================================================

    /**
     * Spin-wait (bounded) for the fast-path thread to clear its active flag.
     * Called by the upgrading thread while holding the lock. The fast-path
     * operation is at most one array write + one atomic increment, so this
     * spin is extremely short.
     */
    private void spinOnFastActive(VarHandle fastActiveHandle) {
        // Bounded spin — fast-path op is nanoseconds
        for (int i = 0; i < 1024; i++) {
            if ((int) fastActiveHandle.getVolatile(this) == 0) return;
            Thread.onSpinWait();
        }
        // Should never take this long, but be safe
        while ((int) fastActiveHandle.getVolatile(this) != 0) {
            Thread.onSpinWait();
        }
    }

    private void signalNotEmpty() {
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    private void signalNotFull() {
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    // ================================================================
    //  Lifecycle
    // ================================================================

    @Override
    public boolean cancel(String msg) {
        // Lock ordering: putLock → takeLock
        putLock.lock();
        try {
            takeLock.lock();
            try {
                if ((int) STATE.getVolatile(this) == CANCELLED) return false;
                STATE.setVolatile(this, CANCELLED);
                notEmpty.signalAll();
            } finally {
                takeLock.unlock();
            }
            notFull.signalAll();
        } finally {
            putLock.unlock();
        }
        return true;
    }

    @Override
    public boolean seal() {
        putLock.lock();
        try {
            if ((int) STATE.getVolatile(this) != OPEN) return false;
            STATE.setVolatile(this, SEALED);
            notFull.signalAll();
        } finally {
            putLock.unlock();
        }
        // Wake consumers so they can detect sealed + empty
        takeLock.lock();
        try {
            notEmpty.signalAll();
        } finally {
            takeLock.unlock();
        }
        return true;
    }

    @Override
    public boolean isCancelled() {
        return (int) STATE.getVolatile(this) == CANCELLED;
    }

    @Override
    public boolean isSealed() {
        return (int) STATE.getVolatile(this) >= SEALED;
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
        return count.get();
    }

    @Override
    public double saturation() {
        return (double) count.get() / capacity;
    }
}
