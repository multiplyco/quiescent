package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
 * Transition safety: a volatile {@code putFastActive} flag brackets the
 * fast-path write. The owner sets it to 1 and re-reads {@code producerOwner}
 * (Dekker handshake) before entering the fast path. The upgrading thread,
 * under the lock, spins on this flag after setting {@code CONTENDED}.
 */
public class BoundedChannelAdaptive implements IChannel, IBuffered {

    private static final int OPEN = 0;
    private static final int SEALED = 1;
    private static final int CANCELLED = 2;

    /** Sentinel indicating the side has been upgraded to the locked path. */
    private static final Thread CONTENDED = new Thread("CONTENDED-SENTINEL");

    private static final VarHandle COUNT;
    static {
        try {
            COUNT = MethodHandles.lookup().findVarHandle(BoundedChannelAdaptive.class, "count", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Producer-side fields ----
    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();
    private volatile Thread producerOwner;
    private volatile int putFastActive;
    private long tail;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared coordination ----
    @SuppressWarnings("unused") private volatile int count;
    private volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side fields ----
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private volatile Thread consumerOwner;
    private volatile int takeFastActive;
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
        this(requestedSize); // padded flag accepted for API compat
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
        Thread owner = this.producerOwner;
        if (owner == CONTENDED)
            return putLocked(value);

        Thread self = Thread.currentThread();
        if (owner == self) {
            // Dekker handshake: set flag, re-check owner
            this.putFastActive = 1;
            if (this.producerOwner == self) {
                return putFast(value);
            }
            // Upgraded between first read and re-check
            this.putFastActive = 0;
            return putLocked(value);
        }
        return putSlow(value, self);
    }

    /**
     * Lock-free fast path — only the owner thread reaches here.
     * Caller has already set putFastActive = 1.
     */
    private boolean putFast(Object value) {
        if ((int) COUNT.getVolatile(this) >= capacity) {
            this.putFastActive = 0;
            return putFastPark(value);
        }

        if (this.state != OPEN) {
            this.putFastActive = 0;
            return false;
        }

        buffer[(int)(tail++ & mask)] = value;
        int c = (int) COUNT.getAndAdd(this, 1);
        this.putFastActive = 0;

        if (c == 0) signalNotEmpty();
        return true;
    }

    /**
     * Fast-path owner detected buffer full. Flag is already cleared.
     * Acquires putLock to park via Condition — same as the locked path.
     */
    private boolean putFastPark(Object value) {
        int c;
        putLock.lock();
        try {
            while ((int) COUNT.getVolatile(this) >= capacity) {
                if (this.state != OPEN) return false;
                notFull.await();
            }
            if (this.state != OPEN) return false;
            buffer[(int)(tail++ & mask)] = value;
            c = (int) COUNT.getAndAdd(this, 1);
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
     * First use or contention detected. Always under putLock.
     */
    private boolean putSlow(Object value, Thread self) {
        int c;
        putLock.lock();
        try {
            Thread owner = this.producerOwner;
            if (owner == null) {
                this.producerOwner = self;
            } else if (owner != CONTENDED) {
                this.producerOwner = CONTENDED;
                spinOnPutFastActive();
            }

            while ((int) COUNT.getVolatile(this) >= capacity) {
                if (this.state != OPEN) return false;
                notFull.await();
            }
            if (this.state != OPEN) return false;
            buffer[(int)(tail++ & mask)] = value;
            c = (int) COUNT.getAndAdd(this, 1);
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
     * Post-upgrade locked path — identical to BoundedChannelLocked.put().
     */
    private boolean putLocked(Object value) {
        int c;
        putLock.lock();
        try {
            while ((int) COUNT.getVolatile(this) >= capacity) {
                if (this.state != OPEN) return false;
                notFull.await();
            }
            if (this.state != OPEN) return false;
            buffer[(int)(tail++ & mask)] = value;
            c = (int) COUNT.getAndAdd(this, 1);
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
        Thread owner = this.consumerOwner;
        if (owner == CONTENDED)
            return takeLocked();

        Thread self = Thread.currentThread();
        if (owner == self) {
            this.takeFastActive = 1;
            if (this.consumerOwner == self) {
                return takeFast();
            }
            this.takeFastActive = 0;
            return takeLocked();
        }
        return takeSlow(self);
    }

    /**
     * Lock-free fast path — only the owner thread reaches here.
     * Caller has already set takeFastActive = 1.
     */
    private Object takeFast() {
        if ((int) COUNT.getVolatile(this) == 0) {
            this.takeFastActive = 0;
            return takeFastPark();
        }

        if (this.state == CANCELLED) {
            this.takeFastActive = 0;
            return IChannel.CANCELLED;
        }

        Object value = buffer[(int)(head & mask)];
        buffer[(int)(head++ & mask)] = null;
        int c = (int) COUNT.getAndAdd(this, -1);
        this.takeFastActive = 0;

        if (c == capacity) signalNotFull();
        return value;
    }

    /**
     * Fast-path owner detected buffer empty. Flag is already cleared.
     */
    private Object takeFastPark() {
        int c;
        Object value;
        takeLock.lock();
        try {
            while ((int) COUNT.getVolatile(this) == 0) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = (int) COUNT.getAndAdd(this, -1);
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
     * First use or contention detected. Always under takeLock.
     */
    private Object takeSlow(Thread self) {
        int c;
        Object value;
        takeLock.lock();
        try {
            Thread owner = this.consumerOwner;
            if (owner == null) {
                this.consumerOwner = self;
            } else if (owner != CONTENDED) {
                this.consumerOwner = CONTENDED;
                spinOnTakeFastActive();
            }

            while ((int) COUNT.getVolatile(this) == 0) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = (int) COUNT.getAndAdd(this, -1);
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
     * Post-upgrade locked path — identical to BoundedChannelLocked.take().
     */
    private Object takeLocked() {
        int c;
        Object value;
        takeLock.lock();
        try {
            while ((int) COUNT.getVolatile(this) == 0) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = (int) COUNT.getAndAdd(this, -1);
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

    private void spinOnPutFastActive() {
        for (int i = 0; i < 1024; i++) {
            if (this.putFastActive == 0) return;
            Thread.onSpinWait();
        }
        while (this.putFastActive != 0) {
            Thread.yield();
        }
    }

    private void spinOnTakeFastActive() {
        for (int i = 0; i < 1024; i++) {
            if (this.takeFastActive == 0) return;
            Thread.onSpinWait();
        }
        while (this.takeFastActive != 0) {
            Thread.yield();
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
                if (this.state == CANCELLED) return false;
                this.state = CANCELLED;
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
            if (this.state != OPEN) return false;
            this.state = SEALED;
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
        return (int) COUNT.getVolatile(this);
    }

    @Override
    public double saturation() {
        return (double) (int) COUNT.getVolatile(this) / capacity;
    }
}
