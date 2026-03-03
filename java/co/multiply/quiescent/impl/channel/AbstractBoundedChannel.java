package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base for bounded channels. Owns the adaptive take
 * implementation and the put-side infrastructure (lock, condition,
 * buffer). Subclasses implement {@link #put(Object)}:
 * <ul>
 *   <li>{@link BoundedChannel} — owner-tracking with Dekker handshake
 *       for lock-free single-producer fast path.</li>
 *   <li>{@link BoundedChannelXf} — transducer support; acquires
 *       {@code putLock} directly and calls {@link #putDirect(Object)}.</li>
 * </ul>
 * <p>
 * Both producer and consumer sides use an adaptive ownership protocol:
 * single-thread fast path, irreversible upgrade to locked on contention.
 */
public abstract class AbstractBoundedChannel implements IChannel, IBuffered {

    static final int OPEN = 0;
    static final int SEALED = 1;
    static final int CANCELLED = 2;

    /** Sentinel indicating the side has been upgraded to the locked path. */
    static final Thread CONTENDED = new Thread("CONTENDED-SENTINEL");

    static final VarHandle COUNT;
    static {
        try {
            COUNT = MethodHandles.lookup().findVarHandle(
                AbstractBoundedChannel.class, "count", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Producer-side fields (subclasses access putLock, notFull, tail) ----
    final ReentrantLock putLock = new ReentrantLock();
    final Condition notFull = putLock.newCondition();
    long tail;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared coordination ----
    @SuppressWarnings("unused") volatile int count;
    volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side fields (private — take logic lives here) ----
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private volatile Thread consumerOwner;
    private volatile int takeFastActive;
    private long head;

    // ---- Buffer ----
    final Object[] buffer;
    final int capacity;
    final int mask;

    // ---- Constructor ----

    AbstractBoundedChannel(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  PUT — shared infrastructure
    // ================================================================

    /**
     * Writes a value assuming the caller holds {@code putLock}.
     * Waits on {@code notFull} if the buffer is full, cascade-signals
     * other parked producers, and cross-lock signals consumers when
     * the buffer transitions from empty to non-empty.
     *
     * @return the count before increment ({@code >= 0}), or {@code -1}
     *         if the channel is sealed/cancelled or the thread was interrupted.
     */
    int putDirect(Object value) {
        try {
            while ((int) COUNT.getAcquire(this) >= capacity) {
                if (state != OPEN) return -1;
                notFull.await();
            }
            if (state != OPEN) return -1;
            buffer[(int)(tail++ & mask)] = value;
            int c = (int) COUNT.getAndAddRelease(this, 1);
            if (c + 1 < capacity)
                notFull.signal();
            return c;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        }
    }

    void signalNotEmpty() {
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
    //  TAKE — full adaptive implementation
    // ================================================================

    @Override
    public final Object take() {
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

    private Object takeFast() {
        if ((int) COUNT.getAcquire(this) == 0) {
            this.takeFastActive = 0;
            return takeFastPark();
        }

        if (this.state == CANCELLED) {
            this.takeFastActive = 0;
            return IChannel.CANCELLED;
        }

        Object value = buffer[(int)(head & mask)];
        buffer[(int)(head++ & mask)] = null;
        int c = (int) COUNT.getAndAddRelease(this, -1);
        this.takeFastActive = 0;

        if (c == capacity) signalNotFull();
        return value;
    }

    private Object takeFastPark() {
        int c;
        Object value;
        takeLock.lock();
        try {
            while ((int) COUNT.getAcquire(this) == 0) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            if (this.state == CANCELLED) return IChannel.CANCELLED;
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = (int) COUNT.getAndAddRelease(this, -1);
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

            while ((int) COUNT.getAcquire(this) == 0) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            if (this.state == CANCELLED) return IChannel.CANCELLED;
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = (int) COUNT.getAndAddRelease(this, -1);
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

    private Object takeLocked() {
        int c;
        Object value;
        takeLock.lock();
        try {
            while ((int) COUNT.getAcquire(this) == 0) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            if (this.state == CANCELLED) return IChannel.CANCELLED;
            value = buffer[(int)(head & mask)];
            buffer[(int)(head++ & mask)] = null;
            c = (int) COUNT.getAndAddRelease(this, -1);
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

    private void spinOnTakeFastActive() {
        for (int i = 0; i < 1024; i++) {
            if (this.takeFastActive == 0) return;
            Thread.onSpinWait();
        }
        while (this.takeFastActive != 0) {
            Thread.yield();
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
