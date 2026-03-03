package co.multiply.quiescent.impl.channel;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded channel with two-lock design.
 * <p>
 * Uses separate locks for producers and consumers. Parking happens
 * before slot claiming (via Condition.await()), making operations
 * cleanly interruptible — no slots are "burned" on interruption.
 * <p>
 * An AtomicInteger count bridges the two sides, providing both
 * coordination and memory visibility (happens-before between
 * producer writes and consumer reads). Cross-lock signaling only
 * happens at transitions (empty→non-empty, full→non-full).
 * <p>
 * Trade-offs vs lock-free XADD ({@link BoundedChannel}):
 * <ul>
 *   <li>Pro: flat scaling under high fan-in (many producers).</li>
 *   <li>Pro: clean interruptibility (park is pre-claim).</li>
 *   <li>Pro: stable latency, low variance.</li>
 *   <li>Con: slightly slower at low contention (1P1C).</li>
 * </ul>
 */
public class BoundedChannelLocked implements IChannel, IBuffered {

    private static final int OPEN = 0;
    private static final int SEALED = 1;
    private static final int CANCELLED = 2;

    private final Object[] buffer;
    private final int capacity;
    private final int mask;

    // Producer side — only accessed under putLock
    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();
    private long tail;

    // Consumer side — only accessed under takeLock
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private long head;

    // Shared coordination
    private final AtomicInteger count = new AtomicInteger();

    // Lifecycle
    private volatile int state;

    public BoundedChannelLocked(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
    }

    // Accept padded flag for API compatibility (ignored — lock eliminates contention)
    public BoundedChannelLocked(int requestedSize, boolean padded) {
        this(requestedSize);
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ---- IChannel: Core operations ----

    @Override
    public boolean put(Object value) {
        final AtomicInteger count = this.count;
        int c;
        putLock.lock();
        try {
            while (count.get() >= capacity) {
                if (state != OPEN) return false;
                notFull.await();
            }
            if (state != OPEN) return false;
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

    @Override
    public Object take() {
        final AtomicInteger count = this.count;
        int c;
        Object value;
        takeLock.lock();
        try {
            while (count.get() == 0) {
                if (state >= SEALED) return IChannel.CANCELLED;
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

    // ---- Cross-lock signaling ----

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

    // ---- IChannel: Lifecycle ----

    @Override
    public boolean cancel(String msg) {
        // Lock ordering: putLock → takeLock (consistent everywhere)
        putLock.lock();
        try {
            takeLock.lock();
            try {
                if (state == CANCELLED) return false;
                state = CANCELLED;
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
            if (state != OPEN) return false;
            state = SEALED;
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
        return state == CANCELLED;
    }

    @Override
    public boolean isSealed() {
        return state >= SEALED;
    }

    // ---- IBuffered ----

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
