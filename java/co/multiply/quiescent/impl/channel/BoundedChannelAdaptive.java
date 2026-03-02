package co.multiply.quiescent.impl.channel;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Adaptive bounded channel that biases toward a lock-free fast path
 * for single-owner scenarios and irreversibly upgrades to a two-lock
 * path when contention is detected.
 * <p>
 * Optimized to remove shared AtomicInteger count and instead use
 * separated putCount and takeCount to prevent cache line bouncing
 * in single-threaded fast paths.
 */
public class BoundedChannelAdaptive implements IChannel, IBuffered {

    private static final int OPEN = 0;
    private static final int SEALED = 1;
    private static final int CANCELLED = 2;

    private static final Thread CONTENDED = new Thread("CONTENDED-SENTINEL");

    // ---- Producer-side fields ----
    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();
    private volatile Thread producerOwner;
    private volatile int putFastActive;
    private volatile long putCount;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared fields ----
    private volatile int state;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side fields ----
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private volatile Thread consumerOwner;
    private volatile int takeFastActive;
    private volatile long takeCount;

    // ---- Buffer ----
    private final Object[] buffer;
    private final int capacity;
    private final int mask;

    public BoundedChannelAdaptive(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
    }

    public BoundedChannelAdaptive(int requestedSize, boolean padded) {
        this(requestedSize);
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
            return putContended(value);
        
        Thread self = Thread.currentThread();
        if (owner == self) {
            this.putFastActive = 1;
            if (this.producerOwner == self) {
                return putFast(value);
            }
            this.putFastActive = 0;
            return putContended(value);
        }
        return putSlow(value, self);
    }

    private boolean putFast(Object value) {
        long p = this.putCount;
        if (p - this.takeCount >= capacity) {
            this.putFastActive = 0;
            return putFastPark(value);
        }
        
        if (this.state != OPEN) {
            this.putFastActive = 0;
            return false;
        }

        buffer[(int)(p & mask)] = value;
        this.putCount = p + 1;
        this.putFastActive = 0;

        if (p == this.takeCount) {
            signalNotEmpty();
        }
        return true;
    }

    private boolean putFastPark(Object value) {
        boolean wasEmpty = false;
        putLock.lock();
        try {
            while (this.putCount - this.takeCount >= capacity) {
                if (this.state != OPEN) return false;
                notFull.await();
            }
            if (this.state != OPEN) return false;
            
            long p = this.putCount;
            buffer[(int)(p & mask)] = value;
            this.putCount = p + 1;
            
            if (p + 1 - this.takeCount < capacity) {
                notFull.signal();
            }
            if (p == this.takeCount) {
                wasEmpty = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            putLock.unlock();
        }
        if (wasEmpty) signalNotEmpty();
        return true;
    }

    private boolean putSlow(Object value, Thread self) {
        boolean wasEmpty = false;
        putLock.lock();
        try {
            Thread owner = this.producerOwner;

            if (owner == null) {
                this.producerOwner = self;
            } else if (owner != CONTENDED) {
                this.producerOwner = CONTENDED;
                spinOnFastActive(true);
            }

            while (this.putCount - this.takeCount >= capacity) {
                if (this.state != OPEN) return false;
                notFull.await();
            }
            if (this.state != OPEN) return false;
            
            long p = this.putCount;
            buffer[(int)(p & mask)] = value;
            this.putCount = p + 1;
            
            if (p + 1 - this.takeCount < capacity) {
                notFull.signal();
            }
            if (p == this.takeCount) {
                wasEmpty = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            putLock.unlock();
        }
        if (wasEmpty) signalNotEmpty();
        return true;
    }

    private boolean putContended(Object value) {
        boolean wasEmpty = false;
        putLock.lock();
        try {
            while (this.putCount - this.takeCount >= capacity) {
                if (this.state != OPEN) return false;
                notFull.await();
            }
            if (this.state != OPEN) return false;
            
            long p = this.putCount;
            buffer[(int)(p & mask)] = value;
            this.putCount = p + 1;
            
            if (p + 1 - this.takeCount < capacity) {
                notFull.signal();
            }
            if (p == this.takeCount) {
                wasEmpty = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            putLock.unlock();
        }
        if (wasEmpty) signalNotEmpty();
        return true;
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() {
        Thread owner = this.consumerOwner;
        if (owner == CONTENDED)
            return takeContended();
        
        Thread self = Thread.currentThread();
        if (owner == self) {
            this.takeFastActive = 1;
            if (this.consumerOwner == self) {
                return takeFast();
            }
            this.takeFastActive = 0;
            return takeContended();
        }
        return takeSlow(self);
    }

    private Object takeFast() {
        long t = this.takeCount;

        if (t == this.putCount) {
            this.takeFastActive = 0;
            return takeFastPark();
        }

        if (this.state == CANCELLED) {
            this.takeFastActive = 0;
            return IChannel.CANCELLED;
        }

        Object value = buffer[(int)(t & mask)];
        buffer[(int)(t & mask)] = null;

        this.takeCount = t + 1;
        this.takeFastActive = 0;

        if (this.putCount - t == capacity) {
            signalNotFull();
        }
        return value;
    }

    private Object takeFastPark() {
        Object value;
        boolean wasFull = false;
        takeLock.lock();
        try {
            while (this.takeCount == this.putCount) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            
            long t = this.takeCount;
            value = buffer[(int)(t & mask)];
            buffer[(int)(t & mask)] = null;
            this.takeCount = t + 1;
            
            if (this.putCount - (t + 1) > 0) {
                notEmpty.signal();
            }
            if (this.putCount - t == capacity) {
                wasFull = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IChannel.CANCELLED;
        } finally {
            takeLock.unlock();
        }
        if (wasFull) signalNotFull();
        return value;
    }

    private Object takeSlow(Thread self) {
        Object value;
        boolean wasFull = false;
        takeLock.lock();
        try {
            Thread owner = this.consumerOwner;

            if (owner == null) {
                this.consumerOwner = self;
            } else if (owner != CONTENDED) {
                this.consumerOwner = CONTENDED;
                spinOnFastActive(false);
            }

            while (this.takeCount == this.putCount) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            
            long t = this.takeCount;
            value = buffer[(int)(t & mask)];
            buffer[(int)(t & mask)] = null;
            this.takeCount = t + 1;
            
            if (this.putCount - (t + 1) > 0) {
                notEmpty.signal();
            }
            if (this.putCount - t == capacity) {
                wasFull = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IChannel.CANCELLED;
        } finally {
            takeLock.unlock();
        }
        if (wasFull) signalNotFull();
        return value;
    }

    private Object takeContended() {
        Object value;
        boolean wasFull = false;
        takeLock.lock();
        try {
            while (this.takeCount == this.putCount) {
                if (this.state >= SEALED) return IChannel.CANCELLED;
                notEmpty.await();
            }
            
            long t = this.takeCount;
            value = buffer[(int)(t & mask)];
            buffer[(int)(t & mask)] = null;
            this.takeCount = t + 1;
            
            if (this.putCount - (t + 1) > 0) {
                notEmpty.signal();
            }
            if (this.putCount - t == capacity) {
                wasFull = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IChannel.CANCELLED;
        } finally {
            takeLock.unlock();
        }
        if (wasFull) signalNotFull();
        return value;
    }

    // ================================================================
    //  Fast-active spin + Cross-lock signaling
    // ================================================================

    private void spinOnFastActive(boolean isProducer) {
        for (int i = 0; i < 1024; i++) {
            if (isProducer ? this.putFastActive == 0 : this.takeFastActive == 0) return;
            Thread.onSpinWait();
        }
        while (isProducer ? this.putFastActive != 0 : this.takeFastActive != 0) {
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
        return (int) (this.putCount - this.takeCount);
    }

    @Override
    public double saturation() {
        return (double) count() / capacity;
    }
}
