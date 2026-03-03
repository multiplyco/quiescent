package co.multiply.quiescent.impl.channel;

/**
 * Adaptive bounded channel with lock-free single-producer fast path.
 * <p>
 * The producer side uses ownership tracking with a Dekker handshake
 * to enable a lock-free fast path when a single thread is producing.
 * When a second producer is detected, the side irreversibly upgrades
 * to the locked path. The consumer side (in the superclass) uses the
 * same adaptive protocol independently.
 *
 * @see AbstractBoundedChannelAdaptive
 */
public class BoundedChannelAdaptive extends AbstractBoundedChannelAdaptive {

    private volatile Thread producerOwner;
    private volatile int putFastActive;

    public BoundedChannelAdaptive(int requestedSize) {
        super(requestedSize);
    }

    public BoundedChannelAdaptive(int requestedSize, boolean padded) {
        super(requestedSize); // padded flag accepted for API compat
    }

    // ================================================================
    //  PUT — adaptive owner-tracking
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
        if ((int) COUNT.getAcquire(this) >= capacity) {
            this.putFastActive = 0;
            return putFastPark(value);
        }

        if (this.state != OPEN) {
            this.putFastActive = 0;
            return false;
        }

        buffer[(int)(tail++ & mask)] = value;
        int c = (int) COUNT.getAndAddRelease(this, 1);
        this.putFastActive = 0;

        if (c == 0) signalNotEmpty();
        return true;
    }

    /**
     * Fast-path owner detected buffer full. Flag is already cleared.
     * Acquires putLock to park via Condition.
     */
    private boolean putFastPark(Object value) {
        int c;
        putLock.lock();
        try {
            c = putDirect(value);
        } finally {
            putLock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return c >= 0;
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

            c = putDirect(value);
        } finally {
            putLock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return c >= 0;
    }

    /**
     * Post-upgrade locked path.
     */
    private boolean putLocked(Object value) {
        int c;
        putLock.lock();
        try {
            c = putDirect(value);
        } finally {
            putLock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return c >= 0;
    }

    private void spinOnPutFastActive() {
        for (int i = 0; i < 1024; i++) {
            if (this.putFastActive == 0) return;
            Thread.onSpinWait();
        }
        while (this.putFastActive != 0) {
            Thread.yield();
        }
    }
}
