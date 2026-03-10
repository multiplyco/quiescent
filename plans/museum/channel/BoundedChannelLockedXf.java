package co.multiply.quiescent.impl.channel;

import java.util.concurrent.locks.ReentrantLock;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;

/**
 * Bounded channel with two-lock design and transducer support.
 * <p>
 * Uses a separate ReentrantLock to serialize transducer execution,
 * preserving the inner channel's state and locking semantics.
 */
public class BoundedChannelLockedXf extends BoundedChannelLocked {

    private final ReentrantLock xfLock = new ReentrantLock();
    private final IFn rf;

    public BoundedChannelLockedXf(int requestedSize, IFn xf) {
        this(requestedSize, xf, false);
    }

    public BoundedChannelLockedXf(int requestedSize, IFn xf, boolean padded) {
        super(requestedSize, padded);

        AFn baseRf = new AFn() {
            public Object invoke(Object acc) {
                return acc;
            }
            public Object invoke(Object acc, Object val) {
                BoundedChannelLockedXf.super.put(val);
                return acc;
            }
        };
        this.rf = (IFn) xf.invoke(baseRf);
    }

    @Override
    public boolean put(Object value) throws InterruptedException {
        if (isSealed() || isCancelled()) return false;
        xfLock.lock();
        try {
            if (isSealed() || isCancelled()) return false;
            Object result = rf.invoke(this, value);
            if (RT.isReduced(result)) {
                rf.invoke(this); // flush
                throw new UnsupportedOperationException("Seal not yet implemented");
            }
        } finally {
            xfLock.unlock();
        }
        return true;
    }
}
