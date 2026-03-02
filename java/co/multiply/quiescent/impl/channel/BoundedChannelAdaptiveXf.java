package co.multiply.quiescent.impl.channel;

import java.util.concurrent.locks.ReentrantLock;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;

/**
 * Adaptive bounded channel with transducer support.
 * <p>
 * Uses a separate ReentrantLock to serialize transducer execution,
 * preserving the inner channel's adaptive locking semantics.
 */
public class BoundedChannelAdaptiveXf extends BoundedChannelAdaptive {

    private final ReentrantLock xfLock = new ReentrantLock();
    private final IFn rf;

    public BoundedChannelAdaptiveXf(int requestedSize, IFn xf) {
        this(requestedSize, xf, false);
    }

    public BoundedChannelAdaptiveXf(int requestedSize, IFn xf, boolean padded) {
        super(requestedSize, padded);

        AFn baseRf = new AFn() {
            public Object invoke(Object acc) {
                return acc;
            }
            public Object invoke(Object acc, Object val) {
                BoundedChannelAdaptiveXf.super.put(val);
                return acc;
            }
        };
        this.rf = (IFn) xf.invoke(baseRf);
    }

    @Override
    public boolean put(Object value) {
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
