package co.multiply.quiescent.impl.channel;

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;

/**
 * Adaptive bounded channel with transducer support.
 * <p>
 * Producers are serialized through {@code putLock} (inherited from the
 * base class) to protect stateful transducer state — no separate xfLock
 * is needed. The transducer's step function calls {@link #putDirect(Object)}
 * which writes directly to the buffer under the lock. The consumer side
 * retains the adaptive fast path from the superclass.
 *
 * @see AbstractBoundedChannelAdaptive
 */
public class BoundedChannelAdaptiveXf extends AbstractBoundedChannelAdaptive {

    private final IFn rf;

    public BoundedChannelAdaptiveXf(int requestedSize, IFn xf) {
        this(requestedSize, xf, false);
    }

    public BoundedChannelAdaptiveXf(int requestedSize, IFn xf, boolean padded) {
        super(requestedSize);

        AFn baseRf = new AFn() {
            public Object invoke(Object acc) {
                return acc;
            }
            public Object invoke(Object acc, Object val) {
                int c = putDirect(val);
                if (c == 0) signalNotEmpty();
                return acc;
            }
        };
        this.rf = (IFn) xf.invoke(baseRf);
    }

    @Override
    public boolean put(Object value) {
        if (state >= SEALED) return false;
        putLock.lock();
        try {
            if (state >= SEALED) return false;
            Object result = rf.invoke(this, value);
            if (RT.isReduced(result)) {
                rf.invoke(this); // flush
                throw new UnsupportedOperationException("Seal not yet implemented");
            }
        } finally {
            putLock.unlock();
        }
        return true;
    }
}
