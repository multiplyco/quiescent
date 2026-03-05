package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Claim cell for alt operations. One ChannelRef is created per alt take/put
 * attempt and shared across all channels in the alt set.
 * <p>
 * The {@code channel} field starts null (unclaimed). The first channel to
 * CAS it from null to itself wins the claim. All other channels see the
 * non-null value and skip the stale AltNode in their queues.
 */
public class ChannelRef {

    static final VarHandle CHANNEL;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            CHANNEL = lookup.findVarHandle(ChannelRef.class, "channel", IChannel.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    volatile IChannel channel;

    /**
     * CAS from null to the given channel. Returns true if this claim won.
     */
    public boolean claim(IChannel ch) {
        return CHANNEL.compareAndSet(this, null, ch);
    }

    /**
     * Read the claimed channel (may be null if unclaimed).
     */
    public IChannel get() {
        return (IChannel) CHANNEL.getVolatile(this);
    }
}
