package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * Alt provides a way to take (or put) from the first of several channels
 * that's ready. It implements {@link IChannel}, so it composes naturally:
 * {@code (alt :prio control-ch (alt :fair data-ch1 data-ch2))}.
 * <p>
 * Two order modes:
 * <ul>
 *   <li>{@code :prio} — always tries channels in given order</li>
 *   <li>{@code :fair} — round-robin offset, incremented per operation</li>
 * </ul>
 * Two termination modes:
 * <ul>
 *   <li>{@code :all} (default) — done when all channels are closed; skips closed channels</li>
 *   <li>{@code :any} — done when any channel is closed; short-circuits on first closed</li>
 * </ul>
 * <p>
 * Alt has no lock of its own — it's a stateless dispatch layer. Each
 * operation distributes across the underlying channels' locks.
 */
public class Alt implements IChannel {

    static final VarHandle OFFSET;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            OFFSET = lookup.findVarHandle(Alt.class, "offset", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    final IChannel[] channels;
    final boolean fair;
    final boolean any;
    volatile long offset;

    public Alt(boolean fair, boolean any, IChannel... channels) {
        if (channels.length == 0)
            throw new IllegalArgumentException("Alt requires at least one channel");
        this.channels = channels.clone();
        this.fair = fair;
        this.any = any;
    }

    private int startIndex() {
        int len = channels.length;
        return fair ? (int) Math.floorMod((long) OFFSET.getAndAdd(this, 1L), len) : 0;
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() throws InterruptedException {
        while (true) {
            ChannelRef ref = new ChannelRef();
            Object val = altTake(ref);

            if (val == IChannel.CANCELLED) return IChannel.CANCELLED;
            if (val != IChannel.UNAVAILABLE) return val;

            // Check if ref was claimed during altTake iteration
            IChannel claimed = ref.get();
            if (claimed != null) {
                Object v = claimed.take();
                if (v != IChannel.CANCELLED || any) return v;
                continue; // :all mode — channel closed, retry
            }

            // All channels empty, no claim — park until woken
            while (ref.get() == null) {
                LockSupport.park(this);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    return IChannel.CANCELLED;
                }
            }

            // Woken — take from winning channel
            Object v = ref.get().take();
            if (v != IChannel.CANCELLED || any) return v;
            // :all mode — channel closed, retry
        }
    }

    // ================================================================
    //  PUT
    // ================================================================

    @Override
    public boolean put(Object value) throws InterruptedException {
        while (true) {
            ChannelRef ref = new ChannelRef();
            boolean put = altPut(value, ref);
            if (put) return true;

            // Check if ref was claimed during altPut iteration
            IChannel claimed = ref.get();
            if (claimed != null) {
                boolean ok = claimed.put(value);
                if (ok || any) return ok;
                continue; // :all mode — channel closed, retry
            }

            // Check if all channels are closed (avoid infinite retry)
            if (allClosed()) return false;

            // All channels full, no claim — park until woken
            while (ref.get() == null) {
                LockSupport.park(this);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            // Woken — put to winning channel
            boolean ok = ref.get().put(value);
            if (ok || any) return ok;
            // :all mode — channel closed, retry
        }
    }

    private boolean allClosed() {
        for (IChannel ch : channels) {
            if (!ch.isSealed()) return false;
        }
        return true;
    }

    // ================================================================
    //  ALT TAKE — iterates channels, used by both take() and composition
    // ================================================================

    @Override
    public Object altTake(ChannelRef ref) {
        if (ref.get() != null) return IChannel.UNAVAILABLE;

        int len = channels.length;
        int start = startIndex();
        int closedCount = 0;

        for (int i = 0; i < len; i++) {
            int idx = (start + i) % len;
            Object val = channels[idx].altTake(ref);

            if (val == IChannel.CANCELLED) {
                if (any) return IChannel.CANCELLED;
                closedCount++;
                continue;
            }
            if (val != IChannel.UNAVAILABLE) return val;
            if (ref.get() != null) return IChannel.UNAVAILABLE;
        }
        return closedCount == len ? IChannel.CANCELLED : IChannel.UNAVAILABLE;
    }

    // ================================================================
    //  ALT PUT — iterates channels, used by both put() and composition
    // ================================================================

    @Override
    public boolean altPut(Object value, ChannelRef ref) {
        if (ref.get() != null) return false;

        int len = channels.length;
        int start = startIndex();
        int closedCount = 0;

        for (int i = 0; i < len; i++) {
            int idx = (start + i) % len;
            IChannel ch = channels[idx];
            boolean put = ch.altPut(value, ref);
            if (put) return true;

            if (ch.isCancelled() || ch.isSealed()) {
                if (any) return false;
                closedCount++;
                continue;
            }
            if (ref.get() != null) return false;
        }
        return false;
    }

    // ================================================================
    //  Lifecycle — delegate to underlying channels
    // ================================================================

    @Override
    public boolean cancel(String msg) {
        boolean any = false;
        for (IChannel ch : channels) {
            if (ch.cancel(msg)) any = true;
        }
        return any;
    }

    @Override
    public boolean seal() {
        boolean any = false;
        for (IChannel ch : channels) {
            if (ch.seal()) any = true;
        }
        return any;
    }

    @Override
    public boolean isCancelled() {
        for (IChannel ch : channels) {
            if (!ch.isCancelled()) return false;
        }
        return true;
    }

    @Override
    public boolean isSealed() {
        for (IChannel ch : channels) {
            if (!ch.isSealed()) return false;
        }
        return true;
    }
}
