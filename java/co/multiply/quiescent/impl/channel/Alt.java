package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Alt provides a way to take (or put) from the first of several channels
 * that's ready. Implements {@link IChannel}, so it composes naturally.
 * <p>
 * Two order modes:
 * <ul>
 *   <li>{@code :prio} — always tries channels in given order</li>
 *   <li>{@code :fair} — round-robin offset, incremented per operation</li>
 * </ul>
 * Two termination modes:
 * <ul>
 *   <li>{@code :all} (default) — done when all channels are closed; skips closed</li>
 *   <li>{@code :any} — done when any channel is closed; short-circuits</li>
 * </ul>
 * <p>
 * <b>WIP</b> — stub implementation, pending alt protocol design.
 */
public class Alt implements IChannel {

    static final VarHandle OFFSET;

    static {
        try {
            OFFSET = MethodHandles.lookup()
                .findVarHandle(Alt.class, "offset", long.class);
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

    @Override
    public Object take() throws InterruptedException {
        int len = channels.length;
        int start = startIndex();
        for (int i = 0; i < len; i++) {
            int idx = (start + i) % len;
            IChannel ch = channels[idx];
            if (ch.isCancelled()) {
                if (any) return IChannel.CANCELLED;
                continue;
            }
            return ch.take();
        }
        return IChannel.CANCELLED;
    }

    @Override
    public boolean put(Object value) throws InterruptedException {
        int len = channels.length;
        int start = startIndex();
        for (int i = 0; i < len; i++) {
            int idx = (start + i) % len;
            IChannel ch = channels[idx];
            if (ch.isSealed() || ch.isCancelled()) continue;
            return ch.put(value);
        }
        return false;
    }

    @Override
    public boolean cancel(String msg) {
        boolean did = false;
        for (IChannel ch : channels) {
            if (ch.cancel(msg)) did = true;
        }
        return did;
    }

    @Override
    public boolean seal() {
        boolean did = false;
        for (IChannel ch : channels) {
            if (ch.seal()) did = true;
        }
        return did;
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
