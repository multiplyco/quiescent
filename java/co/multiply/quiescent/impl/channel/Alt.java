package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

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
 * Take uses VT cascade: one virtual thread per channel, spawned lazily
 * at each park point. An {@link AltClaim} CAS ensures exactly-once
 * delivery. Dead VTs (claim already won) release their lock and exit.
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

    // ================================================================
    //  AltClaim — shared coordination cell for one Alt operation
    // ================================================================

    static class AltClaim {
        static final Object PENDING = new Object();
        static final VarHandle CLAIMED;
        static final VarHandle REMAINING;

        static {
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                CLAIMED = lookup.findVarHandle(AltClaim.class, "claimed", int.class);
                REMAINING = lookup.findVarHandle(AltClaim.class, "remaining", int.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        volatile int claimed;            // 0 = open, 1 = claimed
        volatile Object result = PENDING;
        final Thread callerThread;
        volatile int remaining;          // countdown: channels not yet exhausted

        AltClaim(Thread callerThread, int channelCount) {
            this.callerThread = callerThread;
            this.remaining = channelCount;
        }

        boolean tryClaim() {
            return CLAIMED.compareAndSet(this, 0, 1);
        }

        boolean isClaimed() {
            return claimed != 0;
        }

        void deliver(Object value) {
            result = value;
            LockSupport.unpark(callerThread);
        }

        void channelDone() {
            int r = (int) REMAINING.getAndAdd(this, -1) - 1;
            if (r == 0 && tryClaim()) {
                deliver(IChannel.CANCELLED);
            }
        }
    }

    // ================================================================
    //  Fields
    // ================================================================

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
    //  TAKE (VT cascade)
    // ================================================================

    @Override
    public Object take() throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();
        AltClaim claim = new AltClaim(Thread.currentThread(), channels.length);
        spawnVT(0, claim);

        boolean interrupted = false;
        while (claim.result == AltClaim.PENDING) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }

        // Wake any dead VTs suspended on empty channels
        for (IChannel ch : channels)
            if (ch instanceof BoundedChannel bc) bc.takeLock.resume();

        if (interrupted) Thread.currentThread().interrupt();
        return claim.result;
    }

    void spawnVT(int idx, AltClaim claim) {
        Thread.startVirtualThread(() -> handleChannel(idx, claim));
    }

    void exitDone(int idx, AltClaim claim, boolean spawned) {
        if (!spawned && idx + 1 < channels.length && !claim.isClaimed()) {
            spawnVT(idx + 1, claim);
        }
        if (any) {
            if (claim.tryClaim()) claim.deliver(IChannel.CANCELLED);
        } else {
            claim.channelDone();
        }
    }

    private void handleChannel(int idx, AltClaim claim) {
        BoundedChannel ch = (BoundedChannel) channels[idx];
        AltNode node = new AltNode(Thread.currentThread(), claim, this, idx);
        ch.takeLock.acquire(node);
        Thread.interrupted();
        if (node.combined) { exitDone(idx, claim, node.spawned); return; }
        if (claim.isClaimed()) { node.release(); return; }
        ch.takeLock.owner = node;
        node.execute();
    }

    // ================================================================
    //  PUT (stub — M4)
    // ================================================================

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

    // ================================================================
    //  Lifecycle
    // ================================================================

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
