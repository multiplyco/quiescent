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
    //  AltNode — node in a channel's lock chain
    // ================================================================

    static class AltNode extends RelayLock.Node {
        final AltClaim claim;

        AltNode(Thread thread, AltClaim claim) {
            super(thread);
            this.claim = claim;
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

    private void spawnVT(int idx, AltClaim claim) {
        Thread.startVirtualThread(() -> handleChannel(idx, claim));
    }

    private boolean cascade(int idx, AltClaim claim, boolean spawned) {
        if (!spawned && idx + 1 < channels.length && !claim.isClaimed()) {
            spawnVT(idx + 1, claim);
        }
        return true;
    }

    private void exitDone(int idx, AltClaim claim, boolean spawned) {
        cascade(idx, claim, spawned);
        if (any) {
            if (claim.tryClaim()) claim.deliver(IChannel.CANCELLED);
        } else {
            claim.channelDone();
        }
    }

    private void handleChannel(int idx, AltClaim claim) {
        BoundedChannel ch = (BoundedChannel) channels[idx];
        AltNode node = new AltNode(Thread.currentThread(), claim);
        boolean spawned = false;

        // ── Enqueue into ch.takeLock chain (inline) ──
        RelayLock.Node prev = (RelayLock.Node) RelayLock.SLOT.getAndSet(ch.takeLock, node);
        Object prevState = RelayLock.STATE.getAndSet(prev, node);
        if (prevState != RelayLock.DONE) {
            spawned = cascade(idx, claim, spawned);
            while (prev.state != RelayLock.DONE && !node.combined) {
                LockSupport.park();
                Thread.interrupted();
            }
        }
        if (node.combined) { exitDone(idx, claim, spawned); return; }
        if (claim.isClaimed()) { node.release(); return; }

        // ── Lock owner ──
        ch.takeLock.owner = node;
        while (true) {
            if (claim.isClaimed()) { node.release(); return; }
            if (ch.state == BoundedChannel.CANCELLED) {
                node.release();
                exitDone(idx, claim, spawned);
                return;
            }

            long h = ch.head;
            long t = (long) BoundedChannel.TAIL.getAcquire(ch);
            if (t - h > 0) {
                if (claim.tryClaim()) {
                    Object val = ch.buffer[(int)(h & ch.mask)];
                    ch.buffer[(int)(h & ch.mask)] = null;
                    BoundedChannel.HEAD.setVolatile(ch, h + 1);
                    node.release();
                    if ((long) BoundedChannel.TAIL.getAcquire(ch) - h >= ch.capacity) {
                        ch.putLock.resume();
                    }
                    claim.deliver(val);
                    return;
                } else {
                    node.release();
                    return;
                }
            }

            if (ch.state >= BoundedChannel.SEALED) {
                node.release();
                exitDone(idx, claim, spawned);
                return;
            }

            spawned = cascade(idx, claim, spawned);
            ch.takeLock.suspend();
        }
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
