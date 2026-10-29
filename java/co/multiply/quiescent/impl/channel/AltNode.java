package co.multiply.quiescent.impl.channel;

class AltNode extends Node {
    final Alt.AltClaim claim;
    final Alt alt;
    final int idx;
    boolean spawned;

    AltNode(Thread thread, Alt.AltClaim claim, Alt alt, int idx) {
        super(thread);
        this.claim = claim;
        this.alt = alt;
        this.idx = idx;
    }

    @Override
    void onContention() {
        if (!spawned && idx + 1 < alt.channels.length && !claim.isClaimed()) {
            alt.spawnVT(idx + 1, claim);
            spawned = true;
        }
    }

    void execute() {
        BoundedChannel ch = (BoundedChannel) alt.channels[idx];
        while (true) {
            if (claim.isClaimed()) { this.release(); return; }
            if (ch.isCancelled()) {
                this.release();
                alt.exitDone(idx, claim, spawned);
                return;
            }

            long h = ch.head;
            long t = (long) BoundedChannel.TAIL.getAcquire(ch);
            if (t - h > 0) {
                if (claim.tryClaim()) {
                    Object val = ch.takeOneItem(h, this);
                    claim.deliver(val);
                } else {
                    this.release();
                }
                return;
            }

            if (ch.isSealed()) {
                this.release();
                alt.exitDone(idx, claim, spawned);
                return;
            }

            if (!spawned && idx + 1 < alt.channels.length && !claim.isClaimed()) {
                alt.spawnVT(idx + 1, claim);
                spawned = true;
            }
            ch.takeLock.suspend();
        }
    }
}
