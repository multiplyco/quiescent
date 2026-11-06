package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Bounded channel backed by a ring buffer with separate put/take locks.
 * <p>
 * Uses {@link RelayLock} for FIFO mutual exclusion and cross-lock
 * coordination (suspend/resume). The lock is held while suspended —
 * this is safe because producers and consumers use separate locks.
 * <p>
 * Cross-lock signaling uses a Dekker-style protocol: the producer
 * publishes its put sequence then reads the published take sequence;
 * the consumer publishes its take sequence then reads the published put
 * sequence. The consumer only signals the producer when the buffer was
 * observably full. The ordering argument must include the intervening
 * lock exchanges as well as the cursor accesses. The SIGNALED sentinel in
 * RelayLock handles the residual race between the Dekker check and the park.
 * <p>
 * Buffer writes are accumulated via {@link #putAccumulate} without
 * advancing publishedPutSequence. A single volatile publication flushes
 * all accumulated values at the end of {@link #put}, or earlier if
 * backpressure forces a mid-batch flush while combining queued puts.
 * This batches volatile writes and gives consumers a
 * larger batch to drain per wake-up.
 * <p>
 * Sealing joins the put queue. Once it owns the put lock, it records the
 * final producer sequence and publishes SEALED. That sequence is a persistent
 * end marker, outside the ring, so sealing needs no free buffer slot.
 */
public class BoundedChannel implements IChannel, IBuffered {

    static final int OPEN = 0;
    static final int SEALED = 1;

    /** A queue boundary: a combiner must hand ownership to the sealing caller. */
    private static final class SealNode extends RelayLock.Node {
        SealNode(Thread thread) {
            super(thread);
        }
    }

    static final VarHandle PUBLISHED_PUT_SEQUENCE;
    static final VarHandle PUBLISHED_TAKE_SEQUENCE;
    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PUBLISHED_PUT_SEQUENCE = lookup.findVarHandle(
                BoundedChannel.class, "publishedPutSequence", long.class);
            PUBLISHED_TAKE_SEQUENCE = lookup.findVarHandle(
                BoundedChannel.class, "publishedTakeSequence", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Locks ----
    final RelayLock putLock;
    final RelayLock takeLock;

    // ---- Producer-side ----
    // Published production boundary; consumers may read values before it.
    volatile long publishedPutSequence;

    // ---- Padding between producer and shared ----
    @SuppressWarnings("unused") private long pp01, pp02, pp03, pp04, pp05, pp06, pp07, pp08;

    // ---- Shared coordination ----
    volatile int state;
    // Written once under putLock, before publishing state = SEALED.
    // Consumers read it only after acquiring that state publication.
    long sealedSequence;

    // ---- Padding between shared and consumer ----
    @SuppressWarnings("unused") private long sp01, sp02, sp03, sp04, sp05, sp06, sp07, sp08;

    // ---- Consumer-side ----
    // Published consumption boundary; producers may reuse slots before it.
    volatile long publishedTakeSequence;

    // ---- Buffer ----
    final Object[] buffer;
    final int capacity;
    final int mask;

    // ---- Put-side state (putLock only) ----
    boolean putInterrupted;     // interrupt captured during putAccumulate
    long putSequence;           // working position, includes unpublished writes

    // ================================================================
    //  Constructors
    // ================================================================

    public BoundedChannel(int requestedSize) {
        if (requestedSize < 1)
            throw new IllegalArgumentException("Buffer size must be >= 1, got " + requestedSize);
        this.capacity = nextPowerOf2(requestedSize);
        this.mask = capacity - 1;
        this.buffer = new Object[capacity];
        this.putLock = new RelayLock();
        this.takeLock = new RelayLock();
    }

    private static int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return Integer.highestOneBit(n - 1) << 1;
    }

    // ================================================================
    //  PUT
    // ================================================================

    /**
     * Write one value to the ring buffer without advancing publishedPutSequence.
     * Flushes (publishes putSequence, resumes takeLock) and suspends only
     * when the buffer is full, including while combining queued puts.
     * Called under putLock.
     * <p>
     * {@code putSequence} must be initialized before the first call.
     *
     * @return true if written, false only when {@code state != OPEN}
     */
    private boolean putAccumulate(Object value) {
        while (true) {
            if (state != OPEN) return false;
            long takeSequence = (long) PUBLISHED_TAKE_SEQUENCE.getAcquire(this);
            if (putSequence - takeSequence < capacity) {
                buffer[(int)(putSequence & mask)] = value;
                putSequence++;
                return true;
            }
            // Buffer full — flush accumulated writes so consumers can drain
            PUBLISHED_PUT_SEQUENCE.setVolatile(this, putSequence);
            takeLock.resume();
            // Dekker: re-read the published take sequence after publishing puts.
            // If the consumer already drained, skip the park.
            takeSequence = (long) PUBLISHED_TAKE_SEQUENCE.getAcquire(this);
            if (putSequence - takeSequence < capacity) continue;
            if (putLock.suspend()) {
                putInterrupted = true;
            }
        }
    }

    @Override
    public boolean put(Object value) throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();

        RelayLock.Node node = new RelayLock.Node(Thread.currentThread());
        node.value = value;
        putLock.acquire(node);

        if (node.combined) return true;

        putInterrupted = false;
        putSequence = publishedPutSequence;

        boolean ok = putAccumulate(value);

        RelayLock.Node last = node;
        if (ok) {
            last = putCombine(node);
        }

        // Flush accumulated writes (commit only — defer consumer notification)
        boolean flushed = putSequence != publishedPutSequence;
        if (flushed) {
            PUBLISHED_PUT_SEQUENCE.setVolatile(this, putSequence);
        }

        // Scratch fields belong to putLock's owner. Snapshot everything the
        // outgoing caller needs before a successor can reset those fields.
        boolean interrupted = putInterrupted;
        last.release();

        // Notify consumers after release — successor parks on signal
        // immediately (buffer still full), gets woken by consumer side
        if (flushed) {
            takeLock.resume();
        }

        if (interrupted) Thread.currentThread().interrupt();
        return ok;
    }

    /**
     * Combine waiting successors. Each value goes through
     * {@link #putAccumulate} — the same path as the owner's value.
     * Parks on backpressure (consumer drains, combiner continues),
     * amortizing lock acquisition across the batch.
     */
    private RelayLock.Node putCombine(RelayLock.Node node) {
        RelayLock.Node current = node;

        while (state == OPEN) {
            Object succObj = current.state;
            if (!(succObj instanceof RelayLock.Node succ)) break;
            if (succ instanceof SealNode) break;

            if (!putAccumulate(succ.value)) break;

            succ.combined = true;
            succ.wake();
            current = succ;
        }

        return current;
    }

    // ================================================================
    //  TAKE
    // ================================================================

    @Override
    public Object take() throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException();
        RelayLock.Node node = new RelayLock.Node(Thread.currentThread());
        takeLock.acquire(node);

        if (node.combined) return node.value;

        boolean interrupted = false;
        while (true) {
            long takeSequence = publishedTakeSequence;
            long observedPutSequence = (long) PUBLISHED_PUT_SEQUENCE.getAcquire(this);
            if (observedPutSequence - takeSequence > 0) {
                Object ownValue = buffer[(int)(takeSequence & mask)];
                buffer[(int)(takeSequence & mask)] = null;

                RelayLock.Node last = takeCombine(node, takeSequence + 1, observedPutSequence);

                if (last == node) PUBLISHED_TAKE_SEQUENCE.setVolatile(this, takeSequence + 1);

                last.release();

                // Dekker: re-read the published put sequence after publishing takes.
                // Signal the producer only if the buffer was full (it might be parked).
                long refreshedPutSequence = (long) PUBLISHED_PUT_SEQUENCE.getAcquire(this);
                if (refreshedPutSequence - takeSequence >= capacity) {
                    putLock.resume();
                }

                if (interrupted) Thread.currentThread().interrupt();
                return ownValue;
            }
            if (state == SEALED) {
                // The empty publishedPutSequence observation may predate sealing. SEALED
                // publishes an immutable final sequence, including every
                // preceding put's flush. Retry if those values remain.
                if (takeSequence != sealedSequence) continue;
                node.release();
                if (interrupted) Thread.currentThread().interrupt();
                return IChannel.SEALED;
            }
            if (takeLock.suspend()) {
                interrupted = true;
            }
        }
    }

    private RelayLock.Node takeCombine(
            RelayLock.Node node, long startTakeSequence, long observedPutSequence) {
        long takeSequence = startTakeSequence;
        RelayLock.Node current = node;

        while (takeSequence < observedPutSequence) {
            Object succObj = current.state;
            if (!(succObj instanceof RelayLock.Node succ)) break;
            succ.value = buffer[(int)(takeSequence & mask)];
            buffer[(int)(takeSequence & mask)] = null;
            takeSequence++;
            succ.combined = true;
            succ.wake();
            current = succ;
        }

        if (takeSequence > startTakeSequence) {
            PUBLISHED_TAKE_SEQUENCE.setVolatile(this, takeSequence);
        }

        return current;
    }

    // ================================================================
    //  Lifecycle
    // ================================================================

    @Override
    public boolean seal() {
        if (state != OPEN) return false;
        RelayLock.Node node = new SealNode(Thread.currentThread());
        putLock.acquire(node);
        try {
            if (state != OPEN) return false;
            // All preceding puts have flushed before handing over ownership.
            sealedSequence = publishedPutSequence;
            state = SEALED;
            // The state write publishes sealedSequence and the preceding output.
            // Queued producers are released by the normal ownership handoff.
            takeLock.resume();
            return true;
        } finally {
            node.release();
        }
    }

    @Override
    public boolean isSealed() {
        return this.state == SEALED;
    }

    // ================================================================
    //  IBuffered
    // ================================================================

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int count() {
        // These observations can span many buffer turnovers. Bound the estimate
        // before narrowing: a large negative difference can otherwise become positive.
        long observedCount = (long) PUBLISHED_PUT_SEQUENCE.getVolatile(this)
            - (long) PUBLISHED_TAKE_SEQUENCE.getVolatile(this);
        return (int) Math.max(0L, Math.min((long) capacity, observedCount));
    }

    @Override
    public double saturation() {
        return (double) count() / capacity;
    }
}
