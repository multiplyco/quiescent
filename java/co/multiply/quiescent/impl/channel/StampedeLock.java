package co.multiply.quiescent.impl.channel;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.ThreadLocalRandom;

public class StampedeLock {

    static final Thread SENTINEL = new Thread("STAMPEDE-SENTINEL");
    static final int EMPTY = -1;
    static final int SCAN_ROUNDS = 32;
    static final int STRIDE = 16; 

    static final VarHandle INT_ARRAY;
    static final VarHandle THREAD_ARRAY;
    static final VarHandle TAIL_THREAD;
    static final VarHandle TAIL_SLOT;

    static {
        try {
            INT_ARRAY = MethodHandles.arrayElementVarHandle(int[].class);
            THREAD_ARRAY = MethodHandles.arrayElementVarHandle(Thread[].class);
            var lookup = MethodHandles.lookup();
            TAIL_THREAD = lookup.findVarHandle(StampedeLock.class, "tailThread", Thread.class);
            TAIL_SLOT = lookup.findVarHandle(StampedeLock.class, "tailSlot", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    final Thread[] threadArray;
    final int slotCount;

    @SuppressWarnings("unused") private long p01, p02, p03, p04, p05, p06, p07, p08;

    final int[] intArray;

    @SuppressWarnings("unused") private long p11, p12, p13, p14, p15, p16, p17, p18;

    @SuppressWarnings("unused") volatile Thread tailThread;
    @SuppressWarnings("unused") volatile int tailSlot;

    @SuppressWarnings("unused") private long p21, p22, p23, p24, p25, p26, p27, p28;

    public StampedeLock() {
        this(defaultSlotCount());
    }

    public StampedeLock(int slotCount) {
        if (slotCount < 2)
            throw new IllegalArgumentException("slotCount must be >= 2, got " + slotCount);
        if ((slotCount & (slotCount - 1)) != 0)
            throw new IllegalArgumentException("slotCount must be a power of 2, got " + slotCount);
        this.slotCount = slotCount;
        this.threadArray = new Thread[slotCount * STRIDE];
        this.intArray = new int[slotCount * STRIDE];
        Arrays.fill(this.intArray, EMPTY);
    }

    private static int defaultSlotCount() {
        int cores = Runtime.getRuntime().availableProcessors();
        int half = Math.max(cores / 2, 2);
        return Integer.highestOneBit(half - 1) << 1;
    }

    private Thread getThread(int i) {
        return (Thread) THREAD_ARRAY.getVolatile(threadArray, i * STRIDE);
    }

    private void setThread(int i, Thread t) {
        THREAD_ARRAY.setVolatile(threadArray, i * STRIDE, t);
    }

    private boolean casThread(int i, Thread expected, Thread newValue) {
        return THREAD_ARRAY.compareAndSet(threadArray, i * STRIDE, expected, newValue);
    }

    private int getInt(int i) {
        return (int) INT_ARRAY.getVolatile(intArray, i * STRIDE);
    }

    private void setInt(int i, int val) {
        INT_ARRAY.setVolatile(intArray, i * STRIDE, val);
    }

public void wake() {
        Thread t = getThread(0);
        if (t != null && t != SENTINEL) LockSupport.unpark(t);
    }

    public Thread acquire() {
        Thread self = Thread.currentThread();
        if (casThread(0, null, self)) {
            clearStaleSlots(self);
            return null;
        }
        Thread[] successor = new Thread[1];
        boolean interrupted = acquireSlow(self, successor);
        clearStaleSlots(self);
        if (interrupted) Thread.currentThread().interrupt();
        return successor[0];
    }

    /**
     * Clear any stale references to this thread in non-zero slots.
     * When a thread gets slot 0 via release(successor), its original
     * landing slot is not cleared, leaving a stale reference that
     * scanAndHandoff could later find and hand off to a terminated thread.
     */
    private void clearStaleSlots(Thread self) {
        for (int i = 1; i < slotCount; i++) {
            casThread(i, self, null);
        }
    }

    private int randomSlot() {
        int r;
        while ((r = ThreadLocalRandom.current().nextInt() & (slotCount - 1)) == 0);
        return r;
    }

    private boolean acquireSlow(Thread self, Thread[] successor) {
        boolean interrupted = false;

        while (true) {
            int slot = randomSlot();

            Thread displaced = null;
            boolean landed = false;
            
            for (int attempts = 0; attempts < slotCount; attempts++) {
                Thread occupant = getThread(slot);
                if (occupant != SENTINEL && casThread(slot, occupant, self)) {
                    if (occupant != null) displaced = occupant;
                    landed = true;
                    break;
                }
                slot = randomSlot();
            }

            if (!landed) {
                if (casThread(0, null, self)) return interrupted;
                continue;
            }

            if (getThread(0) == null) {
                if (displaced == null && casThread(slot, self, null)) {
                    if (casThread(0, null, self)) {
                        return interrupted;
                    }
                    continue;
                }
                if (casThread(0, null, self)) {
                    casThread(slot, self, null); // best-effort clear
                    successor[0] = displaced;
                    return interrupted;
                }
            }

            Thread tail = (Thread) TAIL_THREAD.getVolatile(this);
            if (tail != null) LockSupport.unpark(tail);

            if (displaced != null) {
                successor[0] = displaced;
                return interrupted | parkInSlot(self); 
            }

            int result = threadProtocol(self, slot, successor);
            if (result < 0) interrupted = true;
            if (Math.abs(result) == 1) continue; 
            return interrupted;
        }
    }

    private int threadProtocol(Thread self, int mySlot, Thread[] successor) {
        int scanRounds = 0;
        boolean isTail = false;
        boolean interrupted = false;

        while (true) {
            int breadcrumb = getInt(mySlot);
            if (breadcrumb != EMPTY) {
                if (isTail) TAIL_THREAD.compareAndSet(this, self, null);
                setInt(mySlot, EMPTY);
                casThread(mySlot, SENTINEL, null);
                followBreadcrumbTrail(breadcrumb);
                return (parkInSlot(self) || interrupted) ? -2 : 0;
            }

            Thread occupant = getThread(mySlot);
            if (occupant == SENTINEL) {
                if (isTail) {
                    TAIL_THREAD.compareAndSet(this, self, null);
                    isTail = false;
                }
                Thread.onSpinWait();
                continue;
            }
            if (occupant != self) {
                if (isTail) TAIL_THREAD.compareAndSet(this, self, null);
                if (getThread(0) == self) return interrupted ? -2 : 0; 
                return (parkInSlot(self) || interrupted) ? -2 : 0; 
            }

            for (int i = mySlot + 1; i < slotCount; i++) {
                Thread target = getThread(i);
                if (target != null && target != SENTINEL) {
                    if (casThread(i, target, SENTINEL)) {
                        if (isTail) TAIL_THREAD.compareAndSet(this, self, null);
                        setInt(i, mySlot);
                        successor[0] = target;
                        return (parkInSlot(self) || interrupted) ? -2 : 0;
                    }
                }
            }

            if (getThread(0) == null) {
                if (isTail) TAIL_THREAD.compareAndSet(this, self, null);
                if (casThread(mySlot, self, null)) {
                    if (casThread(0, null, self)) return interrupted ? -2 : 0;
                    return interrupted ? -1 : 1;
                }
                return (parkInSlot(self) || interrupted) ? -2 : 0;
            }

            if (isTail) {
                LockSupport.park(this);
                TAIL_THREAD.compareAndSet(this, self, null);
                isTail = false;
                scanRounds = 0;
                if (Thread.interrupted()) interrupted = true;
                continue;
            }

            scanRounds++;
            if (scanRounds >= SCAN_ROUNDS) {
                TAIL_SLOT.setVolatile(this, mySlot);
                TAIL_THREAD.setVolatile(this, self);
                isTail = true;
                continue;
            }

            Thread.onSpinWait();
        }
    }

    private boolean parkInSlot(Thread self) {
        boolean interrupted = false;
        while (getThread(0) != self) {
            LockSupport.park(this);
            if (Thread.interrupted()) interrupted = true;
        }
        return interrupted;
    }

    private int followBreadcrumbTrail(int startSlot) {
        int current = startSlot;
        for (;;) {
            int next = getInt(current);
            if (next == EMPTY) return current;
            setInt(current, EMPTY);
            casThread(current, SENTINEL, null);
            current = next;
        }
    }

    public void release(Thread successor) {
        if (successor != null) {
            setThread(0, successor);
            LockSupport.unpark(successor);
            return;
        }

        if (scanAndHandoff()) return;

        Thread tail = (Thread) TAIL_THREAD.getVolatile(this);
        if (tail != null) {
            if (TAIL_THREAD.compareAndSet(this, tail, null)) {
                int slot = (int) TAIL_SLOT.getVolatile(this);
                if (casThread(slot, tail, null)) {
                    setThread(0, tail);
                    LockSupport.unpark(tail);
                    return;
                }
                LockSupport.unpark(tail);
                if (scanAndHandoff()) return;
            }
        }

        setThread(0, null);
    }

    private boolean scanAndHandoff() {
        for (;;) {
            boolean sawCandidate = false;
            for (int i = 1; i < slotCount; i++) {
                Thread target = getThread(i);
                if (target != null && target != SENTINEL) {
                    if (casThread(i, target, null)) {
                        setThread(0, target);
                        LockSupport.unpark(target);
                        return true;
                    }
                    sawCandidate = true;
                }
            }
            if (!sawCandidate) return false;
        }
    }
}
