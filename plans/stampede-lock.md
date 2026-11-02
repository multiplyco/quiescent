# Stampede Lock

A lock where threads self-organize into a handoff chain using an array
of CAS slots, rather than a traditional linked structure (CLH/MCS).

## Core Idea

Instead of serializing all arrivals through a single atomic (queue tail),
spread arrivals across N slots. Threads then "fold" themselves into a
handoff chain by claiming neighbors. The result is the same sequential
handoff as MCS, but with dramatically reduced contention at arrival time.

## Structure

```
thread_array[0..N]:  AtomicReferenceArray<Thread>
  [0] = head (lock holder, the "crown")
  [1..N] = arrival/staging slots (thread_ref | SENTINEL | null)

int_array[0..N]:     int[] accessed via VarHandle (plain/opaque stores OK)
  breadcrumb values: claimant's slot index (0..N), or -1 when empty

tail_slot:           AtomicReference<Thread>
  parked tail identity (Dekker-guarded)
```

- **thread_array[0]** is distinguished. Whoever holds it holds the lock.
- **thread_array[1..N]** are arrival slots. Threads CAS themselves in.
- **int_array** holds breadcrumb integers, written by claimants, read by claimees.
  Separate from thread_array to avoid mixed types in a single array.
- **tail_slot** is used only when the chain's tail parks during a lull.

The two-array split keeps types homogeneous on the JVM: thread_array is
`AtomicReferenceArray<Thread>`, int_array is plain `int[]` (or
`AtomicIntegerArray`). No boxing, no sentinel-objects-encoding-integers.

## Thread Lifecycle

### Arrival

```
1. Try CAS(thread_array[0], null, self). Succeeded? Head. Enter critical section.
2. Pick random slot i (1..N).
3. Try to land:
   - null: CAS(thread_array[i], null, self).
   - SENTINEL: skip, re-randomize.
   - thread_ref (occupied by A): Barge — CAS(thread_array[i], A, self).
4. CAS failed? Re-randomize (back to step 2).
5. CAS succeeded (barge or empty slot). Check slot 0:
   - Read thread_array[0].
   - Occupied? Slot 0 is held. Continue:
     - If barged: record A as successor, park immediately. Done.
     - If empty slot: continue to Thread Protocol below.
   - Empty? Try to become head:
     a. CAS(thread_array[i], self, null) to vacate slot i.
        - Failed (barged while vacating)? I was displaced, someone has me
          as successor. Continue as claimed thread (check int_array, etc.)
        - Succeeded? Try CAS(thread_array[0], null, self).
          - Succeeded? Head. Enter critical section.
            (If we barged earlier, we already have a successor chain —
            the entire chain is now attached to the head. Process it
            after the critical section.)
          - Failed (someone else grabbed slot 0)? Back to step 2.
```

**Slot 0 recheck prevents deadlock.** Without this check, a thread could
land in slot i just as the head finishes, scans the array (misses the
thread), clears slot 0, and exits. The thread would be stuck in slot i
with nobody to pick it up. The recheck ensures that if slot 0 is empty
after landing, the thread tries to become head rather than waiting forever.

**Barger upgrade.** When a barger successfully grabs slot 0, it becomes head
with a ready-made chain already attached (the displaced thread and its
successors). The entire chain gets processed immediately after the critical
section — no waiting to be picked up by a future head.

**Barging tradeoffs:**

Barging allows an arrival to land in one CAS regardless of array occupancy.
The displaced thread is unaffected — it continues scanning right and checking
int_array[i] as before. It doesn't know or care that it was displaced. When
someone eventually claims the slot (now holding the new arrival),
int_array[i] gets written, and the displaced thread sees it.

Under high load where most slots are occupied, barging avoids the retry loop
of finding an empty slot. Best case: one CAS + park. Without barging, an
arrival under high load might need multiple random probes or a linear scan
before finding a null slot.

Barging can only affect **sub-chains in the array** — threads that haven't
entered the execution pipeline yet. The ordering among these threads is
arbitrary anyway (determined by random slot choice and fold timing). Once the
head picks up a sub-chain and starts handing off, those threads are linked by
internal successor references and cannot be barged. So barging reshuffles
ordering only in the phase where ordering doesn't matter yet.

### Slot State Machine (Two-Array)

The state of slot `i` is split across two arrays:

```
thread_array[i]:   null | thread_ref | SENTINEL
int_array[i]:      -1 (empty) | claimant's slot index (0..N)
```

**thread_array transitions:**
```
null → thread_ref    (new arrival: CAS(thread_array[i], null, self))
thread_ref → thread_ref (barge: CAS(thread_array[i], existing, self))
thread_ref → SENTINEL (claimant: CAS(thread_array[i], target_ref, SENTINEL))
SENTINEL → null      (claimee: CAS(thread_array[i], SENTINEL, null))
```

**int_array transitions:**
```
-1 → index           (claimant writes breadcrumb AFTER CAS-ing to SENTINEL)
index → -1           (claimee reads breadcrumb, then clears)
```

**Claim protocol (A at slot 1 claims B at slot 5):**
```
A: CAS(thread_array[5], B, SENTINEL)               ← lock the slot
A: write int_array[5] = 1                          ← breadcrumb (A's slot index)
A: record B as successor, park

B: (in its main loop) checks int_array[5] != -1    ← "I've been claimed"
B: reads int_array[5] → 1                          ← claimant's slot index
B: writes int_array[5] = -1                        ← clear breadcrumb
B: CAS(thread_array[5], SENTINEL, null)             ← frees slot for reuse
```

**Two signals, two audiences.** The SENTINEL in thread_array and the
breadcrumb in int_array serve different purposes:
- **int_array** is the signal the **claimee** watches. During its main loop
  (scanning for threads to claim, spinning, etc.), the claimee periodically
  checks `int_array[my_slot] != -1`. This is the only read it needs to detect
  that it's been claimed. It never reads thread_array[my_slot].
- **SENTINEL** in thread_array is for **new arrivals**. It locks the slot so
  no arrival can CAS into it while the int_array handshake is in progress.
  The claimee clears SENTINEL only after reading and clearing the breadcrumb.

**SENTINEL prevents int_array clobbering.** Without it, a new arrival could
grab the slot and get claimed by someone else, overwriting the int_array
entry before the original claimee reads it.

**Memory ordering.** The claimant writes int_array *after* the CAS. The
claimee polls int_array and will eventually see the write due to cache
coherence. No formal happens-before is needed — the claimee spins until
int_array is != -1, which is guaranteed to resolve within nanoseconds
in practice. If stronger guarantees are desired, the claimant can use a
release store on int_array, or use AtomicIntegerArray.

### Thread Protocol (After Arrival)

Every non-head thread follows the same loop:

```
1. CAS into thread_array slot (or barge into occupied slot)
2. If barged: already have a successor (the displaced thread). Park immediately.
3. Otherwise, scan right, try to claim someone:
   - CAS(thread_array[target], target_ref, SENTINEL)
   - Succeeded? Write int_array[target] = my slot index. Record successor, park.
   - Failed? Scan further right.
4. Nobody to claim? Check int_array[my_slot] != -1.
   → not -1: I've been claimed!
     Read int_array[my_slot] (claimant's slot index), clear it to -1.
     CAS(thread_array[my_slot], SENTINEL, null) to free the slot.
     (If barged, slot holds the barger's thread_ref — CAS simply fails. No-op.)
     Follow the breadcrumb trail (see below).
   → -1: not claimed yet. Spin/scan, eventually become canonical tail.
5. Repeat steps 3-4: interleave scanning for threads to claim with
   checking int_array[my_slot] for being claimed.
```

The claimee **never reads thread_array[my_slot] to detect claims**. It only
checks int_array[my_slot], giving it one fewer volatile read per loop
iteration. The only time the claimee touches thread_array[my_slot] is
`CAS(SENTINEL, null)` — which succeeds for normal claims (freeing the slot)
and harmlessly fails for barges (slot holds a thread_ref, not SENTINEL).

**One claim per thread.** A thread claims at most one successor. Once you've
claimed someone, you're done — record them and park immediately. You do NOT
need to stay awake to relay information.

### Breadcrumb Trail (Scan Base Propagation)

When a thread is claimed, the claimant sets SENTINEL in thread_array (locking
the slot) then writes a breadcrumb to int_array. The tail of the chain needs
to learn its scan base — the leftmost slot index in the chain. Rather than
relaying this through each intermediate node, the **tail walks the breadcrumb
trail itself**.

After being claimed (int_array[my_slot] becomes != -1), a thread reads
the breadcrumb (claimant's slot index directly), clears int_array to -1,
and does `CAS(thread_array[my_slot], SENTINEL, null)` to free the slot.
(This succeeds for normal claims, harmlessly fails for barges.)

The thread then checks int_array at the claimant's slot. If that's also
not -1, another breadcrumb is waiting — read it, clear it,
CAS(SENTINEL → null), continue. The tail follows the trail until it reaches
a slot whose int_array is -1 (the sub-chain head hasn't been claimed yet).
The last breadcrumb read is the current scan base.

**Example — concurrent claims, B claims C while A claims B:**

(int_array stores slot index directly; -1 = empty)

```
A is at slot 1. B is at slot 3. C is at slot 5.

A claims B:
  CAS(thread_array[3], B, SENTINEL)
  int_array[3] = 1

B claims C:
  CAS(thread_array[5], C, SENTINEL)
  int_array[5] = 3
  B parks immediately (has successor C).

C checks int_array[5] → 3 (not -1, I'm claimed!):
  clears int_array[5] = -1
  CAS(thread_array[5], SENTINEL, null)    ← slot 5 free for reuse

C checks int_array[3] → 1 (not -1, trail continues!):
  clears int_array[3] = -1
  CAS(thread_array[3], SENTINEL, null)    ← slot 3 free for reuse

C checks int_array[1] → -1 (sub-chain head A not yet claimed).
C's scan base = 1. Done.
```

This works regardless of timing:
- **B claims C first, A claims B later:** C reads 3 from int_array[5], frees
  slot 5, checks int_array[3] → -1 (A hasn't claimed B yet). C spins on
  int_array[3]. A claims B, writes int_array[3] = 1. C reads it, frees
  slot 3. Scan base = 1.
- **A claims B first, B claims C later:** int_array[3] already has 1. B CASes
  thread_array[5] to SENTINEL, writes int_array[5] = 3, parks. C reads 3
  from int_array[5], follows to int_array[3], reads 1. Done in one pass.
- **Simultaneous:** Both breadcrumbs present. C walks both in one pass.

**Key property:** Intermediate nodes park immediately after claiming. They
don't relay anything. Only the tail (the thread with no one to claim) walks
the trail. Each slot in the trail is a distinct cache line, so the walk
involves no contention. The tail also frees slots as it walks, making them
available for new arrivals.

**Updating the scan base over time:** The tail may reach the end of the
breadcrumb trail before the sub-chain head has been claimed (int_array at
that slot is still 0). The tail spins on that int_array slot, interleaving
with scanning for new threads to claim. When the sub-chain head eventually
gets claimed (by the slot 0 head, or by another sub-chain's tail), the
int_array entry becomes != -1, and the tail reads the new breadcrumb —
extending its scan base leftward without any coordination from intermediate
nodes.

### Folding (Chain Formation)

Folding emerges from all threads running the protocol above in parallel.

**Right-only rule (including own slot):** A thread can claim any thread in a
slot to the right of its original position, **including its own slot** — if
the slot it previously occupied now contains a new thread, that thread is
eligible. This is critical: without it, the rightmost slot (slot 7) can
never claim anyone, and the array fills with unreachable sub-chain heads.

With this rule, slots recycle: a thread vacates a slot (gets claimed), a new
arrival CASes into it, and that new arrival can be claimed by the thread that
previously occupied it. This prevents the array from filling up and ensures
chains naturally anchor toward the left of the array.

The right-only constraint (including own slot) still prevents cycles — a
thread can only claim slots at its own index or higher, so all edges point
rightward in index space and the dependency graph is always a DAG.

**Scan range:** A thread at slot `i` scans slots `i+1` through N (and its
own slot `i` if it was displaced/claimed and the slot was reused). Slot 0
is never scanned — it's the head's slot. Slot N (rightmost) has an empty
scan range — it cannot claim anyone. It is inert: it waits to be claimed
by a thread to its left, or serves as a barge target for new arrivals.

**Claim semantics:** Thread A claims thread B via the two-array protocol:
A does `CAS(thread_array[B's slot], B, SENTINEL)`. If the CAS fails (another
thread claimed B first), A scans further — int_array is untouched. On
success, A writes `int_array[B's slot] = A's index`, records B as its
successor, and parks immediately (A's own thread_array slot remains occupied
— A is a sub-chain head). B detects the claim by checking int_array (not
thread_array), reads the breadcrumb, clears int_array, frees the slot
(CAS SENTINEL → null), and then follows the breadcrumb trail or scans right
to claim someone else.

**Parallel folding:** Multiple threads fold simultaneously. CAS arbitrates
conflicts (two threads targeting the same slot — one wins, one keeps scanning).

**The array is never empty while a chain exists.** A sub-chain head cannot
be claimed by another array thread (nobody in the sub-chain is to its left,
and folding only goes right). Only the slot 0 head or a tail from a different
sub-chain (to its left) can claim it. After all sub-chains merge, exactly
one unclaimed thread remains in the array — the chain's entry point, waiting
for the head to grab it.

Example (int_array stores slot index directly; -1 = empty):
```
thread_array: [A, D, _, B, _, E, _, C]     (A holds lock at slot 0)
int_array:    [-1,-1,-1,-1,-1,-1,-1,-1]

Round 1 (parallel):
  D(1) claims B(3): CAS(thread_array[3], B, SENTINEL), int_array[3] = 1
  E(5) claims C(7): CAS(thread_array[7], C, SENTINEL), int_array[7] = 5
  D and E park.
  thread_array: [A, D, _, S, _, E, _, S]   (S = SENTINEL)

Round 2 — B and C see int_array != -1, they're claimed:
  B reads int_array[3] → 1, clears to -1, frees slot 3 (SENTINEL → null)
  B scans right → finds E(5), claims E: CAS(thread_array[5], E, SENTINEL),
    int_array[5] = 3. B parks.
  C reads int_array[7] → 5, clears to -1, frees slot 7 (SENTINEL → null)
  C scans right → nothing to the right (scan range: 8+, empty)
  thread_array: [A, D, _, _, _, S, _, _]

  C follows breadcrumb trail:
    int_array[5] → 3, clears to -1, frees slot 5 (SENTINEL → null)
    int_array[3] → -1 (not claimed yet). C spins on int_array[1] (D's slot).
  Chain so far: D → B → E → C (tail)
  D is sub-chain head at slot 1, waiting for head to finish.
  thread_array: [A, D, _, _, _, _, _, _]

Later — A (slot 0) finishes its work, scans right:
  A finds D(1): CAS(thread_array[1], D, SENTINEL), int_array[1] = 0
  C sees int_array[1] != -1 → reads 0. Scan base = 0 (full range).
  C is now the canonical tail with full array visibility.

A writes D to slot 0, unparks D. D is new head.
Full chain: A → D → B → E → C
```

### Sub-chain Resolution

Folding can produce multiple disconnected sub-chains temporarily. This
resolves naturally:

- Each sub-chain's **head** is the leftmost member, still occupying an array slot.
- Each sub-chain's **tail** is scanning for more threads to claim.
- A tail from one sub-chain will see the head of another sub-chain in the
  array and claim it, merging the chains.
- A thread only becomes the *canonical tail* (writes to tail_slot) when it
  has been claimed, has scanned the entire array, and found nothing. This
  means all sub-chains have merged and the array is empty.

**Key invariant:** Only one thread can be the canonical tail at any time,
because it requires the array to be completely empty (besides slot 0). Two
disconnected sub-chains means at least two heads in the array, so at least
one tail will find something to claim.

### Tail Parking (Dekker Protocol)

When the tail has nobody left to claim, it must decide whether to spin or
park. During lulls, spinning is wasteful. The tail uses a Dekker-style
protocol to park safely:

**Tail thread:**
```
1. Spin on array for N iterations
2. Still nobody? Write self to tail_slot    ← "I'm going to sleep"
3. Full fence (VarHandle.fullFence())
4. Re-scan array                            ← Dekker check
5. Found someone?
     → claim them, CAS(tail_slot, self, null), loop
   Found nobody?
     → park()
     → on wake: scan array, claim, CAS(tail_slot, self, null)
```

**New arrival (after CAS-ing into thread_array, whether into null or barge):**
```
1. CAS into thread_array[i]                ← "I'm here" (null→self or barge)
2. If barged: record displaced thread as successor, park. Done.
   (Barging threads park immediately — no Dekker check needed, since they
   don't remain in the array as scannable threads. The displaced thread
   is still active and will handle tail duties if needed.)
3. If landed in null slot:
   Full fence
   Read tail_slot                           ← Dekker check
   If occupied: unpark(tail_thread)
```

**Dekker guarantee:** Both sides write their own flag, then read the other's.
At least one side sees the other. Either:
- Tail sees the arrival → claims it, no park needed
- Arrival sees the tail → unparks it
- Both → harmless extra unpark

**CAS on tail_slot clear:** The tail uses `CAS(tail_slot, self, null)` when
clearing, not a plain write. This avoids clobbering if another thread has
legitimately become the new tail in the interim (shouldn't happen given the
invariant, but defensive).

### Head Behavior

The head does NOT participate in folding while it holds the lock. It does
its main work first, then comes back to hand off.

**The critical section is the coalescing window.** While the head is working,
arriving threads CAS into the array and fold among themselves undisturbed.
By the time the head finishes and scans, the array has had time to settle —
sub-chains have merged, threads have linked up. The head is more likely to
pick up a long chain in a single grab rather than catching threads mid-arrival.

This also covers the light-contention case naturally: if the head was alone
on arrival, by the time it finishes its work a new thread may have appeared.
No wasted setup for the uncontended path.

**Head release protocol:**

```
1. Do main work (critical section)
2. Scan array left-to-right, find first occupied slot (thread_ref, not SENTINEL)
3. Claim it via normal protocol:
   CAS(thread_array[i], target_ref, SENTINEL)
   Write int_array[i] = 0 (slot 0 — signals "you're connected to head")
4. Write claimed thread to slot 0, unpark it → new head
5. Exit
```

The head uses the standard claim protocol (SENTINEL + int_array) so that
the sub-chain's tail, which may be watching int_array at this slot via the
breadcrumb trail, sees the update and can extend its scan base.

If the array is empty, check tail_slot. If a tail is parked there, unpark
it and hand slot 0 to it. If truly nobody is waiting (no array entries, no
tail), clear slot 0 and leave.

Note: the head's CAS on an array slot may fail if a folding thread claims
the same target simultaneously. The head simply scans further right. CAS
arbitrates as usual.

The claimed thread may already have a chain of successors built up from
folding. That entire chain transfers with it — the new head will hand off
to its own successor when it finishes.

**Handoff cost:** Single write to slot 0 + unpark. No contention on the
handoff path (MCS property preserved).

## Properties

**Arrival contention:** O(1) expected. With 8 slots and moderate arrival
rates, CAS collisions on the array are rare. Contrast with single-slot
queues where every arrival serializes.

**No allocation:** No queue nodes are allocated. Each thread stores its
successor on the stack / in a local variable. Scan base propagation uses
the slots themselves (breadcrumb trail) rather than allocated shared objects.

**Ordering:** The chain order is NOT strictly FIFO. It's determined by
array position and fold order. For a mutex, this is fine — you need mutual
exclusion and eventual progress, not fairness. (Starvation freedom depends
on the fold scanning pattern.)

**Handoff:** Fully uncontended. Single write + unpark per handoff.

**Tail overhead:** The canonical tail is either spinning (during bursts)
or Dekker-parked (during lulls). New arrivals pay one extra fence +
tail_slot read, which is cheap.

## API Surface

```java
class StampedeLock {
    void acquire()   // arrival protocol → become head (slot 0)
    void release()   // hand off slot 0 to next in chain (or clear if empty)
    void wake()      // unpark whoever is at slot 0 (external signal)
}
```

**acquire():** Run the arrival protocol. Try slot 0, if occupied pick a
random slot, barge or land, fold into chain, eventually receive slot 0
via handoff. Returns when the caller holds slot 0.

**release():** Scan the array left-to-right, claim the first occupied slot,
write it to slot 0, unpark it. If nobody waiting, clear slot 0.

**wake():** Read thread_array[0], unpark it. No lock acquisition needed.
This is the external signal mechanism — the caller does NOT need to hold
this lock. Trivially:
```java
void wake() {
    Thread t = (Thread) THREAD_ARRAY.getVolatile(this, 0);
    if (t != null) LockSupport.unpark(t);
}
```

## Usage in BoundedChannel

BoundedChannel uses two independent lock instances:

```java
StampedeLock putLock  = new StampedeLock(arraySize);
StampedeLock takeLock = new StampedeLock(arraySize);
```

**Producer (put):**
```java
putLock.acquire();
try {
    while (count >= capacity) {
        if (state != OPEN) return -1;
        LockSupport.park();          // sleep at slot 0, holding lock
    }
    buffer[putIndex] = value;
    count++;
    takeLock.wake();                 // signal consumer: "buffer not empty"
} finally {
    putLock.release();               // hand off to next producer in chain
}
```

**Consumer (take):**
```java
takeLock.acquire();
try {
    while (count == 0) {
        if (state >= SEALED) return CANCELLED;
        LockSupport.park();          // sleep at slot 0, holding lock
    }
    value = buffer[takeIndex];
    count--;
    putLock.wake();                  // signal producer: "buffer not full"
} finally {
    takeLock.release();              // hand off to next consumer in chain
}
```

**Seal/Cancel:**
```java
state = SEALED;
putLock.wake();                      // wake parked producer
takeLock.wake();                     // wake parked consumer
// Each wakes, sees state flag, exits, hands off to next in chain.
// Cancel cascades through the chain automatically via release().
```

**Key simplifications over ReentrantLock + Condition:**
- No Condition objects. The lock holder just parks itself directly.
- No cross-lock acquisition for signaling. `wake()` is a volatile read +
  unpark — the consumer never needs to acquire putLock to signal producers.
- No signalAll(). For seal/cancel, wake the head once. It sees the flag,
  exits, hands off to the next thread, which also sees the flag and exits.
  The chain unwinds itself.
- No reentrancy tracking needed. The lock is never acquired recursively.

## Open Questions

- **Array size tuning.** 8 slots is arbitrary. Larger arrays reduce arrival
  contention with minimal downside: the head's scan is unaffected (it walks
  left-to-right and sub-chains bunch leftward, so it finds the first
  occupant quickly). The tail's breadcrumb walk is proportional to chain
  length, not array size. The main cost of a larger array is the tail's
  scan for new arrivals during lulls — but during lulls the tail is
  Dekker-parked anyway. Under high load, more slots = less contention.
  Best sized up-front to expected concurrency. Adaptive resizing is
  possible but adds a read to every thread's hot path (current array
  size / reference), which is a permanent cost for a rare event.

  For virtual threads, the actual concurrent contention on the array is
  bounded by carrier threads, not total VT count. VTs fold and park
  quickly — a VT arrives, CASes in, claims or gets claimed, and parks
  within a few operations. The only thread that stays active is the
  tail. So the effective arrival contention is "how many carrier threads
  are in the arrival phase simultaneously," which is at most core count.
  An 8-16 slot array may suffice even for VT-heavy workloads.

  A reasonable default heuristic: `0.5 * cores`, rounded up to the next
  power of 2. Needs tuning/benchmarking to confirm.

- **Spin duration before tail parks.** Too short increases park/unpark
  overhead. Too long wastes CPU during lulls. Adaptive backoff?

- **Fairness.** Not strict FIFO. There is an inherent spatial bias:
  threads that land further left in the array tend to become sub-chain
  heads and get picked up by the head first. A latecomer at slot 1 can
  be promoted ahead of a long-waiting chain at slot 3.

  However, the unfairness is bounded and self-correcting. Processing
  sweeps left-to-right in **waves**:

  ```
  Wave 1: head picks leftmost chain
    2 -> 3 -> 4 -> 4 -> 6 -> 7 -> 7 -> 7
  Wave 2: last thread finishes, scans array fresh
    1 -> 2 -> 5 -> 5 -> 8
  ```

  Within a wave, left-biased threads are served first. The rightmost
  slot (7) can only claim same-slot arrivals, so it builds a strictly
  sequential mini-chain — effectively a single-slot MCS queue. Right-side
  threads accumulate longer tails within a wave.

  Between waves, the array resets. Every thread gets a fresh random draw
  at a left position. So fairness is **approximate across waves, left-
  biased within a wave**. No thread is starved — the maximum bypass within
  a wave is bounded by array size.

  Alternatives (left-biased arrival, sequence numbers) reintroduce the
  single contention point that the array was designed to eliminate.

- **Applicability to bounded channels.** Addressed above — two lock instances
  (putLock / takeLock) with `wake()` replacing Condition variables. The
  chain acts as an implicit condition queue.
