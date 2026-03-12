# RelayLock

A FIFO handoff lock with integrated cross-lock signaling. Three exchange
(getAndSet) operations per lock cycle. No CAS, no spinning, no queue
structure.

## Core Idea

A single atomic slot holds a node. Each thread creates a node, swaps it
into the slot via getAndSet, and receives back the predecessor's node.
The predecessor's node is a one-shot rendezvous point: both threads do a
getAndSet on its state field to coordinate handoff.

The state field has three states: `null` (initial), a Node reference
(successor registered), or `DONE` (owner finished). The getAndSet return
value tells each side what happened.

## Structure

```
slot:  volatile Node   — the single coordination point (VarHandle getAndSet)
ref:   volatile Node   — cross-lock signal field (SIGNALED | waiter node)
```

```java
class Node {
    final Thread thread;       // owning thread (null for sentinel nodes)
    volatile Object state;     // null | Node (successor) | DONE
    Object value;              // payload for combining (put value or take result)
    volatile boolean combined; // set by combiner before waking

    void wake() {
        LockSupport.unpark(thread);
    }

    void release() {
        dispatch(STATE.getAndSet(this, DONE));
    }
}
```

The `value` and `combined` fields support combining (see below). A lock
holder can walk the successor chain and perform operations on behalf of
waiting threads, eliminating context switches.

## Thread Lifecycle

### Full cycle: 3 exchanges

```
1. getAndSet(slot, myNode)           → predecessor's node
2. getAndSet(predecessor.state, myNode)  → null (park) or DONE (proceed)
3. getAndSet(myNode.state, DONE)         → null (no successor) or Node (wake it)
```

### Step by step

**Acquire:**

```
1. Create Node(Thread.currentThread())
2. prev = SLOT.getAndSet(this, myNode)
3. prevState = STATE.getAndSet(prev, myNode)
4. If prevState == DONE: predecessor finished, proceed (owner)
5. If prevState == null: predecessor still working, park
   (loop: while prev.state != DONE && !myNode.combined)
```

The park loop checks two conditions. The first (`prev.state != DONE`) is
the normal handoff. The second (`!myNode.combined`) is for combining:
the lock holder may have already performed this thread's operation and
set `combined = true`, waking it directly.

**Release (after critical section):**

```
1. successor = STATE.getAndSet(myNode, DONE)
2. dispatch(successor):
   - null: nobody waiting, done
   - Node: wake it (unpark its thread)
```

### Initialization

The constructor creates a sentinel node with `state = DONE` and sets it
as the initial slot value. The first thread to acquire swaps this
sentinel out, sees DONE on the state exchange, and proceeds immediately.

```java
public RelayLock() {
    Node initial = new Node(null);
    initial.state = DONE;
    this.slot = initial;
}
```

### Why this works

The state field is a rendezvous between exactly two threads: the current
owner (A) and the next arrival (B). Both do getAndSet on the same field.
The return value tells them who arrived first:

**Case 1 — B arrives first:**

- B does getAndSet(A.state, nodeB) → gets null → parks
- A finishes, does getAndSet(A.state, DONE) → gets nodeB → wakes B

**Case 2 — A finishes first:**

- A does getAndSet(A.state, DONE) → gets null → walks away
- B does getAndSet(A.state, nodeB) → gets DONE → proceeds immediately

**Case 3 — simultaneous:**

- getAndSet is atomic; one of the above cases applies

No lost wakeups. No Dekker protocol needed. The atomicity of the exchange
handles the coordination that would otherwise require two ordered memory
accesses (Dekker) or a compare-and-swap.

### FIFO ordering

```
A arrives:  slot.getAndSet(nodeA) → sentinel  → A is owner
B arrives:  slot.getAndSet(nodeB) → nodeA     → B waits on A
C arrives:  slot.getAndSet(nodeC) → nodeB     → C waits on B
```

Each thread only knows its predecessor. The chain is implicit:
A → B → C. Handoff follows arrival order.

### No cleanup needed

There is no head pointer, no queue to drain. After A finishes and hands
off to B, A's node remains in no structure — it's just garbage. B's node
is in the slot until C swaps it out (or indefinitely if nobody comes).

A node left in the slot doesn't go stale. If B finishes and nobody arrives
for an hour, the node still works: eventual arrival C swaps it out, does
getAndSet on its state field, sees DONE, and proceeds immediately.

## Machine cost

On x86, getAndSet compiles to XCHG (single instruction, implicit lock
prefix). Three exchanges per lock cycle = three machine instructions for
the synchronization path.

On ARM, getAndSet is a LDAXR/STLXR pair (load-linked/store-conditional).
Still the minimal atomic primitive — no comparison step like CAS.

### Per-lock-cycle cost

| Operation                        | Instruction (x86) | Count |
|----------------------------------|-------------------|-------|
| Enqueue (swap slot)              | XCHG              | 1     |
| Acquire (swap predecessor state) | XCHG              | 1     |
| Release (swap own state)         | XCHG              | 1     |
| **Total**                        |                   | **3** |

Plus one object allocation (Node with four fields) per acquisition.

### Comparison to alternatives

**CLH lock:** getAndSet on tail + spin on predecessor's flag. Similar
enqueue cost, but spins instead of parking. No park/unpark overhead in
the fast case, but burns CPU. RelayLock trades spinning for one extra
exchange + park/unpark.

**MCS lock:** getAndSet on tail + write next pointer. Has a window where
the next pointer isn't visible yet, requiring a short spin. RelayLock
avoids this because the successor writes its node into the predecessor's
state (step 2), so the link is established synchronously.

**ReentrantLock (AQS):** CAS on state + CLH-variant queue with multiple
CAS operations for enqueue/dequeue. Heavier per-operation cost, more
complex state machine.

## Combining

When a thread acquires the lock and performs its operation, it can peek at
the successor chain before releasing. If successors are already queued and
the operation can be done on their behalf, the lock holder combines them:

```
1. Lock holder finishes own operation
2. Check: is current.state a Node (successor waiting)?
3. If yes, and there's capacity:
   - Perform successor's operation using successor.value
   - Set successor.combined = true  (volatile write → happens-before)
   - Wake successor (it sees combined, skips critical section)
   - Advance to successor, repeat
4. Release the last node in the combined chain
```

The combined thread wakes in `awaitPredecessor`, sees `combined == true`,
and returns immediately — it never enters the critical section.

### Why combining helps

Without combining, N queued threads require N context switches: each
thread wakes, does its operation, releases, wakes the next. With
combining, one thread does all N operations in a tight loop and then
releases once. The successor threads wake only to find their work already
done.

This is especially effective under contention: the more threads queue up,
the more the lock holder can batch. The cost per operation drops because
the lock/unlock overhead is amortized across the batch.

### Combining on put

The lock holder writes its own value to the ring buffer, then walks
successors. For each, it writes `succ.value` to the buffer and advances
the tail. After the batch, it does a single volatile tail write and
resumes the take lock if the buffer was empty.

```java
while(newTail -h<capacity &&state ==OPEN){
Object succObj = current.state;
    if(!(succObj instanceof
Node succ))break;
buffer[(int)(newTail &mask)]=succ.value;
newTail++;
succ.combined =true;
        succ.

wake();

current =succ;
}
        if(newTail >t){
        TAIL.

setVolatile(this,newTail);
    takeLock.

resume();
}
```

### Combining on take

Same pattern: the lock holder reads its own value from the buffer, then
walks successors. For each, it reads the next buffer slot into
`succ.value` and advances the head. A single volatile head write follows.

### Combining with transducers

When a transducer is present, combining feeds each successor's value
through the reducing function rather than writing directly. If the
transducer signals completion (Reduced), the channel is sealed.

## Suspend / Resume (Cross-Lock Signaling)

When a thread wins the lock but can't proceed (buffer empty for consumer,
buffer full for producer), it suspends on the lock's signal field and
waits for the other side to resume it.

The signal is integrated into RelayLock, not a separate object. A single
`ref` field transitions between two states:

```
SIGNALED  — sentinel node, no waiter (initial state)
node      — a waiter is registered
```

### SIGNALED sentinel

```java
static final Node SIGNALED;

static {
    SIGNALED = new Node(null);
    SIGNALED.state = DONE;
}
```

A Node with `thread = null` and `state = DONE`. Using a sentinel instead
of null eliminates the need for Dekker-style rechecks: if a resume fires
between the buffer check and the suspend, the suspending thread sees
SIGNALED via its getAndSet return value and returns without parking.

### suspend(node)

```java
public boolean suspend(Node node) {
    Node prev = (Node) REF.getAndSet(this, node);
    if (prev == SIGNALED) return false;      // resume arrived first
    if (prev != node && prev.state != DONE)
        prev.wake();                         // stale waiter, wake it
    while (ref == node) {
        LockSupport.park(this);
    }
    return interrupted;
}
```

Called by a consumer (buffer empty) or producer (buffer full) while
holding its lock. The lock is held while suspended — producers and
consumers use separate locks, so this doesn't block the other side.

Returns true if the thread was interrupted while parked (interrupt flag
is captured and deferred).

### resume()

```java
public void resume() {
    Node prev = (Node) REF.getAndSet(this, SIGNALED);
    if (prev == SIGNALED) return;            // no waiter
    if (prev.state == DONE) return;          // stale
    prev.wake();
}
```

Called by the other side (consumer resumes putLock, producer resumes
takeLock) after an operation that may unblock the waiter. The getAndSet
atomically replaces the waiter with SIGNALED, so a concurrent suspend
will see SIGNALED and not park.

### Steady-state cost (buffer neither empty nor full)

3 exchanges for the lock, 0 for the signal. The signal is only
touched at the edges.

## Usage in BoundedChannel

Two RelayLock instances: one for producers, one for consumers. Each
lock's signal field handles cross-lock coordination.

```java
final RelayLock putLock = new RelayLock();
final RelayLock takeLock = new RelayLock();
```

### Ring buffer

Power-of-2 sized array with a bitmask for index computation. Producer
and consumer cursors (`tail` and `head`) are monotonically increasing
longs. Cache-line padding separates them:

```
volatile long tail      — producer cursor
long[8] padding
volatile int state      — OPEN | SEALED | CANCELLED
long[8] padding
volatile long head      — consumer cursor
```

The producer reads `head` via `getAcquire` (weaker than volatile — only
needs to see consumer progress, not establish happens-before for other
fields). The consumer reads `tail` the same way. Writes to both cursors
are volatile.

### Put path

```
1. Create Node(currentThread), set node.value = value
2. Acquire putLock (may park in chain)
3. If node.combined: predecessor already wrote our value, return
4. Write value to buffer, advance tail
5. If buffer was empty: takeLock.resume()
6. Combine: walk successor chain, write their values
7. Release last node
```

If the buffer is full at step 4, the thread suspends on `putLock`
(holding the lock) until a consumer resumes it.

### Take path

```
1. Create Node(currentThread)
2. Acquire takeLock (may park in chain)
3. If node.combined: predecessor already read our value into node.value, return it
4. Read value from buffer, advance head
5. Combine: walk successor chain, read their values
6. Release last node
7. If buffer was full before: putLock.resume()
```

If the buffer is empty at step 4, the thread suspends on `takeLock`
until a producer resumes it. If the channel is SEALED or CANCELLED,
the thread returns CANCELLED.

### Transducers

When constructed with a transducer, the put path feeds values through a
reducing function that wraps `putDirect`. The reducing function can
expand (mapcat), filter, or transform values. Backpressure propagates
naturally: `putDirect` suspends when the buffer is full, even mid-batch
from a mapcat expansion. If the reducing function returns Reduced, the
channel is sealed.

Combining with transducers works the same way — each successor's value
is fed through the reducing function rather than written directly.

### Seal/Cancel

```java
this.state =SEALED;   // or CANCELLED
VarHandle.

fullFence();
putLock.

resume();
takeLock.

resume();
```

Both sides are woken. Woken threads see the state, skip work, release
their lock nodes, and the chain unwinds via successive DONE handoffs.

## Properties

- **FIFO.** Strict arrival order. No spatial bias.
- **Bounded cost.** Every synchronization operation is a single getAndSet.
  No retry loops, no spinning, no unbounded CAS contention.
- **Predictable.** Same cost regardless of contention level.
- **Simple.** One slot, one node type, three states, three operations.
- **Combining.** Lock holder batches operations for queued successors,
  amortizing lock overhead and eliminating context switches under
  contention.
- **No queue management.** No head/tail bookkeeping, no node recycling,
  no cleanup on drain.
- **Allocation.** One Node (four fields) per lock acquisition. Trade-off
  vs allocation-free designs.
- **Deferred interruption.** A thread in the chain cannot bail until it
  becomes owner. The chain is a strict sequence with no skip-ahead —
  each thread only knows its predecessor, so there is no way to splice
  yourself out from the middle. Interruption is deferred: the thread
  waits for ownership, then restores the interrupt flag, skips the
  critical work, and hands off immediately. For short critical sections
  (buffer read/write), the delay is bounded by chain length times a
  trivial operation.
- **Channel-level cancellation.** While individual threads are
  uninterruptible mid-chain, the channel as a whole is cancellable.
  Set the channel state to CANCELLED, resume both locks. The owner
  sees CANCELLED, skips work, writes DONE, successor wakes, sees
  CANCELLED, writes DONE — the chain unwinds itself as a rapid cascade.
  No queue traversal, no individual thread interruption. Just kick the
  head and the relay does the rest.
- **No global view.** The chain is not an inspectable data structure.
  There is no head pointer, no tail pointer, no linked list in the heap.
  The chain exists as a sequence of threads blocked on predecessor nodes,
  largely on the stack. The only heap-visible state is the slot (holding
  the most recent node) and the individual nodes (each holding at most
  one successor reference). The chain's structure is implicit in the
  call stacks of the participating threads.

## Benchmark Results

All benchmarks transfer 10M items per channel. Quiescent (RelayLock +
BoundedChannel) vs core.async. Default buffer size is 1024 unless noted.

### Virtual threads and carrier saturation

The design is tuned for virtual threads. When a VT parks (on the lock
chain or on a signal), the carrier thread doesn't idle — it switches to
another runnable VT. This is a cheap continuation swap, not an OS-level
context switch. The "blocking" lock becomes a scheduling hint: *I can't
advance, but someone else can.*

This means a system under contention can achieve lock-free behaviour in
aggregate. RelayLock's semantics are blocking — a VT waits on its
predecessor — but the carrier thread never stops making progress as long
as there's a runnable VT somewhere. The more channels and threads in the
system, the more saturated the carriers stay, and the more the park cost
approaches zero.

The single-channel benchmarks below are a microbenchmark artifact: a
lone channel with nothing else running is an abnormal situation. When a
VT parks on an empty buffer and no other VT is runnable, the carrier
descends to OS-level idle — a scenario that doesn't occur in real
systems. Even a system with exactly one channel will have other work
(HTTP handlers, database calls, computation) keeping carriers busy.

The many-channel and pipeline tests are the representative workload.

### Single-channel throughput

| Scenario  | Buffer | Speedup | Notes                        |
|-----------|--------|---------|------------------------------|
| 1P1C      | 1024   | 2.3x    |                              |
| 1P1C      | 16     | 2.6x    |                              |
| 1P1C      | 1      | 1.0x    | Park-heavy, signal dominates |
| Ping-pong | 1      | 3.1x    | Pure handoff latency         |
| 4P4C      | 1024   | 0.5x    | core.async faster            |
| 4P4C      | 16     | 1.1x    |                              |
| 4P4C      | 1      | 1.0x    |                              |

The 4P4C deficit in isolation is misleading. With 8 VTs contending on
one channel and nothing else running, carrier threads idle during parks.
Spawn 50 of those same 4P4C channels and they all complete in 77ms
(26.1x faster than core.async) — the carriers stay saturated and the
park cost vanishes.

### Many-channel scaling

| Scenario       | Speedup | Notes |
|----------------|---------|-------|
| 50×1P1C        | 13.0x   |       |
| 50×4P4C        | 26.1x   |       |
| 200×1P1C       | 23.1x   |       |
| 200×1P1C buf=1 | 29.0x   |       |
| Mixed (40 ch)  | 18.5x   |       |

This is where the design pays off. core.async's thread-pool-based
dispatch collapses under many concurrent channels. RelayLock's direct
handoff (no thread pool, no dispatch queue) keeps carrier threads
saturated — every park is an instant switch to another channel's VT.

### Fan-in (many producers, one consumer)

| Scenario  | Buffer | Speedup |
|-----------|--------|---------|
| 16P1C     | 64     | 4.5x    |
| 32P1C     | 64     | 6.5x    |
| 64P1C     | 64     | 7.5x    |
| 128P1C    | 64     | 9.2x    |
| 16P1C     | 1024   | 4.8x    |
| 32P1C     | 1024   | 12.5x   |
| 64P1C     | 1024   | 17.7x   |
| 128P1C    | 1024   | 24.2x   |
| 24×16P1C  | 1024   | 31.0x   |
| 24×32P1C  | 1024   | 209.2x  |
| 24×64P1C  | 1024   | 674.6x  |
| 24×128P1C | 1024   | 837.6x  |

Fan-in is the strongest scenario. Two effects compound: combining
batches more writes per lock cycle as producers queue up, and carrier
saturation from the many contending VTs ensures parks are free. The
multi-channel fan-in variants (24×NP1C) show extreme speedups as
core.async's dispatch queue becomes a bottleneck while RelayLock's
per-channel combining scales independently.

### Transducer channels

| Scenario       | Buffer | Speedup |
|----------------|--------|---------|
| XF map 1P1C    | 1024   | 3.0x    |
| XF filter 1P1C | 1024   | 3.0x    |
| XF mapcat 1P1C | 1024   | 2.6x    |
| XF map 4P4C    | 1024   | 0.6x    |
| XF 128P1C      | 64     | 12.2x   |
| XF 128P1C      | 1024   | 20.2x   |

Same pattern: 1P1C and fan-in are strong. The 4P4C result is the
single-channel isolation artifact — not representative of production
load.

### Pipeline

| Scenario         | Buffer | Speedup |
|------------------|--------|---------|
| Pipe 4P→1P→4C    | 16     | 4.3x    |
| Pipe 4P→1P→4C    | 64     | 4.3x    |
| Pipe 4P→1P→4C    | 1024   | 4.4x    |
| Pipe XF 4P→1P→4C | 1024   | 4.1x    |
| 20×Pipe 4P→1P→4C | —      | 33.2x   |

Consistent 4x for single pipelines across buffer sizes. The 20-pipeline
variant shows the multi-channel scaling advantage — carrier saturation
from 20 concurrent pipelines eliminates park overhead.

### Discussion

The abstraction boundary shifts with virtual threads. At the VT level, RelayLock is blocking — a virtual thread parks
waiting on its predecessor. But at the carrier thread level, that park is just a continuation switch. The carrier never
stops making progress as long as there's a runnable virtual thread somewhere.

So the 24×128P1C benchmark showing 625x isn't just combining — it's the carrier threads staying saturated. 3072 virtual
threads contending across 24 channels means there's always a runnable VT ready when one parks. The carrier thread does a
cheap continuation swap and immediately resumes useful work. The "blocking" lock becomes a scheduling hint: I can't
advance, but someone else can.

Contrast with platform threads: parking means an OS-level context switch, the core goes idle or picks up an unrelated OS
thread, cache lines go cold. The blocking is real all the way down.

The 1P1C case with a single channel is the degenerate case — one VT parks on an empty buffer, the carrier has nothing
else to run, so it actually descends to OS-level idle. That's why the speedup is modest there. The design rewards
system-level concurrency, not single-channel throughput.

Traditional lock-free algorithms optimize for individual progress without cooperation. RelayLock optimizes for aggregate
progress through cooperation — the combining, the FIFO handoff, the parking that feeds the VT scheduler. The more
threads participate, the better it gets.

## Alt (Select / Choice)

Alt takes from (or puts to) whichever of several channels is ready
first. Each Alt operation consumes or produces exactly one value on
one channel.

### Properties

- **Fairly queued.** AltNodes participate in each channel's lock chain
  like normal nodes. FIFO ordering is preserved.
- **Exactly-once.** A shared claim cell (single CAS) ensures only one
  channel completes per Alt operation.
- **Fair or priority.** `:fair` round-robins the first channel tried.
  `:prio` always starts at the top.

### The core tension

RelayLock's implicit lock chain provides fair queuing, but a thread
can only sit in one chain at a time. To wait on N channels
simultaneously, we need N threads.

The calling thread must never enter a lock chain. If it did and
another channel completed the Alt, the caller would be trapped until
its predecessor releases — which may never happen (predecessor
suspended on an empty buffer with no producer). The caller parks
exclusively on the claim cell. Virtual threads own the lock chains.

### AltNode

```java
class AltNode extends Node {
    final AltClaim claim;      // shared across all VTs in one Alt op
    Node prev;                 // predecessor, set after enqueue
}
```

AltNode IS a Node. Its `state` field serves the same role: the lock
chain successor registers on it, and DONE is written to release.
All AltNodes belonging to the same Alt operation share the same
AltClaim. `node.thread` is set to the VT's thread (not the caller's),
so predecessor wakes route to the VT.

### AltClaim

```java
class AltClaim {
    volatile int claimed;         // 0 = open, 1 = claimed (CAS field)
    volatile Object result;       // the value (take) or Boolean (put)
    final Thread callerThread;    // the Alt caller to unpark

    boolean tryClaim();           // CAS 0 → 1

    boolean isClaimed();          // read claimed

    void setResult(Object value);  // write result, unpark caller
}
```

### Cascade protocol

Alt spawns virtual threads lazily via a cascade. Each participant
handles one channel: enqueue, try for data, and spawn the next VT
only at the point where it must park.

### Step by step

```
Alt.take(channels):
    claim = new AltClaim(currentThread)
    start VT for ch[0] (cascade spawns further VTs as needed)

    while !claim.isClaimed():
        park()

    return claim.result
```

The caller does no channel work. It creates the claim, starts the first
VT, and parks. The VT cascade handles all lock acquisition, buffer
access, and combining. Dead VTs (claim already won by another channel)
drain naturally through the lock chain — no interrupt, no cleanup.

### VT lifecycle (handleChannel)

```
handleChannel(ch[i], node, claim, idx):
    prev = node.prev

    if prev.state != DONE and !node.combined:
        // Must wait in lock chain — spawn VT for next channel NOW
        if idx+1 < N:
            spawn + enqueue + start next VT for ch[idx+1]
        awaitPredecessor(prev, node)

    if node.combined: return                    ← combiner handled us
    if claim.isClaimed(): release, return       ← another channel won

    // We're the lock owner
    loop:
        if claim.isClaimed(): release, return
        if buffer non-empty:
            if claim.tryClaim():
                take value
                claim.setResult(value)          ← unparks caller
                combine successors
                release last, return
            else: release, return
        if sealed/cancelled: release, return
        // Buffer empty — spawn VT for next channel if not yet spawned
        if idx+1 < N and not yet spawned:
            spawn + enqueue + start next VT for ch[idx+1]
        suspend(node)
        // woken by producer resume → re-check
```

The cascade spawns the next VT at the **first park point** — either
the lock chain (predecessor busy) or the signal (buffer empty). If
the VT gets DONE and data, it completes without spawning further VTs.

Dead VTs (claim already won) wake naturally when their predecessor
releases, check `claim.isClaimed()`, and release — potentially
combining successors on their way out. No special cleanup needed.

### Combining interaction

The combining loop handles AltNodes transparently. When the lock
holder walks the successor chain:

**Live AltNode** (claim CAS succeeds): the combiner claimed this Alt
for this channel. It reads a value from the buffer (take) or writes
the AltNode's value to the buffer (put), advances the cursor, sets
`combined=true`, calls `claim.setResult()` to wake the Alt caller,
and wakes the VT. The VT exits immediately.

**Dead AltNode** (claim CAS fails): another channel already won this
Alt. The combiner sets `combined=true` and wakes the VT. No buffer
work, no cursor advance. The VT wakes, sees `combined`, exits. The
dead AltNode is a skip node — discarded with minimal cost.

```java
// In takeCombine:
while(newHead<t &&state ==OPEN){
Object succObj = current.state;
    if(!(succObj instanceof
Node succ))break;

        if(succ instanceof
AltNode alt){
        if(!alt.claim.

tryClaim()){
// Dead alt — skip, no buffer work
succ.combined =true;
        succ.

wake();

current =succ;
            continue;
                    }
                    // Live alt — claim won
                    alt.claim.

setResult(buffer[(int)(newHead &mask)]);
        }

// Normal combine (live alt falls through here too)
succ.value =buffer[(int)(newHead &mask)];
buffer[(int)(newHead &mask)]=null;
newHead++;
succ.combined =true;
        succ.

wake();

current =succ;
}
```

Dead AltNodes don't truncate the batch. The combiner passes through
a chain of `[live, dead, dead, live, normal, dead, live]` in one
pass, advancing the cursor only for live nodes. Under heavy Alt
contention, this preserves the batching advantage.

### Fair / Prio

Both modes affect the cascade order. In the wait phase (VTs racing),
any channel that becomes ready first wins.

```java
private int scanStart() {
    return fair
            ? (int) Math.floorMod(OFFSET.getAndAdd(this, 1L), channels.length)
            : 0;
}
```

**`:prio`** — cascade starts at index 0, always. The caller tries
channel 0 first. If channel 0 has data, it wins deterministically
(one VT, no cascade). If no channel has data, any VT can win — consistent with
standard Alt semantics (priority applies to "multiple ready", not
"waiting").

**`:fair`** — cascade starts at a rotating offset. Each Alt operation
starts at the next channel. Prevents starvation.

### Composition

Alt implements IChannel. An outer Alt that includes an inner Alt as
one of its channels calls `innerAlt.take()` from a VT. The inner Alt
runs its own cascade internally. Each level manages its own claim
cell. Nesting adds VTs proportionally (N_outer + N_inner), but VTs
are cheap.

### Cost

| Scenario                           | VTs spawned              | Lock cycles     |
|------------------------------------|--------------------------|-----------------|
| ch[0] DONE + data                  | 1                        | 1 (3 exchanges) |
| ch[0] contended, data after wait   | 2 (ch[1] dead)           | 1 each          |
| All empty, ch[k] gets data first   | k+1                      | 1 each          |
| N concurrent Alts batched on ch[0] | N (all combined)         | 1 total         |

The last row is the batching win: N concurrent Alt calls all cascade
to the same channel and are combined in one lock cycle. The combiner
does N claim CASes + N buffer reads + one volatile HEAD write.

### Per-Alt allocation

| Object                            | Size                        | When                |
|-----------------------------------|-----------------------------|---------------------|
| AltClaim                          | ~40 bytes                   | always              |
| AltNode + VT (continuation+stack) | ~1 KB                       | per channel visited |

## Alt Implementation Milestones

### M1: Core mechanics — Alt take, prio only

AltClaim, AltNode, cascade spawning with started VTs. Claim CAS for
exactly-once. Dead VT passthrough (check `isClaimed()`, release).
No combining — Alt VTs just take and release. `:prio` only (always
start at index 0).

Proves: cascade protocol, claim coordination, dead VT draining.

### M2: Fair mode

Add rotating offset for `:fair`. Trivial change to cascade start
index, but needs testing to verify starvation prevention.

### M3: Combining with AltNodes

Lock holders walk successor chain and handle AltNodes: live nodes
(claim CAS succeeds) get combined, dead nodes (claim CAS fails)
get skipped. Dead AltNodes driving combine passes for successors
behind them.

### M4: Alt put

Mirror of Alt take. Same cascade/claim mechanics, buffer write
instead of read.

### M5: Alt as IChannel (composition)

Alt implements the channel protocol. Enables nesting: an outer Alt
can include an inner Alt as one of its channels.

### M6: Unstarted VT optimization

Create VTs via `Thread.ofVirtual().unstarted()` to defer continuation
allocation. Caller does the initial enqueue + state exchange inline;
VT is only started if the caller can't complete immediately. Gives a
fast path where the common case (uncontended lock, buffer non-empty)
completes with zero started VTs (~200 bytes instead of ~1 KB).

Evaluate after profiling whether the allocation savings justify the
added complexity (two code paths for the same operation).

## Open Questions

- **4P4C contention.** Symmetric many-to-many on a single channel is the
  weak spot. The single slot serializes all arrivals. Techniques like
  arrival spreading (multiple slots with fallback) could help, but add
  complexity.

- **Allocation pressure.** One node per acquisition. Under high throughput
  this creates GC pressure. Could nodes be recycled (thread-local pool)?
  Would need care to avoid use-after-recycle — a node must not be recycled
  until the successor has completed its getAndSet on it.
