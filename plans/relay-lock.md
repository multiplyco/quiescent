# RelayLock

A lock where threads form a FIFO handoff chain using three exchange (getAndSet)
operations per lock cycle. No CAS, no spinning, no queue structure.

## Core Idea

A single atomic slot holds a node. Each thread creates a node (one field),
swaps it into the slot via getAndSet, and receives back the predecessor's
node. The predecessor's node is a one-shot rendezvous point: both threads
do a getAndSet on its field to coordinate handoff.

The node field has three states: `null` (initial), a thread reference
(successor registered), or `DONE` (owner finished). The getAndSet return
value tells each side what happened.

## Structure

```
slot:  AtomicReference<Node>   — the single coordination point
```

```java
class Node {
    // Accessed via VarHandle getAndSet. Three states:
    //   null    — initial
    //   Thread  — successor has registered
    //   DONE    — owner has finished critical section
    volatile Object state;
}
```

## Thread Lifecycle

### Full cycle: 3 exchanges

```
1. getAndSet(slot, myNode)         → predecessor's node (or null)
2. getAndSet(predecessor.state, self)  → null (park) or DONE (proceed)
3. getAndSet(myNode.state, DONE)       → null (no successor) or Thread (unpark it)
```

### Step by step

**Acquire:**

```
1. Create node (one object, one field)
2. prev = slot.getAndSet(myNode)
3. If prev == null: first arrival, proceed (owner)
4. If prev != null:
     result = prev.state.getAndSet(self)
     If result == DONE: predecessor finished, proceed (owner)
     If result == null: predecessor still working, park
```

**Release (after critical section):**

```
1. successor = myNode.state.getAndSet(DONE)
2. If successor == null: nobody waiting, done
3. If successor is a Thread: unpark it
```

### Why this works

The node field is a rendezvous between exactly two threads: the current
owner (A) and the next arrival (B). Both do getAndSet on the same field.
The return value tells them who arrived first:

**Case 1 — B arrives first:**
- B does getAndSet(A.state, self) → gets null → parks
- A finishes, does getAndSet(A.state, DONE) → gets B → unparks B

**Case 2 — A finishes first:**
- A does getAndSet(A.state, DONE) → gets null → walks away
- B does getAndSet(A.state, self) → gets DONE → proceeds immediately

**Case 3 — simultaneous:**
- getAndSet is atomic; one of the above cases applies

No lost wakeups. No Dekker protocol needed. The atomicity of the exchange
handles the coordination that would otherwise require two ordered memory
accesses (Dekker) or a compare-and-swap.

### FIFO ordering

```
A arrives:  slot.getAndSet(nodeA) → null       → A is owner
B arrives:  slot.getAndSet(nodeB) → nodeA      → B waits on A
C arrives:  slot.getAndSet(nodeC) → nodeB      → C waits on B
```

Each thread only knows its predecessor. The chain is implicit:
A → B → C. Handoff follows arrival order.

### No cleanup needed

There is no head pointer, no queue to drain. After A finishes and hands
off to B, A's node remains in no structure — it's just garbage. B's node
is in the slot until C swaps it out (or indefinitely if nobody comes).

A node left in the slot doesn't go stale. If B finishes and nobody arrives
for an hour, the node still works: eventual arrival C swaps it out, does
getAndSet on its field, sees DONE, and proceeds immediately.

## Machine cost

On x86, getAndSet compiles to XCHG (single instruction, implicit lock
prefix). Three exchanges per lock cycle = three machine instructions for
the synchronization path.

On ARM, getAndSet is a LDAXR/STLXR pair (load-linked/store-conditional).
Still the minimal atomic primitive — no comparison step like CAS.

### Per-lock-cycle cost

| Operation | Instruction (x86) | Count |
|---|---|---|
| Enqueue (swap slot) | XCHG | 1 |
| Acquire (swap predecessor node) | XCHG | 1 |
| Release (swap own node) | XCHG | 1 |
| **Total** | | **3** |

Plus one object allocation (node with one field) per acquisition.

### Comparison to alternatives

**CLH lock:** getAndSet on tail + spin on predecessor's flag. Similar
enqueue cost, but spins instead of parking. No park/unpark overhead in
the fast case, but burns CPU. RelayLock trades spinning for one extra
exchange + park/unpark.

**MCS lock:** getAndSet on tail + write next pointer. Has a window where
the next pointer isn't visible yet, requiring a short spin. RelayLock
avoids this because the successor writes itself into the predecessor's
node (step 2), so the link is established synchronously.

**ReentrantLock (AQS):** CAS on state + CLH-variant queue with multiple
CAS operations for enqueue/dequeue. Heavier per-operation cost, more
complex state machine.

## Usage in BoundedChannel

Two RelayLock instances: one for producers, one for consumers.

```java
RelayLock putLock  = new RelayLock();
RelayLock takeLock = new RelayLock();
```

### Signal — buffer-edge coordination

When a thread wins the lock but can't proceed (buffer empty for consumer,
buffer full for producer), it needs to park and be woken by the other side.

A separate Signal object handles this with two methods:

```java
class Signal {
    volatile Object ref; // null | Thread | AltNode

    void await()                 // register self, park
    void signal(IChannel channel) // wake waiter, handle Alt claim
}
```

**await():** `REF.getAndSet(this, Thread.currentThread())`, then park.
Called by a consumer (buffer empty) or producer (buffer full) while
holding its lock. The lock is held while parked — producers and
consumers use separate locks, so this doesn't block the other side.

**signal(channel):** `REF.getAndSet(this, null)`, then dispatch on the
return value in a loop:

```java
void signal(IChannel channel) {
    Object waiter = REF.getAndSet(this, null);
    while (true) {
        if (waiter == null) return;
        if (waiter instanceof Thread t) {
            LockSupport.unpark(t);
            return;
        }
        AltNode alt = (AltNode) waiter;
        if (alt.ref.claim(channel)) {
            LockSupport.unpark(alt.thread);
            return;
        }
        // Dead alt — do its lock release, loop on successor
        waiter = STATE.getAndSet(alt, DONE);
    }
}
```

The loop handles chains of dead AltNodes: each iteration writes DONE
(releasing the dead alt's lock node) and follows the successor. The
loop terminates when it finds a live waiter (Thread or successful Alt
claim) or null (nobody waiting).

**Channel state (OPEN/SEALED/CANCELLED)** is a separate volatile field
on the channel, not part of Signal. Checked by the lock holder before
and after parking. Cancellation wakes the signal and the chain unwinds
via successive DONE handoffs.

### Steady-state cost (buffer neither empty nor full)

3 exchanges for the lock, 0 for the signal. The signal is only
touched at the edges.

### Seal/Cancel

```java
state = SEALED;
signal.signal(this); // wake whoever's waiting
// Woken thread sees state, exits, releases lock, chain unwinds
```

## Alt (Select / Choice)

Alt takes from whichever of several channels has a value first. AltNode
extends Node with a thread reference and a shared claim (ChannelRef).
It participates in the lock chain and the signal field using the same
getAndSet protocol — other threads cooperate to complete or skip dead
AltNodes via instanceof dispatch and a single CAS.

### AltNode structure

```java
class AltNode extends Node {
    final Thread thread;        // the Alt's thread to unpark
    final ChannelRef ref;       // shared claim flag, initially null
}
```

AltNode IS a Node. Its `state` field serves the same role: the lock
chain successor registers on it, and DONE is written to release.
All AltNodes belonging to the same Alt operation share the same
ChannelRef. Whichever channel successfully CAS's the ref claims the Alt.

### How Alt enters a channel

Alt creates an AltNode and acquires the lock like any thread:

```
1. altNode = new AltNode(self, ref)
2. prev = SLOT.getAndSet(lock, altNode)      ← enqueue
3. STATE.getAndSet(prev, altNode)             ← register on predecessor
   (writes AltNode, not Thread — predecessor will dispatch on type)
4. Got DONE? Alt is the owner. Check buffer.
   Got null? Predecessor still working. Park.
```

The predecessor's release path dispatches:

```java
Object successor = STATE.getAndSet(myNode, DONE);
if (successor instanceof Thread t) {
    LockSupport.unpark(t);
} else if (successor instanceof AltNode alt) {
    if (alt.ref.claim(channel)) {
        LockSupport.unpark(alt.thread);
    } else {
        // Dead alt — do its lock release, loop on successor
        Object next = STATE.getAndSet(alt, DONE);
        // ... continue dispatch on next (same loop)
    }
}
```

This is the same loop as Signal.signal() — a uniform dispatch that
skips dead AltNodes by writing DONE and following the chain.

### Alt holds the lock, buffer empty

When Alt acquires the lock and finds the buffer empty, it registers
on the Signal with its AltNode (not a thread reference):

```
1. Alt acquires lock → altNode is in the slot
2. Buffer empty → signal.await(altNode)
3. Alt parks
```

Meanwhile, altNode is in two places:
- **Signal ref field** — waiting for a producer to signal
- **Lock slot** — a successor may register on altNode.state

A successor B arrives, does SLOT.getAndSet → gets altNode,
does STATE.getAndSet(altNode, threadB) → null → parks.

Now altNode.state = threadB, Signal ref = altNode.

### Signal wakes Alt (claim succeeds)

```
1. Producer deposits value, calls signal.signal(channel)
2. REF.getAndSet(null) → altNode
3. alt.ref.claim(channel) → succeeds
4. unpark(alt.thread)
5. Alt wakes, takes value from buffer
6. Alt releases: STATE.getAndSet(altNode, DONE) → threadB → unpark B
7. B wakes as new owner. Normal chain continues.
```

### Signal skips dead Alt (claim fails)

```
1. Producer deposits value, calls signal.signal(channel)
2. REF.getAndSet(null) → altNode
3. alt.ref.claim(channel) → fails (another channel won)
4. waiter = STATE.getAndSet(altNode, DONE) → threadB
5. Loop: waiter is Thread → unpark(threadB) → return
6. B wakes, sees altNode.state == DONE, becomes owner, takes value.
```

The signal cooperatively does the lock release on behalf of the dead
Alt. Same getAndSet-DONE protocol, just a different thread driving it.

**No successor yet (altNode.state == null):**
```
4. STATE.getAndSet(altNode, DONE) → null → return
5. DONE is set. Next arrival sees it immediately, proceeds.
```

### Dead Alt wakes later from winning channel

The Alt thread was claimed by another channel and eventually wakes.
It does `release(altNode)`: `STATE.getAndSet(altNode, DONE)`.

- Gets DONE → no-op (signal already released)
- Gets a Thread → spurious unpark (thread already proceeded)

Both are harmless. The release is idempotent. All park loops re-check
conditions before continuing, so spurious unparks cause no harm.

### Alt visits multiple channels

Alt acquires and releases each channel's lock in sequence, checking
the claim between each:

```
1. Visit ch1:
   Acquire lock, check buffer.
   Has value? Take it, claim ref, release. Done.
   Empty? signal.await(altNode1), release lock, park... or:
          release lock, move on to ch2.
   Check ref — claimed? Stop.

2. Visit ch2:
   Check ref — not claimed. Acquire lock, repeat.

3. Visit ch3:
   Check ref — claimed by ch1! Stop registering.
```

AltNodes left on losing channels are dead nodes. They will be skipped
by whoever encounters them — the release path or signal.signal() writes
DONE and follows the successor.

### Uniform dispatch

The release path and signal.signal() share the same dispatch loop:

```java
while (true) {
    if (waiter == null) return;
    if (waiter instanceof Thread t) {
        LockSupport.unpark(t);
        return;
    }
    AltNode alt = (AltNode) waiter;
    if (alt.ref.claim(channel)) {
        LockSupport.unpark(alt.thread);
        return;
    }
    waiter = STATE.getAndSet(alt, DONE);
}
```

One loop, three cases (null, Thread, AltNode). The chain composes:
regular → alt → alt → regular all work. The instanceof branch is the
only Alt-specific logic. The non-Alt hot path is unaffected — successor
is always a Thread, one instanceof check (predicted by JIT), unpark.

### Cost

- **Winning channel:** one CAS (on ref) + unpark. Same as a normal
  handoff plus one CAS.
- **Losing channels:** one CAS (fails) + one getAndSet (relay past).
  Two atomic ops to skip a dead node.
- **Allocation:** one AltNode per channel registered. All share the
  same ChannelRef.

## Properties

- **FIFO.** Strict arrival order. No spatial bias (unlike StampedeLock).
- **Bounded cost.** Every operation is a single getAndSet. No retry loops,
  no spinning, no unbounded CAS contention.
- **Predictable.** Same cost regardless of contention level.
- **Simple.** One slot, one node type, three states, three operations.
- **No queue management.** No head/tail bookkeeping, no node recycling,
  no cleanup on drain.
- **Allocation.** One small object (one field) per lock acquisition.
  Trade-off vs allocation-free designs (StampedeLock).
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
  Set the channel state to CANCELLED, wake the current owner via the
  signal field. The owner sees CANCELLED, skips work, writes DONE,
  successor wakes, sees CANCELLED, writes DONE — the chain unwinds
  itself as a rapid cascade. No queue traversal, no individual thread
  interruption. Just kick the head and the relay does the rest.
- **No global view.** The chain is not an inspectable data structure.
  There is no head pointer, no tail pointer, no linked list in the heap.
  The chain exists as a sequence of threads blocked on predecessor nodes,
  largely on the stack. The only heap-visible state is the slot (holding
  the most recent node) and the individual nodes (each holding at most
  one successor reference). The chain's structure is implicit in the
  call stacks of the participating threads.

## Open Questions

- **Allocation pressure.** One node per acquisition. Under high throughput
  this creates GC pressure. Could nodes be recycled (thread-local pool)?
  Would need care to avoid use-after-recycle — a node must not be recycled
  until the successor has completed its getAndSet on it.

- **Park/unpark overhead.** Every contended acquisition parks. CLH/MCS
  spin briefly first, which is faster when the critical section is very
  short. An adaptive front-spin before parking could help, at the cost of
  complexity.

- **Performance vs StampedeLock.** StampedeLock spreads arrival contention
  across N slots (O(1) expected CAS collisions). RelayLock serializes all
  arrivals through one slot. Under very high contention, the single slot
  may become a bottleneck. Under moderate contention, the simpler protocol
  may win. Benchmarking needed.

- **Signal field ordering.** The getAndSet on the signal field needs to be
  correctly ordered with respect to buffer reads/writes. The lock's
  acquire/release provides happens-before for buffer access, but the
  signal field is accessed across locks (producer signals consumer and
  vice versa). Need to verify that the getAndSet's full-fence semantics
  are sufficient, or whether additional ordering is needed.
