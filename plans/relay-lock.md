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

### Buffer-edge signaling

When a thread wins the lock but can't proceed (buffer empty for consumer,
buffer full for producer), it needs to park and be woken by the other side.

A single shared signal field handles this, using getAndSet:

```
signal: AtomicReference<Object>   — null | Thread
```

The buffer can't be simultaneously empty and full, so at most one side is
waiting at any time. Both sides use the same field:

**Consumer finds buffer empty:**
```
signal.getAndSet(self) → null: park (wait for producer)
                       → producer Thread: impossible (buffer can't be both)
```

**Producer deposits, checks signal:**
```
signal.getAndSet(null) → null: nobody waiting
                       → consumer Thread: unpark it
```

**Producer finds buffer full:**
```
signal.getAndSet(self) → null: park (wait for consumer)
```

**Consumer drains, checks signal:**
```
signal.getAndSet(null) → null: nobody waiting
                       → producer Thread: unpark it
```

The getAndSet atomically installs the thread and reads whoever was there.
No Dekker, no CAS, no separate empty/full fields. One field, one operation.

### Steady-state cost (buffer neither empty nor full)

3 exchanges for the lock, 0 for the signal field. The signal field is
only touched at the edges.

### Seal/Cancel

```java
state = SEALED;
signal.getAndSet(null) → if Thread, unpark it
// Woken thread sees state, exits, releases lock, chain unwinds
```

## Alt (Select / Choice)

Alt allows a thread to register on multiple channels and take from
whichever has a value first. Rather than maintaining separate wait
queues or out-of-band cancellation, Alt splices skippable nodes into
the existing lock chains. Other threads cooperate to complete or
discard these nodes via instanceof checks and a single CAS.

### AltRef structure

```java
class AltRef {
    Thread thread;              // the Alt's thread to unpark
    Object next;                // next node in the chain (skip target)
    AtomicReference channel;    // shared claim flag, initially null
}
```

All AltRefs belonging to the same Alt operation share the same `channel`
reference. This is the sole synchronization point between channels:
whichever channel successfully CAS's it claims the Alt.

### Registration

Alt acquires each channel's lock in turn and inserts an AltRef where a
plain thread reference would normally go:

```
ch1 chain:  A → AltRef(t, next→B, claim) → B → C
ch2 chain:  D → AltRef(t, next→E, claim) → E
```

Both AltRefs share the same `claim` field.

The AltRef's `next` field points to the next real waiter in the chain.
This gives any thread encountering the AltRef a way to skip past it.

### Producer encounters AltRef

When a producer finishes its critical section and finds an AltRef as
the successor (via getAndSet on its own node), it does additional work
instead of a plain unpark:

```
1. CAS(altRef.channel, null, thisChannel)
2. Succeeded?
     → Alt claimed. Deliver value, unpark altRef.thread.
3. Failed?
     → Alt already claimed by another channel. This AltRef is dead.
     → Skip: getAndSet(altRef.next.state, DONE)
       → got Thread? Unpark it (relay to the real next waiter).
       → got null? Nobody there yet, done.
```

### Owner finishing, finds AltRef on its node

Same protocol. The owner (predecessor) does getAndSet on its own node
and gets back an AltRef instead of a Thread:

```
1. CAS(altRef.channel, null, thisChannel)
2. Succeeded? Deliver, unpark altRef.thread.
3. Failed? Follow altRef.next, relay DONE to the next real waiter.
```

The chain heals itself: the dead AltRef is transparently skipped, and
the next real thread in the chain receives the handoff.

### Alt thread wakes up

When the Alt's thread is unparked, it knows which channel claimed it
(by reading the `channel` field). It proceeds on that channel only.

The AltRefs on other channels remain in those chains as dead nodes.
They will be skipped when encountered — any thread that does instanceof
AltRef and fails the CAS on `channel` simply relays past it.

### Why this works

- **No separate cancellation mechanism.** Dead AltRefs are cleaned up
  lazily by whoever encounters them. No cancel tokens, no traversal
  of foreign queues.

- **No lock held across channels.** Alt registers on each channel
  independently. The shared `channel` field is the only cross-channel
  coordination, and it's a single CAS — no lock ordering concerns.

- **Chain integrity preserved.** The `next` field ensures that skipping
  an AltRef always connects to the real next waiter. The FIFO property
  holds for non-Alt threads.

- **instanceof dispatch.** The release path checks: is the successor a
  Thread? Plain unpark. Is it an AltRef? Try to claim, or skip. This
  is a single type check — no flags, no enum states, no wrapper objects.

### Cost

- **Winning channel:** one CAS (on `channel`) + unpark. Same as a
  normal handoff plus one CAS.
- **Losing channels:** one CAS (fails) + one getAndSet (relay past).
  Two atomic ops to skip a dead node.
- **Allocation:** one AltRef per channel registered. These share the
  `channel` AtomicReference, so only one extra allocation beyond the
  AltRefs themselves.

### Example

```
Alt registers on ch1 (consumer lock) and ch2 (consumer lock).
Alt thread = T.

ch1 chain: ... → AltRef(T, next→X, claim) → X → ...
ch2 chain: ... → AltRef(T, next→Y, claim) → Y → ...

Producer P1 on ch1 finishes, encounters AltRef:
  CAS(claim, null, ch1) → succeeds
  Delivers value to T, unparks T.
  T wakes, reads claim → ch1, proceeds on ch1.

Later, producer P2 on ch2 finishes, encounters AltRef:
  CAS(claim, null, ch2) → fails (already ch1)
  AltRef is dead. Skips:
    getAndSet(Y.state, DONE) → got Y's thread → unpark Y
  Y becomes owner on ch2. Chain continues normally.
```

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
