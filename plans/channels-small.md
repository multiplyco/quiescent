# Small-Buffer and Rendezvous Channel Design

Lock-free dual queue for buffer sizes 0–2. Unifies rendezvous
(buffer 0) and small-buffer channels under a single mechanism.
Larger buffers (4+) continue to use the adaptive ring buffer
(see `channels-adaptive.md`).

## Motivation

The adaptive ring buffer is optimized for the case where parking is
rare — the buffer absorbs bursts, and the Dekker fast path handles
the common non-parking put/take. But at small buffer sizes, parking
is the common case:

- **Buffer 0 (rendezvous)**: every put parks until a take, every
  take parks until a put. 100% parking.
- **Buffer 1**: the buffer is almost always full or empty. The
  producer or consumer parks on most operations.
- **Buffer 2**: parking is frequent, though less than buffer 1.

The adaptive ring buffer's lock + condition parking path costs 4
lock acquisitions per exchange (producer lock, producer await-release,
consumer lock-from-await, consumer unlock — plus symmetric for the
return signal). For small buffers where parking dominates, this is
the entire cost. Eliminating locks from the parking path is
proportionally large.

Benchmark context: 1P1C buffer=1 on the adaptive ring buffer is
1334 ms vs 127 ms for buffer=1024 (10x slower). The parking overhead
dominates.

Whether the lock-free dual queue actually outperforms the adaptive
lock at these sizes is an empirical question — node allocation and
CAS contention have their own costs. Benchmarking will decide.

## Core Idea: Dual Queue

A lock-free queue where arriving threads either match with a
waiting partner or enqueue themselves and park. No locks in any
path — matching is a per-node CAS, parking is `LockSupport.park()`,
wake-up is `LockSupport.unpark()`.

### The Queue Is the Buffer

There are no separate buffer slots. The queue itself serves as the
buffer. For buffer capacity N, up to N producer nodes can remain
unmatched in the queue without the producer parking — they enqueue
their value and return immediately. The (N+1)th producer enqueues
and parks until a consumer matches it.

This avoids the two-data-path problem: if values could live in both
slots and queue nodes, consumers preferring the slot fast path would
starve queued values (including atomic `onto` batches). A single
data path means FIFO ordering is guaranteed and `onto`/`put` are
treated uniformly.

The tradeoff: every operation allocates a queue node and does a CAS
to enqueue, even within buffer capacity. No pure slot fast path.

### Structure

```
producers                          consumers
... ← P3 ← P2 ← P1 ← [HEAD] → C1 → C2 → C3 → ...
```

HEAD is a sentinel. Producers enqueue to the left, consumers to the
right. At any point, only one side has waiters — if both sides are
present, they match and cancel out.

Each node:

```
Node {
    Object value;           // producer's value (or null for consumer)
    volatile Thread thread; // parked thread (null for non-parking / batch interior)
    volatile int state;     // WAITING, MATCHED, CANCELLED
    volatile Node next;     // link in queue direction
}
```

### Invariant

The queue only accumulates waiters of one type. If there are
producers on the left and a consumer arrives, the consumer matches
a producer (no consumer node created). If there are consumers on
the right and a producer arrives, the producer matches a consumer.
Both sides only accumulate when no partner is available.

### Credits (Buffer Capacity)

```
credits: volatile int   // 0..capacity (VarHandle, acquire/release)
```

Buffer capacity is expressed as credits. A producer that enqueues
a node can return without parking if credits > 0:

```
put(v):
  node = Node(v, thread=null)     // optimistically assume no park
  CAS-push node onto producer side
  c = CREDITS.getAndDecrement()
  if c > 0: return true           // credit consumed, no park needed
  CREDITS.getAndIncrement()       // undo: no credit available
  node.thread = currentThread     // prepare to park
  park()
  return node.state != CANCELLED
```

A consumer that matches a producer node restores a credit:

```
take():
  p = match leftmost producer
  CREDITS.getAndIncrement()       // restore credit
  if p.thread != null: unpark(p.thread)
  return p.value
```

For buffer 0, credits starts at 0 — every producer parks. For
buffer 1, credits starts at 1 — the first producer returns
immediately, the second parks. For buffer 2, credits starts at 2.

### Matching Protocol

**Consumer arrives, producers waiting:**

```
take():
  p = leftmost producer (closest to HEAD)
  if p is ClosedNode: return CANCELLED (sealed/cancelled)
  if CAS(p.state, WAITING, MATCHED):
    v = p.value
    CREDITS.getAndIncrement()                // restore credit
    if p.thread != null: unpark(p.thread)    // wake if parked
    return v                                 // (thread=null for credited/batch nodes)
  // CAS failed: another consumer matched first, try next producer
```

**Producer arrives, consumers waiting:**

```
put(v):
  c = rightmost consumer (closest to HEAD)
  if CAS(c.state, WAITING, MATCHED):
    c.value = v             // hand off value
    if c.thread != null: unpark(c.thread)  // wake consumer
    return true
  // CAS failed: another producer matched first, try next consumer
```

**Producer arrives, sentinel present:**

```
put(v):
  // CAS-append fails — next pointer is a ClosedNode, not null
  return false
```

**No partner available, no credits:**

```
put(v):                           // no consumers waiting
  node = Node(v, currentThread)
  CAS-push node onto left         // fails if sentinel present → return false
  park()
  return node.state != CANCELLED  // true if matched, false if cancelled

take():                           // no producers waiting
  node = Node(null, currentThread)
  CAS-push node onto right        // fails if sentinel present → return CANCELLED
  park()
  return node.value               // set by matching producer
```

Per-exchange cost: one CAS (on node state), one CAS (enqueue), one
credit increment. No lock acquisitions. Multiple independent pairs
match concurrently — the state CAS is per-node, not per-channel.

## Atomic Batch Put: `onto(coll)`

Pre-build a linked list of nodes from the entire collection, CAS the
chain onto the producer side in a single atomic operation. The
calling thread's reference goes on the **last node only** — earlier
nodes have `thread = null`.

```
onto([a, b, c]):
  build chain: Node(a, thread=null) → Node(b, thread=null) → Node(c, thread=self)
  CAS chain head onto producer side
  park()
  return last.state != CANCELLED
```

Consumers match nodes left to right (FIFO). Matching a node with
`thread = null` takes the value but does not unpark — the producer
is still waiting for the rest of the batch. Matching the final node
(with `thread = self`) takes the value **and** unparks the producer.

Single CAS for atomicity, single park/unpark cycle regardless of
batch size. No interleaving from other producers — the chain is a
contiguous sequence in the queue.

Credits are not consumed by `onto` — the batch always parks (the
producer needs to know when all values have been consumed). Credits
only apply to single `put` operations where the producer can
fire-and-forget.

If consumers are already waiting when `onto` is called, match them
directly from the chain before enqueuing the remainder.

## Lifecycle: Sentinels in the Queue

Seal and cancel are expressed as **sentinel nodes** in the queue
itself, rather than a separate volatile state field. Arriving
threads discover lifecycle transitions through the same structure
they already interact with.

### Sentinel Node

```
ClosedNode {
    boolean cancelled;   // false = sealed, true = cancelled
}
```

A single sentinel type. Producers attempting to CAS-append after
a sentinel fail — they see the sentinel instead of null as the
next pointer, and know to bail. Consumers walking the queue reach
the sentinel after draining all preceding nodes and branch on
`cancelled`.

### Seal

```
seal():
  CAS ClosedNode(cancelled=false) onto producer side
```

No new producers can enqueue — they see the sentinel and return
false. Existing producer nodes ahead of the sentinel are still
live; consumers drain them normally. When a consumer reaches the
sentinel, it knows no more values are coming — returns CANCELLED.

### `seal(coll)` — batch put then seal

Append the sentinel as the tail of the batch chain:

```
seal([a, b]):
  Node(a, thread=null) → Node(b, thread=self) → ClosedNode(cancelled=false)
  CAS chain onto producer side
  park()
```

The sentinel is part of the atomic CAS — no window for another
producer to slip in between the last value and the seal. When the
consumer matches Node(b), it unparks the producer. When it reaches
the ClosedNode, it transitions to sealed behavior.

### Cancel

```
cancel():
  CAS ClosedNode(cancelled=true) onto producer side
  CAS ClosedNode(cancelled=true) onto consumer side
  walk remaining producer nodes: CAS WAITING → CANCELLED, unpark
  walk remaining consumer nodes: CAS WAITING → CANCELLED, unpark
```

Both sides get the sentinel, so any thread arriving after cancel
immediately sees it. The cancelling thread walks both sides of the
queue synchronously, waking all parked threads. Woken threads check
their own node state, see CANCELLED, and return accordingly.

### State Checks

Arriving threads discover lifecycle state through the queue
structure — not a separate volatile field. A producer's CAS-append
fails if a sentinel is present. A consumer walking the queue hits
the sentinel after the last real node.

## Transducer Support

Transducers with buffer 0 don't make sense (the transducer needs
buffer space to emit into). For buffer 1–2 with transducer:
serialize producers through a lock (same as `BoundedChannelXf`),
transducer step function enqueues nodes via the queue. Consumer
side is unchanged — lock-free matching from the queue.

## Convergence with Adaptive Ring Buffer

The dual queue is not just for small buffers — it is the universal
parking infrastructure for all channel sizes. The adaptive ring
buffer (buffer 4+) currently uses `ReentrantLock` + `Condition` for
its overflow parking path. Replacing that with the same dual queue
gives uniform semantics across all buffer sizes.

### Adaptive = Rendezvous + Ring Buffer

The layering:

1. **Dual queue** — the foundation. Handles parking, matching,
   `onto` chains, seal/cancel sentinels. Present at every buffer
   size.
2. **Ring buffer + Dekker** — an accelerator for buffer 4+. When
   the buffer has space (put) or items (take), the Dekker fast path
   writes directly to the ring — no nodes, no CAS on queue
   structure. When the buffer is full/empty, fall through to the
   dual queue.

The ring buffer is an optimization to skip the queue when it's not
needed. The queue is always there underneath.

### Unified `alt`

This convergence enables `alt` uniformly. A consumer node in the
dual queue carries a CAS-able state field. For `alt` across
multiple channels:

```
alt(ch1, ch2):
  altState = AtomicInt(PENDING)     // shared across channels
  node1 = ConsumerNode(altState)    // enqueue on ch1's queue
  node2 = ConsumerNode(altState)    // enqueue on ch2's queue
  park()
  // whichever channel matches first CAS's altState PENDING → MATCHED
  // losing channel's node is stale — skipped on match attempt
```

With `ReentrantLock`, alt is hard because `Condition.await()` gives
no handle to the parked thread. The lock hides the wait node behind
an opaque API. With an exposed queue node, external cancellation
(from another channel in the alt firing first) is a single CAS.

This applies at every buffer size:
- Buffer 0–2: consumer always goes through the queue
- Buffer 4+: consumer goes through the queue when the buffer is
  empty (which is exactly when alt needs to park and wait)

The Dekker fast path (buffer 4+) only fires when a value is already
available — no parking, no alt needed. Alt hooks in precisely where
the fast path can't help.

### Why Not ReentrantLock

`ReentrantLock` and the dual queue use the same underlying
primitives: CAS on a tail pointer to enqueue, `LockSupport.park/
unpark` to sleep/wake. Both allocate per-wait (AQS nodes vs queue
nodes). The dual queue cuts out the middleman — the exchange happens
*in* the queue rather than *after acquiring a lock obtained through*
a queue.

The intrusive node is key: it carries value, thread, state, and
next pointer in one allocation. An AQS node is opaque — it only
serves the lock's bookkeeping. With locks you need the AQS node
*plus* your channel state. The dual queue node *is* the
coordination primitive.

This also makes `onto`, `seal(coll)`, and sentinels compose
naturally — they're all just nodes in the same linked structure.

## Channel Tier Summary

| Buffer | Fast path         | Parking / overflow | Non-parking put     |
|--------|-------------------|--------------------|---------------------|
| 0      | —                 | dual queue         | n/a (always parks)  |
| 1–2    | —                 | dual queue         | credit + CAS enqueue|
| 4+     | Dekker + ring buf | dual queue         | Dekker + ring slot  |

All tiers share the dual queue for parking. Buffer 4+ adds the ring
buffer as a fast path that bypasses the queue when space/items are
available.

Buffer 3 is a boundary case — could go either way. Benchmarking
will determine whether the ring buffer accelerator is worth it at
this size.

## Class Hierarchy

```
SmallChannel                  — dual queue, credits (buffer 0–2)
  +- SmallChannelXf?          — transducer support for buffer 1–2

BoundedChannel                — dual queue + ring buffer + Dekker (buffer 4+)
  +- BoundedChannelXf         — transducer put (buffer 4+)
```

Or, if the dual queue replaces locks in the adaptive channel:

```
AbstractChannel               — dual queue, credits, lifecycle sentinels
  +- SmallChannel             — no ring buffer (buffer 0–2)
  +- BoundedChannel           — ring buffer + Dekker accelerator (buffer 4+)
  +- BoundedChannelXf         — transducer put (buffer 4+)
```

The `chan` function selects the implementation based on buffer size:

```clojure
(defn chan
  ([n]
   (if (<= n 2)
     (SmallChannel. (int n))
     (BoundedChannel. (int n))))
  ([n xf]
   (if (<= n 2)
     (SmallChannelXf. (int n) xf)
     (BoundedChannelXf. (int n) xf))))
```

Both implement `IChannel` — the public API is unchanged.

## Open Questions

- **Buffer 3 cutoff**: should buffer 3 use the dual queue or ring
  buffer? Needs benchmarking. The ring buffer's power-of-2 rounding
  makes buffer 3 → capacity 4, which wastes a slot. The dual queue
  with 3 credits avoids this.

- **Node recycling**: for high-throughput MPMC, node allocation per
  operation could create GC pressure. Thread-local node caching
  (each thread reuses its node across operations) would eliminate
  allocation entirely. Worth adding if benchmarks show GC as a
  bottleneck. In Java, small short-lived objects are TLAB-allocated
  (bump pointer) and collected in young-gen — the cost may be
  comparable to a lock acquisition.

- **Queue structure**: linked list (SynchronousQueue-style) vs.
  array-based bounded queue. Linked list is simpler and handles
  arbitrary waiter counts. Array-based avoids allocation but needs
  a size bound.

- **Fairness**: the dual queue is naturally FIFO (match from HEAD
  outward). SynchronousQueue also offers an unfair (LIFO/stack)
  mode that has better throughput under contention due to cache
  locality. Worth considering if fairness isn't required.

- **Performance vs adaptive lock**: the dual queue eliminates lock
  acquisitions but adds per-operation node allocation and CAS on the
  queue structure. Whether this is a net win at buffer 1–2 (where
  the adaptive ring buffer already has a Dekker fast path for SPSC)
  is an empirical question. The design's value is clearest at
  buffer 0 where locks are unavoidable in the alternative.
