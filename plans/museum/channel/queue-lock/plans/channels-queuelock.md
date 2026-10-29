# Queue Lock Design

Lock-free queue that replaces `ReentrantLock` + `Condition` as the
parking infrastructure for bounded channels. The queue doubles as
the mutex — ownership is "my thread is on the owner field." No
separate lock construct, no AQS, no condition variables.

See `channels-adaptive.md` for the current adaptive design this
replaces the locking layer of, and `channels-small.md` for the
dual queue channel design that converges with this.

## Motivation

The adaptive channel uses `ReentrantLock` for its parking/overflow
path. Even uncontended, lock acquisition is a CAS on AQS state.
`Condition.await`/`signal` add further CAS operations and AQS queue
node allocations. For the handover case (producer parks, consumer
wakes it), the lock path involves multiple CAS operations on both
sides. Cross-lock signaling (producer acquires `takeLock` to signal
consumers, and vice versa) adds two lock acquisitions per exchange.

The queue lock replaces all of this with a single mechanism:

- **Uncontended**: one CAS to claim owner, plain write to clear.
  Allocation-free.
- **Handover**: plain write of successor's thread onto owner +
  unpark. No lock acquisition on either side.
- **Contended**: CAS-append to tail, park. Same cost as AQS
  enqueue, but the node is intrusive — it carries the channel
  operation's value, thread, and state. No separate AQS node.
- **Cross-side signaling**: just unpark the other side's owner.
  No lock acquisition. One unpark vs two lock acquisitions.

## Structure

The queue "head" is flattened into fields on the channel itself.
No sentinel node object, no indirection:

```
Producer side:
  volatile Thread producerOwner;   // who holds the producer lock (null = free)
  volatile Node   producerNext;    // first queued producer (null = empty queue)
  Node            producerTailHint;// best-effort tail pointer (plain field)
  long            tail;            // ring buffer write position

Consumer side:
  volatile Thread consumerOwner;   // who holds the consumer lock (null = free)
  volatile Node   consumerNext;    // first queued consumer (null = empty queue)
  Node            consumerTailHint;// best-effort tail pointer (plain field)
  long            head;            // ring buffer read position

Shared (between producer and consumer, cache-line padded):
  volatile int    count;           // items in buffer (VarHandle, acquire/release)
  volatile int    state;           // OPEN, SEALED, CANCELLED
  Object[]        buffer;          // ring buffer
  int             capacity;
  int             mask;
```

This looks almost identical to the current adaptive channel. The
`producerOwner` / `consumerOwner` fields already exist. We add
`producerNext` / `consumerNext` and remove `putLock` / `notFull` /
`takeLock` / `notEmpty`. The cache-line padding between producer
and consumer sides stays the same.

Queue nodes:

```
Node {
    volatile Thread thread;  // who to wake
    volatile Node next;      // successor (null = I am tail)
    Object value;            // the value to put (producer nodes)
}
```

The tail hint is best-effort — just a starting point for finding
the true tail. No CAS needed to update it; plain write. The true
tail is the node with `next == null`.

## Protocol

### Claim (uncontended)

```
put(v):
  if producerOwner == null:
    if CAS(producerOwner, null, self):
      return putWork(v)
  // Owner occupied — enqueue (see Contended below)
  return putEnqueue(v)
```

One CAS to claim. This is the only CAS in the uncontended path.

### Do Work

```
putWork(v):
  c = COUNT.getAcquire()
  if c >= capacity:
    // Buffer full — must park, wait for consumer to drain
    // CAS onto producerOwner was already done, so we're parked
    // as the owner. Consumer will unpark us directly.
    parkOnFull(v)
    return ...
  if state != OPEN: ...

  buffer[tail++ & mask] = v
  c = COUNT.getAndAddRelease(1)
  release(producerOwner, producerNext)
  if c == 0: wakeConsumer()
  return true
```

### Release

```
release():
  if producerNext != null:
    // Successor exists — hand off directly
    handover()
    return
  producerOwner = null           // plain write (single writer)
  // Dekker re-check: someone may have queued after our read
  if producerNext != null:
    // Someone queued while we were clearing
    if CAS(producerOwner, null, self):
      // Reclaimed — hand off to successor
      handover()
```

Check successor before clearing. Clear only when no successor.
Re-check after clearing — Dekker pattern. If someone queued
between the check and the clear, reclaim and hand off.

### Handover

```
handover():
  successor = producerNext
  producerNext = successor.next  // unlink (plain write, single writer)
  producerOwner = successor.thread  // plain write (single writer)
  unpark(successor.thread)
```

The owner field has a **single-writer invariant**: only the current
owner writes to it. Handover is a plain write — no CAS. The
successor wakes, sees itself as owner, proceeds.

The successor's thread is written *before* unpark. When the
successor wakes, it is guaranteed to see itself as owner.

### Enqueue (contended)

```
putEnqueue(v):
  node = Node(thread=self, next=null, value=v)
  loop:
    tail = findTail(producerTailHint)
    if CAS(tail.next, null, node):
      producerTailHint = node         // plain write
      break
    // CAS failed — retry from new tail

  park()
  // Woken — I'm now producerOwner
  return putWork(v)                   // or value already handled
```

### Repeat Owner (SPSC steady state)

Not possible — the owner clears itself on every release (a thread
that claims and never returns would block all subsequent arrivals).
The steady-state SPSC cost per operation is:

1. Read `producerOwner` — volatile read, see null
2. CAS(producerOwner, null, self) — claim
3. Do work
4. Write `producerOwner = null` — plain write (release)

One volatile read + one CAS + one plain write. No parking, no
allocation.

## Buffer Full / Empty: Cross-Side Signaling

When the buffer is full (producer) or empty (consumer), the owner
can't make progress. It must park and wait for the other side.

### Producer Parks on Full Buffer

```
parkOnFull(v):
  // We already hold producerOwner (CAS'd during claim).
  // Dekker: we claimed first, now check condition.
  // A consumer may have drained between our claim and this check.
  loop:
    c = COUNT.getAcquire()
    if c < capacity: break       // space freed, proceed
    if state != OPEN: return false
    park()
    // Woken by consumer (see wakeProducer below)

  // Space available — write
  buffer[tail++ & mask] = v
  c = COUNT.getAndAddRelease(1)
  release()
  if c == 0: wakeConsumer()
  return true
```

The producer is parked *as the owner* — its thread is on
`producerOwner`. The consumer can wake it with a direct unpark.
No condition variable, no lock acquisition.

The Dekker ordering is critical: the producer CAS's onto
`producerOwner` *before* checking count. If it checked count first
and then CAS'd, a consumer could drain between the check and the
CAS, call unpark before the producer has parked, and the wake is
lost. By claiming first: either the consumer sees the owner and
unparks it (producer wakes), or the producer sees the freed space
(no park needed). The volatile CAS on producerOwner and the
acquire read of count provide the ordering.

### Consumer Wakes Parked Producer

```
// In take path, after decrementing count:
if c == capacity:
  // Buffer was full, producer may be parked
  wakeProducer()

wakeProducer():
  t = producerOwner
  if t != null:
    unpark(t)          // wake parked owner directly
```

One volatile read + one unpark. Compare with the current design:
acquire `putLock`, call `notFull.signal()`, release `putLock` —
two lock operations.

The producer wakes, re-checks count (loop in `parkOnFull`), sees
space, writes, releases. If it was a spurious wake or another
producer filled the space, it re-parks.

### Consumer Parks on Empty Buffer — mirror

```
parkOnEmpty():
  // We hold consumerOwner.
  loop:
    c = COUNT.getAcquire()
    if c > 0: break              // value available, proceed
    if state >= SEALED: return CANCELLED
    park()
    // Woken by producer (see wakeConsumer below)

  // Read value
  v = buffer[head & mask]
  buffer[head++ & mask] = null
  c = COUNT.getAndAddRelease(-1)
  release()
  if c == capacity: wakeProducer()
  return v
```

### Producer Wakes Parked Consumer

```
// In put path, after incrementing count:
if c == 0:
  // Buffer was empty, consumer may be parked
  wakeConsumer()

wakeConsumer():
  t = consumerOwner
  if t != null:
    unpark(t)
```

Same pattern: one volatile read + one unpark.

### Cross-Signal Safety

Cross-side signaling writes nothing — it only reads the other
side's owner field and calls unpark. The owner field is only
*written* by the owning side (claim, release, handover). The
cross-signal is a read + unpark, which is safe concurrent with
the owning side's writes.

`LockSupport.unpark` is safe to call on a thread that isn't
parked yet — the permit is remembered. So if the consumer calls
`wakeProducer()` just before the producer calls `park()`, the
park returns immediately. The Dekker ordering (CAS owner before
checking count) ensures no lost wakes.

## Cost Summary

| Scenario             | CAS ops  | Volatile reads | Park/unpark | Allocation |
|----------------------|----------|---------------|-------------|------------|
| Uncontended put/take | 1 (claim)| 1 (owner)     | 0           | 0          |
| Buffer full/empty    | 0        | 1+ (count)    | 1 park      | 0          |
| Cross-side wake      | 0        | 1 (owner)     | 1 unpark    | 0          |
| Handover             | 0        | 0             | 1 unpark    | 0          |
| Contended enqueue    | 1 (tail) | 1+ (walk)     | 1 park      | 1 node     |
| Successor wakes      | 0        | 1 (owner)     | 0           | 0          |

Compare with current `ReentrantLock` path:

| Scenario             | CAS ops  | Lock acq | Condition ops | Allocation |
|----------------------|----------|----------|--------------|------------|
| Uncontended lock     | 1 (AQS)  | 1        | 0            | 0          |
| Park (await)         | 1+ (AQS) | 1        | 1 await      | 1 AQS node |
| Wake (signal)        | 1+ (AQS) | 1        | 1 signal     | 0          |
| Cross-lock signal    | 2+ (AQS) | 2        | 1 signal     | 0          |

The queue lock eliminates cross-lock signaling entirely. The
current design needs `signalNotEmpty` (acquire `takeLock` from
producer side) and `signalNotFull` (acquire `putLock` from consumer
side). With the queue lock: just read the other side's owner field
and unpark. One volatile read vs two lock acquisitions.

## Comparison with Current Adaptive

The current adaptive channel has three paths: fast (Dekker), slow
(first use / upgrade), and locked (post-contention). The queue lock
subsumes all three:

| Current adaptive          | Queue lock equivalent              |
|---------------------------|------------------------------------|
| `producerOwner` field     | `producerOwner` field (same)       |
| `putFastActive` flag      | eliminated (owner clears on exit)  |
| Dekker double-check       | CAS claim + release pattern        |
| `putFast()` lock-free     | `putWork()` lock-free (same)       |
| `putFastPark()` → lock    | `parkOnFull()` → park (no lock)    |
| `putSlow()` → claim/upgrade| `putEnqueue()` → CAS-append, park |
| `putLocked()` → lock      | successor wakes as owner (no lock) |
| CONTENDED (irreversible)  | eliminated (no mode switch)        |
| `putLock` ReentrantLock   | eliminated                         |
| `notFull` Condition       | eliminated                         |
| `signalNotEmpty()` cross  | `wakeConsumer()` — just unpark     |

The Dekker fast path (`putFastActive` flag, double-check) is
replaced by the claim/release cycle. Both are allocation-free and
lock-free in the uncontended case. The queue lock is simpler: one
CAS to claim vs volatile write + volatile read + volatile write +
volatile read (flag set, owner re-check, work, flag clear).

The irreversible CONTENDED upgrade is eliminated. A contended
channel can return to uncontended operation — if all but one
producer leave, the remaining one claims the empty owner slot on
every put. No permanent mode switch.

## Integration Path

1. **Replace `putLock`/`takeLock` in `AbstractBoundedChannel`.**
   Keep the ring buffer and count — only swap the parking
   infrastructure. Controlled experiment: same channel, new lock.

2. **Benchmark against current adaptive + core.async.** Expect:
   - Wins at small buffers (parking dominates, no cross-lock signal)
   - Comparable at large buffers (uncontended CAS ≈ Dekker handshake)
   - Wins at handover (plain write + unpark vs lock + signal)

3. **Once validated, extend to the dual queue channel.** The queue
   lock's node becomes the channel's data node. `onto` batches
   become node chains. Sentinels express seal/cancel. The queue
   lock generalizes into the full dual queue from
   `channels-small.md`.

## Relationship to Other Designs

- **`channels-adaptive.md`**: the queue lock replaces `ReentrantLock`
  in the adaptive channel's parking path. The ring buffer is
  unchanged. The ownership protocol (null → Thread → null/handover)
  is the same fields as today, minus the CONTENDED sentinel and
  Dekker flag.

- **`channels-small.md`**: the dual queue channel is the queue lock
  taken further — the queue is the buffer, the node is the value
  container. The queue lock is the shared foundation.

- **`channels-onto.md`**: atomic `onto` is a pre-built node chain,
  single CAS onto the queue tail. Integrates into the same linked
  structure.

- **`alt`**: the queue lock's node carries a CAS-able state field.
  Alt enqueues nodes on multiple channels' queues sharing a single
  atomic flag. Whichever channel hands over first wins. Falls out
  of the structure — no special alt support in the lock.

## Open Questions

- **Spin before park**: for small buffers where handover is fast,
  spinning briefly before parking could avoid the park/unpark cost.
  The queue position tells you how close you are to the front —
  spin if you're next, park if deeper. Tunable.

- **Fairness**: the queue is naturally FIFO. An unfair variant
  (LIFO/stack) could improve throughput under contention due to
  cache locality (hot thread stays hot). Worth benchmarking.

- **Node recycling**: contended operations allocate a node per
  enqueue. Thread-local node caching could eliminate allocation.
  TLAB allocation is already cheap — measure before optimizing.

- **Virtual thread friendliness**: `LockSupport.park/unpark` work
  with virtual threads (unmount/remount the carrier). The queue
  lock should be VT-friendly by construction, unlike `synchronized`
  which pins the carrier. Verify under Loom.

- **Tail sentinel**: `null` vs self-pointer for tail marker. The
  current doc uses `next == null` as tail marker. An alternative is
  the self-pointer (`next == this`) from the earlier design. Both
  work; `null` is simpler (no need to set `next = this` on
  construction). Self-pointer has the property that a detached node
  can be distinguished from a tail node, which may matter for
  cleanup.
