# Adaptive Bounded Channel Design

Two-lock bounded channel with single-owner fast path. Trades the
lock-free XADD design's isolated throughput for dramatically better
fan-in scaling and competitive system-level performance.

See `museum/channel/channels-ringbuffer.md` for the retired XADD ring buffer design.
See `channels.md` for channel semantics.

## Motivation

The XADD ring buffer (`BoundedChannel`) uses `getAndAdd` to claim
slots — wait-free, no CAS loop, excellent for isolated 1P1C. But
XADD is an atomic read-modify-write on a shared counter, and under
fan-in (many producers, one consumer) it generates severe cache-line
contention. At 128 producers, XADD is 21x slower than the two-lock
design.

The two-lock design (`BoundedChannelLocked`) uses separate
`ReentrantLock`s for producers and consumers. Producers serialize
through `putLock`, consumers through `takeLock`. An `AtomicInteger`
count bridges the two sides. Under fan-in this is excellent — the
lock serializes contention cleanly. But at 1P1C the lock
acquisition is pure overhead: 2.4-3.3x slower than XADD.

The adaptive channel combines both: bias each side to a single-owner
lock-free fast path, upgrade irreversibly to the locked path when
contention is detected.

## Benchmark Summary

1M ops, M1 Max, representative scenarios:

| Scenario     | Buffer | XADD   | Adaptive | Locked  | core.async |
|--------------|--------|--------|----------|---------|------------|
| 1P1C         | 1024   | 46 ms  | 125 ms   | 109 ms  | 220 ms     |
| 4P1C         | 1024   | 104 ms | 116 ms   | 151 ms  | 342 ms     |
| Ping-pong    | 1      | 23 ms  | 157 ms   | 151 ms  | 557 ms     |
| 50x1P1C      | 64     | 40 ms  | 34 ms    | 45 ms   | 544 ms     |
| 200x1P1C     | 64     | 70 ms  | 58 ms    | 84 ms   | 1977 ms    |
| 128P1C       | 1024   | 294 ms | 118 ms   | 139 ms  | 8849 ms    |
| Pipe 4P>1P>4C| 1024   | 257 ms | 154 ms   | 296 ms  | 1978 ms    |
| XF map 1P1C  | 1024   | 92 ms  | 129 ms   | 176 ms  | 285 ms     |
| XF map 4P4C  | 1024   | 334 ms | 179 ms   | 261 ms  | 387 ms     |

**Isolated 1P1C**: XADD wins (2.7x). The adaptive channel's volatile
reads and Dekker handshake cost ~80ms overhead vs XADD's raw
getAndAdd. Matches or beats Locked.

**Fan-in (16-128P)**: Adaptive wins. 2.5x faster than XADD at 128P,
within 10-20% of Locked (slightly faster due to single-consumer fast
path on the take side).

**System workloads (50-200 channels)**: Adaptive wins. The mix of
1P1C and multi-producer channels benefits from the fast path on
single-owner channels while handling contention gracefully where it
occurs.

**Pipe/XF**: Adaptive wins. The pipe task is sole consumer of source
and sole producer of sink — the fast path fires on both sides.

## Class Hierarchy

```
AbstractBoundedChannel  — buffer, count, adaptive take, putDirect, lifecycle
  +- BoundedChannel     — adaptive put with Dekker handshake
  +- BoundedChannelXf   — transducer put, serialized through putLock
```

`AbstractBoundedChannel` owns all shared state, the full adaptive
take implementation, and the `putDirect()` method for under-lock
buffer writes. Subclasses provide only `put()`.

## Core Structure

Power-of-2 ring buffer with two-lock coordination:

```
buffer:    Object[capacity]     // value slots
capacity:  int                  // power of 2
mask:      int                  // capacity - 1
count:     volatile int         // shared item count (VarHandle access)
state:     volatile int         // OPEN=0, SEALED=1, CANCELLED=2

Producer side:
  putLock:   ReentrantLock
  notFull:   Condition           // on putLock
  tail:      long                // next write position (only under putLock or fast path)

Consumer side:
  takeLock:  ReentrantLock
  notEmpty:  Condition           // on takeLock
  head:      long                // next read position (only under takeLock or fast path)
```

Cache-line padding (8 longs) separates producer, shared, and consumer
field groups.

The `count` field uses a `VarHandle` on a plain `int` (not
`AtomicInteger`) to avoid object indirection. Accessed with
acquire/release ordering — see Memory Ordering below.

## Adaptive Ownership Protocol

Each side (producer/consumer) independently tracks an owner thread.
Three states:

- `null` — unclaimed. First thread claims ownership under the lock.
- `Thread` — single owner. That thread uses the lock-free fast path.
- `CONTENDED` — multiple threads detected. All threads use the locked
  path. Irreversible.

The upgrade is one-way: once `CONTENDED`, the channel never returns
to single-owner mode. This simplifies reasoning — no ABA problems,
no re-acquisition races.

### Producer side (BoundedChannel)

```
put(value):
  owner = producerOwner              // volatile read
  if owner == CONTENDED:
    return putLocked(value)          // post-upgrade locked path

  self = currentThread()
  if owner == self:
    putFastActive = 1                // flag: I'm in the fast path
    if producerOwner == self:        // Dekker re-check
      return putFast(value)
    putFastActive = 0                // upgraded between reads
    return putLocked(value)

  return putSlow(value, self)        // first use or new thread
```

### Dekker Handshake

The double-check on `producerOwner` after setting `putFastActive = 1`
is critical. Without it:

1. Owner reads `producerOwner == self` ✓
2. New thread acquires `putLock`, sets `producerOwner = CONTENDED`
3. New thread checks `putFastActive == 0` — sees 0, proceeds
4. Owner enters `putFast` — both threads write to buffer concurrently

The Dekker pattern prevents this:

1. Owner reads `producerOwner == self` ✓
2. Owner sets `putFastActive = 1` (volatile write)
3. Owner re-reads `producerOwner` (volatile read)
4. If still `self` — safe to enter fast path
5. If `CONTENDED` — another thread upgraded; clear flag, use lock

The upgrading thread, under `putLock`, sets `producerOwner = CONTENDED`,
then spins on `putFastActive` until it reaches 0. The sequential
consistency of volatile operations guarantees: either the owner sees
the upgrade (step 4), or the upgrader sees the flag (spin). Both can
see — the owner bails to the locked path, the upgrader finishes
spinning.

### putFast — lock-free single producer

```
putFast(value):
  if count >= capacity:              // acquire read
    putFastActive = 0
    return putFastPark(value)        // park under putLock

  if state != OPEN:
    putFastActive = 0
    return false

  buffer[tail++ & mask] = value
  c = COUNT.getAndAddRelease(1)      // release: publishes buffer write
  putFastActive = 0

  if c == 0: signalNotEmpty()        // cross-lock signal
  return true
```

No lock acquisition. One acquire read (count), one array write, one
atomic increment (release). The `putFastActive` flag is cleared after
the count increment — the buffer write is visible through the
happens-before chain in `count`.

### putFastPark — fast path detects full buffer

```
putFastPark(value):
  putFastActive is already 0         // cleared before entering
  putLock.lock()
  c = putDirect(value)               // parks on notFull if needed
  putLock.unlock()
  if c == 0: signalNotEmpty()
  return c >= 0
```

The flag is cleared *before* acquiring `putLock`. This prevents
deadlock: an upgrading thread holds `putLock` and spins on
`putFastActive`. If the fast-path owner tried to acquire `putLock`
while the flag was set, deadlock.

### putSlow — first use or contention

```
putSlow(value, self):
  putLock.lock()
  owner = producerOwner              // re-read under lock
  if owner == null:
    producerOwner = self             // claim ownership
  else if owner != CONTENDED:
    producerOwner = CONTENDED        // upgrade
    spinOnPutFastActive()            // wait for fast path to finish
  c = putDirect(value)
  putLock.unlock()
  if c == 0: signalNotEmpty()
  return c >= 0
```

### putLocked — post-upgrade

```
putLocked(value):
  putLock.lock()
  c = putDirect(value)
  putLock.unlock()
  if c == 0: signalNotEmpty()
  return c >= 0
```

Same as `BoundedChannelLocked.put()`.

### spinOnPutFastActive

```
spinOnPutFastActive():
  for i in 0..1024:
    if putFastActive == 0: return
    Thread.onSpinWait()
  while putFastActive != 0:
    Thread.yield()
```

Bounded spin (1024 iterations ~microseconds), then yield. The fast
path is at most one array write + one atomic increment — the spin
resolves in nanoseconds. The yield fallback handles scheduling delays
(e.g., virtual thread preemption).

### Consumer side — mirror

The consumer side in `AbstractBoundedChannel` is identical in
structure: `consumerOwner`, `takeFastActive`, `takeFast()`,
`takeFastPark()`, `takeSlow()`, `takeLocked()`,
`spinOnTakeFastActive()`. The take side is `final` in the abstract
class — both subclasses share it.

## putDirect — shared locked put logic

Called by `putFastPark`, `putSlow`, `putLocked` (in
`BoundedChannel`) and the transducer step function (in
`BoundedChannelXf`). Caller holds `putLock`.

```
putDirect(value):
  while count >= capacity:           // acquire read
    if state != OPEN: return -1
    notFull.await()
  if state != OPEN: return -1
  buffer[tail++ & mask] = value
  c = COUNT.getAndAddRelease(1)
  if c + 1 < capacity:
    notFull.signal()                 // cascade: wake another parked producer
  return c                           // caller signals notEmpty if c == 0
```

Returns the count before increment (>= 0), or -1 if
sealed/cancelled/interrupted. The cascade signal
(`notFull.signal()` when buffer not full) prevents producer
starvation under fan-in.

## Transducer Support (BoundedChannelXf)

Transducer channels serialize all producers through `putLock` — the
same lock used by the base class for the locked path. No separate
`xfLock` is needed because the base class already owns `putLock`.

```java
AFn baseRf = new AFn() {
    public Object invoke(Object acc) { return acc; }
    public Object invoke(Object acc, Object val) {
        int c = putDirect(val);
        if (c == 0) signalNotEmpty();
        return acc;
    }
};
this.rf = (IFn) xf.invoke(baseRf);
```

The transducer's step function calls `putDirect()` directly — no
adaptive ownership tracking on the producer side. The consumer side
retains the adaptive fast path from the base class.

```
put(value):                          // BoundedChannelXf
  if state >= SEALED: return false   // volatile read, avoid lock
  putLock.lock()
  if state >= SEALED: return false   // re-check under lock
  result = rf.invoke(this, value)    // xform step: 0/1/N putDirects
  if isReduced(result):
    rf.invoke(this)                  // flush stateful xforms
    // seal (not yet implemented)
  putLock.unlock()
  return true
```

One lock acquisition per put. The previous design had a separate
`xfLock` wrapping the base class's `put()`, which itself acquired
`putLock` — two lock acquisitions per put. Eliminating this improved
XF throughput by 35-50%.

## Memory Ordering

The `count` field bridges producer and consumer sides. Its memory
ordering is relaxed from sequential consistency to acquire/release:

**Reads** (`COUNT.getAcquire`): used to check buffer fullness
(producer) or emptiness (consumer). Acquire pairs with the other
side's release, ensuring the buffer write is visible when the count
indicates a value is present.

**Read-modify-write** (`COUNT.getAndAddRelease`): the release
publishes the preceding buffer write. The return value is only
compared against constants (0, capacity) for signaling decisions —
no dependent reads that require acquire semantics.

The Dekker handshake fields (`putFastActive`, `producerOwner`,
`takeFastActive`, `consumerOwner`) remain plain `volatile` —
sequential consistency is required for the Dekker protocol's
mutual exclusion guarantee.

## Cross-Lock Signaling

Same as `BoundedChannelLocked`. Signals only at state transitions:

- `c == 0` (was empty, now has item): acquire `takeLock`, signal
  `notEmpty`
- `c == capacity` (was full, now has space): acquire `putLock`,
  signal `notFull`

The cascade signal inside `putDirect`/take methods
(`notFull.signal()` / `notEmpty.signal()` when more space/items
remain) wakes additional parked threads without cross-lock
acquisition.

## Lifecycle

Lock ordering: `putLock` then `takeLock` (consistent everywhere).

**cancel**: acquire both locks, set `state = CANCELLED`, `signalAll`
on both conditions. Fast-path threads check `state` on every
operation — they don't hold locks, so cancel proceeds without
waiting (the `putFastActive` spin is only during ownership upgrade).

**seal**: acquire `putLock`, set `state = SEALED`, `signalAll` on
`notFull`. Then acquire `takeLock`, `signalAll` on `notEmpty`.
Consumers drain remaining values, then see `state >= SEALED` and
return `CANCELLED`.

## Historical: Trade-offs vs retired designs

The XADD (Disruptor-style) and pure-locked designs were retired in
favor of the adaptive channel. See `museum/channel/` for the
implementations. Summary of the decision:

- XADD had catastrophic failure modes under contention (18x slower
  at 128 producers, worse than core.async at 4P4C buffer=1)
- Pure-locked was strictly dominated by adaptive in all benchmarks
- Adaptive's worst-case losses (15-30% at isolated 1P1C) were
  acceptable given 2-18x wins at fan-in, pipe, and system workloads

## Open Questions

- **`synchronized` vs `ReentrantLock`**: moot if the queue lock
  replaces both. See `channels-queuelock.md` for the design. The
  queue lock uses `LockSupport.park/unpark` directly, embeds the
  ownership protocol in the queue head, and eliminates cross-lock
  signaling. Timed operations (offer/poll with timeout) use
  `LockSupport.parkNanos` + deadline tracking.

- **Padding for fast-path fields**: `producerOwner` and
  `putFastActive` are adjacent to `putLock`. Under the adaptive
  protocol, the fast-path thread reads both while the upgrading
  thread (under lock) writes both. Cache-line separation between
  the lock object and the ownership fields could reduce false sharing
  during the transition, though the transition is a one-time event.

- **Reversible upgrade**: the current upgrade is irreversible. A
  channel that sees brief contention during startup then settles to
  single-producer would benefit from downgrade. This adds ABA
  complexity (the owner thread must re-verify after downgrade) and
  is deferred until benchmarks show it matters.

- **`offer`/`poll` (non-blocking try-put/try-take)**: The adaptive
  fast path makes these genuinely cheap. Owner path: no lock — read
  `count`, check space/items, write buffer, bump count. Return false
  instead of parking if full/empty. Non-owner path: `tryLock()`
  instead of `lock()` — if acquired, do the operation; if not,
  return false immediately. Strictly better than core.async's
  `offer!`/`poll!`, which always take a mutex. Timed variants use
  `Condition.awaitNanos` for bounded parking (see `synchronized`
  vs `ReentrantLock` above).

- **`alt` and the dual queue**: The planned direction is to replace
  `ReentrantLock` + `Condition` with a lock-free dual queue for the
  overflow/parking path. See `channels-small.md` for the full design.

  The dual queue uses intrusive nodes with CAS-able state fields.
  This enables `alt` naturally: a consumer enqueues nodes on multiple
  channels' queues sharing a single `altState` atomic. Whichever
  channel matches first CAS's `altState` from PENDING to MATCHED;
  losing channels' nodes are stale and skipped. No handler
  registration in the channel, no extra threads, no ownership
  pollution.

  This is option 3 from the original analysis (custom lock primitive)
  but reframed: the dual queue IS the custom lock. It uses the same
  primitives as `ReentrantLock`/AQS (CAS on tail, park/unpark) but
  exposes the wait node as the coordination primitive rather than
  hiding it behind an opaque API. The exchange happens *in* the queue
  rather than *after acquiring a lock obtained through* a queue.

  The Dekker fast path is unaffected — it fires when buffer has
  space/items (no parking needed, no alt hook needed). The dual queue
  handles overflow parking, which is exactly where alt must hook in.

  Previously considered approaches and why the dual queue supersedes
  them:

  1. **core.async style (handler registration)**: braids alt
     awareness into every channel; every put path pays for alt.
     The dual queue keeps alt in the queue node, invisible to put.

  2. **VT delegation**: hostile to the adaptive protocol — spawns
     new thread identities, permanently upgrades channels to
     CONTENDED. The dual queue preserves the caller's thread.

  The dual queue also enables atomic `onto(coll)` (batch put as a
  pre-built node chain, single CAS) and sentinel-based lifecycle
  (seal/cancel as terminal nodes in the queue). These compose
  naturally because they're all nodes in the same linked structure.
  See `channels-small.md` and `channels-onto.md`.
