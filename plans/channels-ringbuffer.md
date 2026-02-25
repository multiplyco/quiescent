# Fixed Buffer Channel: Ring Buffer Design

Disruptor-inspired ring buffer for fixed buffer channels (buffer > 0).
Rendezvous (buffer = 0) is a separate implementation.

See `channels.md` for channel semantics (operations, structured
concurrency, task API, alt, etc.). This document covers the buffer
implementation only.

CLJ implementation. Virtual threads do the heavy lifting — `take`/`put`
park the virtual thread. No IOC needed. CLJS deferred — start with CLJ
as proof of concept.

## Class Hierarchy

```
AbstractBoundedChannel  — ring buffer, take(), parking, IBuffered
  ├─ BoundedChannel     — lock-free XADD put (MPMC)
  └─ BoundedChannelXf   — locked put + transducer support
```

`AbstractBoundedChannel` holds all shared state and logic: ring buffer
arrays, sequence counters, gates, three-tier parking, `take()`,
`tryWake()`, `signalGate()`, `IBuffered` methods, lifecycle stubs.
Subclasses provide only `put()`.

## Core Structure

Pre-allocated ring buffer. Power-of-2 sized array, Disruptor-style
sequence-based indexing. Four parallel arrays allocated once at
creation:

```
values:          Object[paddedSize]  // value slots
producerThreads: Object[paddedSize]  // producer waiter slots
consumerThreads: Object[paddedSize]  // consumer waiter slots
avail:           long[paddedSize]    // generation stamps (readiness + cycle disambiguation)
producerSeq:     long                // next write position (claim counter)
consumerSeq:     long                // next read position (claim counter)

ashft      = padded ? 3 : 0         // configurable: 0 = compact, 3 = stride 8 (64 bytes/slot)
paddedSize = size << ashft
pIdx       = (seq & mask) << ashft   // padded index into arrays
gen        = seq >>> sizeShift       // how many ring cycles for this slot
```

Sequence counters (`producerSeq`, `consumerSeq`) are always separated
by padding fields (`p01`..`p07`, etc.) to prevent false sharing. Array
stride padding is configurable — see "Cache-Line Padding" below.

Producer and consumer waiters use separate arrays
(`producerThreads[]`, `consumerThreads[]`) rather than a shared
`threads[]` array. This avoids type discrimination at runtime — the
producer side only ever writes/reads producer waiters, and vice versa.

Registry slots are initialized to a `CLEARED` sentinel object (not
`null`). This separates "no waiter registered" from "slot has never
been used." The Dekker CAS registers by writing `CAS(CLEARED, self)`
and clears by writing `CLEARED` back. `tryWake` checks `!= CLEARED`
to detect a parked thread.

Both `producerSeq` and `consumerSeq` are initialized to `size` (not
0). This makes the first generation = 1, and `avail[]` is initialized
to all `-1`s, meaning "consumed, free for generation 1." This avoids
the `-0 == 0` ambiguity in two's complement.

Readiness is per-slot via the `avail[]` generation array. The sign
encodes the state, the magnitude encodes the generation:

- **Positive `avail[P] == gen`**: published by producer at generation
  `gen`. Consumer at this generation can read.
- **Negative `avail[P] == -(gen+1)`**: consumed by consumer at
  generation `gen`. Producer at generation `gen+1` can write.

```
avail[P]:  -1 → 1 → -2 → 2 → -3 → 3 → ...
            ↑    ↑    ↑    ↑    ↑    ↑
          free  pub  free  pub  free  pub
          gen1  gen1 gen2  gen2 gen3  gen3
```

This replaces the `EMPTY` sentinel entirely. `null` is a valid
channel value (Clojure `nil`), and the generation check disambiguates
cycles without needing a sentinel in `values[]`.

Sequence counters are purely claim counters — never read by the
opposite side. `producerSeq` and `consumerSeq` are side-private,
eliminating cross-side cache coherence traffic on the hot counters.

The generation check prevents stale reads under oversubscription:
when the ring wraps and a consumer (or producer) on the same
physical slot from a previous cycle hasn't finished, the generation
won't match, and the arriving thread parks instead of reading a
stale value.

Zero allocation per operation. Contiguous memory for the CPU
prefetcher.

## Cache-Line Padding

Two independent forms of padding prevent false sharing:

**Field padding** (always on, ~280 bytes per channel). Seven `long`
fields between each volatile counter (`producerSeq`, `consumerSeq`,
gate waiter counts). Prevents the CPU from placing two hot counters on
the same 64-byte cache line. This is always included — the cost is
negligible and the benefit is large.

**Array stride padding** (configurable, `{:padded true}`). Over-
allocates arrays by 8× so each logical slot occupies its own 64-byte
cache line. Controlled by `ashft`: 0 = compact (no stride), 3 =
padded (stride 8). Memory cost for a 1024-buffer channel: ~21 KB
compact vs ~164 KB padded.

### When to use `:padded true`

Benchmark findings (1M ops, M3 Max):

| Scenario          | Compact | Padded | Effect              |
|-------------------|---------|--------|---------------------|
| 1P1C buf=1024     | 30 ms   | 21 ms  | **1.4× faster**     |
| 4P1C buf=1024     | 95 ms   | 64 ms  | **1.5× faster**     |
| XF map 1P1C       | 99 ms   | 57 ms  | **1.7× faster**     |
| XF map 4P4C       | 284 ms  | 234 ms | **1.2× faster**     |
| 1P1C buf=1        | 199 ms  | 260 ms | 1.3× slower (noise) |
| Ping-pong buf=1   | 19 ms   | 33 ms  | 1.8× slower (noise) |
| 50×1P1C parallel  | 32 ms   | 40 ms  | ~same               |
| 200×1P1C parallel | 68 ms   | 76 ms  | ~same               |

**Beneficial**: isolated high-throughput channels with moderate to
large buffers. Dedicated conveyor belts (e.g. DB pipeline, event
stream). The larger the buffer and the less parking, the more stride
padding helps — adjacent-slot false sharing becomes the bottleneck.

**Irrelevant**: parallel multi-channel workloads (50–200 channels).
Many channels compete for cache anyway; per-slot padding within each
channel doesn't move the needle. Memory savings from compact mode are
more valuable here.

**Irrelevant/harmful**: buf=1 and ping-pong scenarios. Parking and
wake latency dominate; the thread is rarely spinning on adjacent
slots. Extra memory pressure from 8× arrays can hurt.

**Guidance**: default to compact. Enable `:padded true` for long-lived
channels that serve as system-level pipelines where throughput is
critical and channel count is low.

### API

```clojure
(chan 1024)                                ; compact (default)
(chan 1024 {:padded true})                 ; stride-padded arrays
(chan 1024 (map inc))                      ; transducer, compact
(chan 1024 {:xf (map inc) :padded true})   ; transducer + padded
```

## Producer Side (BoundedChannel)

Always MPMC from the start. `getAndAdd` (XADD) claims a unique slot
in one atomic instruction — wait-free, no CAS loop:

```
put(V):
  slot = PRODUCER_SEQ.getAndAdd(this, 1)  // XADD, claim unique slot
  pIdx = (slot & mask) << ashft
  gen  = slot >>> sizeShift
  if avail[pIdx] != -gen:                  // slot not yet freed
    park(producerThreads, ...)             // three-tier parking
  values[pIdx] = V                         // volatile write
  avail[pIdx] = gen                        // volatile write (publish)
  tryWake(consumerThreads, pIdx)           // Dekker: wake parked consumer
  signalGate(consumerGate)                 // wake gated consumers
```

Each producer claims a unique slot via `getAndAdd`, then waits for
its specific slot to be free (generation matches). Out-of-order
writes are safe — each consumer checks its own slot's generation,
not a contiguous sequence. No lock needed.

## Consumer Side

Same structure. Always XADD:

```
take():
  slot = CONSUMER_SEQ.getAndAdd(this, 1)   // XADD, claim unique slot
  pIdx = (slot & mask) << ashft
  gen  = slot >>> sizeShift
  if avail[pIdx] != gen:                    // not published for my generation
    park(consumerThreads, ...)              // three-tier parking
  V = values[pIdx]                          // volatile read
  avail[pIdx] = -(gen + 1)                  // volatile write (consumed, free for next gen)
  tryWake(producerThreads, pIdx)            // Dekker: wake parked producer
  signalGate(producerGate)                  // wake gated producers
  return V
```

Readiness is per-slot via generation check: `avail[pIdx] == gen`
means the producer has published a value for this consumer's
generation. No `producerSeq` read needed.

After reading, the consumer writes `-(gen + 1)` to `avail[pIdx]`,
signaling "consumed, free for the next cycle's producer."

## Three-Tier Parking

Parking handles the case where a thread's slot isn't ready yet.
Three tiers, from fastest to most expensive:

**Tier 1 — Fast path.** `avail[pIdx]` already matches the expected
value. No parking at all — return immediately. This is the common
steady-state case when producer and consumer run at similar rates.

**Tier 2 — Dekker park.** `avail[pIdx]` is at `prevStep` (one
generation behind — I'm next in line for this slot). The thread
CAS-registers itself: `CAS(registry[pIdx], CLEARED, self)`. If the
CAS succeeds, the thread spins briefly (`SPIN_LIMIT` = 256 iterations),
then falls through to `LockSupport.park()`. The counterpart calls
`tryWake`: reads `registry[pIdx]`, if not `CLEARED` →
`LockSupport.unpark(thread)`. After wake, the thread writes `CLEARED`
back to the registry so the next generation can CAS in.

The Dekker invariant prevents lost wakeups: the waiter writes to
`registry[pIdx]` (volatile write), then reads `avail[pIdx]` (volatile
read). The counterpart writes `avail[pIdx]` (volatile write), then
reads `registry[pIdx]` (volatile read). Volatile total order
guarantees: either the waiter sees the state change, or the
counterpart sees the waiter. Both can see — an unpark of an
already-running thread is a no-op.

**Tier 3 — Gate.** `avail[pIdx]` is neither the expected value nor
`prevStep` — the thread is generationally far ahead (multiple ring
cycles behind). After spinning `SPIN_LIMIT` times, the thread
acquires the per-side `ReentrantLock` and calls `Condition.await()`.
The counterpart calls `signalGate`: volatile read on a waiter count
(`gateWaiters`), skip lock acquisition if zero, otherwise lock +
`signalAll()`. After wake, spins reset and the thread retries from
tier 1.

```
park(registry, pIdx, expectedAvail, prevStep, gateLock, gate, gateWaiters):
  spins = 0
  loop:
    current = avail[pIdx]                              // volatile read
    if current == expectedAvail: return                 // Tier 1: ready

    if current == prevStep:                             // Tier 2: next in line
      if CAS(registry[pIdx], CLEARED, self): break      //   register for Dekker
      onSpinWait(); continue                            //   prev gen clearing, brief spin

    if spins < SPIN_LIMIT: spins++; onSpinWait; continue
    gateLock.lock()                                     // Tier 3: gate
    gateWaiters++
    if avail[pIdx] != expectedAvail && != prevStep:
      gate.await()
    gateWaiters--
    gateLock.unlock()
    spins = 0; continue                                 // retry from Tier 1

  // Dekker park (registered):
  spin SPIN_LIMIT times checking avail[pIdx]
  if still not ready: LockSupport.park()
  registry[pIdx] = CLEARED                              // clear for next gen
```

### tryWake and signalGate

```
tryWake(registry, pIdx):
  occupant = registry[pIdx]          // volatile read
  if occupant != CLEARED:
    LockSupport.unpark(occupant)

signalGate(gateLock, gate, gateWaiters):
  if gateWaiters > 0:                // volatile read, skip lock if nobody waiting
    gateLock.lock()
    gate.signalAll()
    gateLock.unlock()
```

### Slot Lifecycle

```
         avail[pIdx]   producerThreads[pIdx]  consumerThreads[pIdx]  Phase
         ───────────   ─────────────────────  ─────────────────────  ─────
 idle:   -(gen)        CLEARED                CLEARED                free for producer gen
 cwait:  -(gen)        CLEARED                Thread                 consumer waiting for value
 full:   gen           CLEARED                CLEARED                value published, no waiter
 pwait:  gen           Thread                 CLEARED                producer waiting for slot
```

Transitions:

- Producer publishes (writes `avail = gen`) → `idle`/`cwait` becomes
  `full` (+ tryWake consumer if cwait)
- Consumer consumes (writes `avail = -(gen+1)`) → `full`/`pwait`
  becomes `idle` for next cycle (+ tryWake producer if pwait)
- Both sides call `signalGate` on the counterpart's gate after every
  transition, waking any tier-3 gated threads.

## Transducer Channels (BoundedChannelXf)

Transducer channels serialize all producers through a `ReentrantLock`
(`xfLock`) to protect stateful transducer state. The consumer side is
unchanged — same lock-free XADD + three-tier parking.

The transducer is a standard Clojure transducer wrapping an `AFn`
base reducing function whose step calls `putDirect()`:

```java
AFn baseRf = new AFn() {
    public Object invoke(Object acc) {
        return acc;
    }           // complete: no-op

    public Object invoke(Object acc, Object val) {             // step: ring buffer put
        putDirect(val);
        return acc;
    }
};
this.rf =(IFn)xf.

invoke(baseRf);
```

### put() — locked path

```
put(V):
  xfLock.lock()
  try:
    result = rf.invoke(this, V)       // xform step — may call putDirect 0/1/N times
    if isReduced(result):
      rf.invoke(this)                 // xform complete (flush stateful xforms)
      seal channel                    // (stub: throws UnsupportedOperationException)
  finally:
    xfLock.unlock()
```

### putDirect() — simplified single-producer ring buffer put

Since `xfLock` guarantees single-producer access, `putDirect` uses
relaxed memory ordering and simplified parking:

```
putDirect(V):
  slot = PRODUCER_SEQ.getOpaque(this)       // opaque read (not XADD — lock serializes)
  PRODUCER_SEQ.setOpaque(this, slot + 1)    // opaque write (lock provides happens-before)
  pIdx = (slot & mask) << ashft
  gen  = slot >>> sizeShift

  if avail[pIdx] != -gen:                    // slot not yet freed
    spin SPIN_LIMIT times
    if still blocked:
      producerThreads[pIdx] = self           // volatile write (no CAS — single producer)
      spin SPIN_LIMIT times                  // spin after registration
      if still blocked:
        while avail[pIdx] != -gen:
          LockSupport.park()
      producerThreads[pIdx] = CLEARED

  values[pIdx] = V                           // volatile write
  avail[pIdx] = gen                          // volatile write (publish)
  tryWake(consumerThreads, pIdx)
  signalGate(consumerGate)
```

**Why `getOpaque`/`setOpaque`?** The `xfLock` provides happens-before
between producers. Consumers never read `producerSeq` in the
value-transfer path — they rely on `avail[pIdx]` for publication. The
volatile stores on `avail` and `values` provide the full fences needed
for the producer→consumer handoff.

**Why no Dekker CAS?** Single-producer guaranteed by lock — no
concurrent writer to the same `producerThreads[pIdx]` slot. Direct
volatile write suffices.

**Why no gate (tier 3)?** With a single producer, at most one thread
is ever waiting for a given slot. No generational pileup.

**Why parking under xfLock is safe:** consumers free slots without
needing `xfLock`. Other producers block on the lock, not on the ring
buffer. Mapcat batches (multiple `putDirect` calls per input) stay
atomic — no interleaving from other producers.

### Transducer behaviors

| Transducer          | Effect in put()                                           |
|---------------------|-----------------------------------------------------------|
| `(map f)`           | baseRf called once with transformed value → 1 putDirect   |
| `(filter p)`        | baseRf called 0 or 1 times → 0 or 1 putDirect             |
| `(mapcat f)`        | baseRf called N times → N putDirects (may park mid-batch) |
| `(take n)`          | After n values, returns reduced → complete + seal         |
| `(partition-all n)` | Accumulates, emits vectors → putDirect with vector values |

## Mode Summary

```
                    Producer side              Consumer side
                    ─────────────              ─────────────
BoundedChannel:     getAndAdd (XADD)           getAndAdd (XADD)
                    three-tier parking          three-tier parking

BoundedChannelXf:   lock + getOpaque/setOpaque getAndAdd (XADD)
                    two-tier parking (no gate)  three-tier parking
```

One ring buffer implementation throughout (AbstractBoundedChannel).
The variables are:

- Producer claim: XADD (BoundedChannel) vs opaque under lock (BoundedChannelXf)
- Consumer claim: always XADD
- Readiness: per-slot generation check (`avail[pIdx]`), not sequence comparison
- Parking: `producerThreads[]`/`consumerThreads[]` + Dekker pattern + gate
- Lock: only for transducer serialization

## Same-Thread Ordering

Values produced by the same thread are always consumed in production
order. Cross-thread ordering is not guaranteed (and cannot be in a
concurrent system).

`getAndAdd` gives each `put` from the same thread a monotonically
increasing slot (a thread's second `put` cannot begin until its first
completes). A thread's values occupy increasing slot indices.

## Comparison to Other Designs

|                 | MPMC                       | Simplicity |
|-----------------|----------------------------|------------|
| **Go channels** | Lock (1 mutex, both sides) | Simple     |
| **core.async**  | Lock (1 mutex) + ArrayList | Simple     |
| **Disruptor**   | CAS + availability buffer  | Complex    |
| **JCTools**     | CAS per variant (4 impls)  | 4 impls    |
| **This design** | XADD + generation array    | 1 impl     |

Improves on Go and core.async by being lock-free on both sides.
Simplifies over Disruptor (XADD instead of CAS, sign-encoded
generation instead of separate availability buffer) and JCTools (one
implementation, not four). Lock only for transducer channels.

## Alt Integration (not yet implemented)

Alt works for both taking and putting. Alt-take selects which channel
to consume from. Alt-put selects which channel to produce to. Both
directions use the same `consumerThreads[]`/`producerThreads[]` arrays
and the same `AltState{thread, done}` mechanism — only one channel
can CAS `state.done` from `false → true`.

### Registry Slot Types (planned)

Each registry slot will contain one of:

- `CLEARED` — no waiter
- `Thread` — ordinary consumer or producer parked
- `AltWaiter(thread, state)` — alt consumer parked (waiting for value)
- `AltProducerWaiter(thread, state, value)` — alt producer parked (waiting for free slot)

Consumer-side waiters (Thread, AltWaiter) appear when `avail[pIdx]`
is negative (slot not yet published). Producer-side waiters appear
when `avail[pIdx]` is positive (previous cycle's value not yet
consumed). The dual queue invariant ensures these never coexist on
the same slot.

All `AltWaiter` nodes for the same alt share an
`AltState{thread, done}` object. The `done` flag is the cross-channel
serialization point — only one channel can CAS it `false → true`.

### Extended Producer Path (planned)

The producer writes the value, then reads `consumerThreads[pIdx]`
(Dekker check). The response depends on waiter type:

```
put(V):
  loop:
    slot = PRODUCER_SEQ.getAndAdd(1)
    pIdx = (slot & mask) << ashft
    gen  = slot >>> sizeShift
    if avail[pIdx] != -gen:
      park(producerThreads, ...)
    values[pIdx] = V                        // volatile write
    avail[pIdx] = gen                       // volatile write (publish)
    T = consumerThreads[pIdx]               // volatile read (Dekker check)

    if T == CLEARED:
      signalGate(consumerGate)
      return                                // value buffered, no waiter

    if T is Thread:
      consumerThreads[pIdx] = CLEARED
      unpark T                              // wake consumer, no CAS
      signalGate(consumerGate)
      return

    if T is AltWaiter:
      if CAS(T.state.done, false, true):    // claim the alt
        consumerThreads[pIdx] = CLEARED
        unpark T.thread
        signalGate(consumerGate)
        return
      // CAS failed: alt resolved elsewhere. Burned slot.
      avail[pIdx] = -(gen + 1)             // restore slot to free
      consumerThreads[pIdx] = CLEARED
      continue                              // loop: write V to next slot
```

Ordinary consumer (`Thread`): no CAS. Volatile write + unpark.

Alt consumer (`AltWaiter`): CAS on `state.done`. On success, wake
the alt thread. On failure, the slot is burned — the producer
restores it to free (`avail = -(gen+1)`, `consumerThreads = CLEARED`)
and loops, writing V to the next slot. The value itself is never lost.

### Consumer Path (planned)

```
take():
  slot = CONSUMER_SEQ.getAndAdd(1)
  pIdx = (slot & mask) << ashft
  gen  = slot >>> sizeShift
  if avail[pIdx] != gen:
    park(consumerThreads, ...)
  V = values[pIdx]                          // read value
  avail[pIdx] = -(gen + 1)                  // consumed, free for next gen
  T = producerThreads[pIdx]

  if T == CLEARED:
    signalGate(producerGate)
    return V

  if T is Thread:
    producerThreads[pIdx] = CLEARED
    unpark T
    signalGate(producerGate)
    return V

  if T is AltProducerWaiter:
    if CAS(T.state.done, false, true):      // claim the alt
      producerThreads[pIdx] = CLEARED
      unpark T.thread
      signalGate(producerGate)
      return V
    // CAS failed: alt resolved elsewhere. Burned slot.
    producerThreads[pIdx] = CLEARED
    signalGate(producerGate)
    return V
```

### Alt-Take Registration (planned)

```
alt(ch1, ch2, ...):
  state = AltState{thread: self, done: false}

  // Phase 1: scan for available values (speculative take)
  for each channel ch:
    cs = ch.consumerSeq
    gen = cs >>> ch.sizeShift
    pIdx = (cs & ch.mask) << ashft
    if ch.avail[pIdx] == gen:                // value published?
      if CAS(ch.consumerSeq, cs, cs + 1):    // claim slot
        V = ch.values[pIdx]
        ch.avail[pIdx] = -(gen + 1)          // consumed
        return V

  // Phase 2: claim slots and register AltWaiters
  for each channel ch:
    slot = ch.consumerSeq.getAndAdd(1)
    pIdx = (slot & ch.mask) << ashft
    ch.consumerThreads[pIdx] = AltWaiter(state)
    state.slots[ch] = slot

  // Phase 3: Dekker recheck per channel
  for each channel ch:
    slot = state.slots[ch]
    gen = slot >>> ch.sizeShift
    pIdx = (slot & ch.mask) << ashft
    if ch.avail[pIdx] == gen:                // value arrived?
      if CAS(state.done, false, true):
        V = ch.values[pIdx]
        ch.avail[pIdx] = -(gen + 1)
        return V

  // Phase 4: park, wait for producer to CAS state.done
  park
  // woken by a producer — scan state.slots to find deliverer
  return delivered value
```

Phase 1 uses CAS on `consumerSeq` (not `getAndAdd`) because it's
speculative — if the take fails (lost to a concurrent consumer), the
alt moves on without having claimed a slot.

Phase 2 uses `getAndAdd` to claim a definitive FIFO position on each
channel. The `AltWaiter` written to `consumerThreads[]` shares the
`AltState` across all channels.

Phase 3 is the Dekker recheck: the alt wrote to `consumerThreads[]`
(volatile write), now reads `avail[pIdx]` (volatile read). If a value
arrived between Phase 1 and Phase 2, the alt sees it here.

### Burned Slot Mechanics (Alt-Take)

When an alt-take resolves on one channel, its `AltWaiter`
registrations on other channels become stale. Cleanup is lazy —
producers handle it:

1. Producer reaches the slot, writes value, publishes `avail = gen`,
   reads `consumerThreads[pIdx]`.
2. Sees `AltWaiter`, CAS's `state.done` → fails (already `true`).
3. Producer restores slot to free: `avail[pIdx] = -(gen + 1)`,
   `consumerThreads[pIdx] = CLEARED`.
4. Loops, writes value to next slot.

Burned slots are immediately restored to free — no stale values
linger. The generation check is unaffected because the burned slot's
avail is set back to negative (free for next cycle).

The alt itself does not need to deregister from non-consumed channels.
Stale `AltWaiter` entries are cleaned up by producers when they
naturally arrive at those slots. This avoids cleanup races entirely.

### Alt-Put Registration (planned)

Mirror of alt-take. The alt producer scans channels for free slots,
then registers `AltProducerWaiter` entries on channels where the
buffer is full:

```
alt-put(V, ch1, ch2, ...):
  state = AltState{thread: self, done: false}

  // Phase 1: scan for free slots (speculative put)
  for each channel ch:
    ps = ch.producerSeq
    gen = ps >>> ch.sizeShift
    pIdx = (ps & ch.mask) << ashft
    if ch.avail[pIdx] == -gen:               // slot free?
      if CAS(ch.producerSeq, ps, ps + 1):    // claim slot
        ch.values[pIdx] = V
        ch.avail[pIdx] = gen                  // publish
        T = ch.consumerThreads[pIdx]
        if T != CLEARED: handle T             // wake consumer
        return true

  // Phase 2: claim slots and register AltProducerWaiters
  for each channel ch:
    slot = ch.producerSeq.getAndAdd(1)
    pIdx = (slot & ch.mask) << ashft
    ch.producerThreads[pIdx] = AltProducerWaiter(state, V)
    state.slots[ch] = slot

  // Phase 3: Dekker recheck per channel
  for each channel ch:
    slot = state.slots[ch]
    gen = slot >>> ch.sizeShift
    pIdx = (slot & ch.mask) << ashft
    if ch.avail[pIdx] == -gen:               // slot freed?
      if CAS(state.done, false, true):
        ch.values[pIdx] = V
        ch.avail[pIdx] = gen                  // publish
        // clear own waiter, handle consumer if present
        return true

  // Phase 4: park, wait for consumer to CAS state.done
  park
  // woken by a consumer — write V to the resolved channel's slot
  return true
```

### Burned Slot Mechanics (Alt-Put)

When an alt-put resolves on one channel, its `AltProducerWaiter`
registrations on other channels become stale. Cleanup is lazy —
consumers handle it:

1. Consumer frees the slot (writes `avail = -(gen+1)`), reads
   `producerThreads[pIdx]`.
2. Sees `AltProducerWaiter`, CAS's `state.done` → fails (already
   `true`).
3. Consumer clears `producerThreads[pIdx] = CLEARED`. Slot is already
   freed via avail — no further cleanup needed.

Simpler than alt-take burned slots: no value to un-write. The slot
is already in free state (negative avail + CLEARED registry).

### Properties

- **Symmetric**: alt-take and alt-put are mirrors. Same `AltState`
  mechanism, same burned slot cleanup (lazy, by the opposite side),
  same four-phase algorithm (speculate, register, Dekker recheck,
  park).
- **FIFO fair**: `getAndAdd` on sequence counters gives sequential
  slot assignment. Regular and alt participants in the same sequence.
  No structural priority of one type over another.
- **No value loss**: values are always delivered to a live slot.
  Burned alt-take slots are restored to idle immediately. Burned
  alt-put slots are already idle.
- **No starvation**: all participants compete in the same sequence
  order per side. Alt does not skip ahead or fall behind.
- **Minimal CAS**: ordinary consumers/producers need no CAS. CAS is
  only on the opposite side encountering an alt waiter (`AltWaiter`
  or `AltProducerWaiter`).

## Open Questions

- **Cancellation** (deferred): A consumer or producer that claimed a
  slot via `getAndAdd` and gets cancelled has a stale `Thread` in
  the registry. Counterpart encounters it, unparks a cancelled
  thread (no-op). The value in `values[pIdx]` is never read. Similar
  dynamics to a burned alt slot. Parked for now — the core operations
  will be internally uninterruptible first. Cancellation checks at
  defined breakoff points (before slot claim, after wake from park)
  will be layered on top once the core protocol is solid.

  **Known issue: interrupt spin.** `LockSupport.park()` returns
  immediately when the thread's interrupt flag is set — it does not
  throw. The interrupt flag is sticky (not cleared by `park`). A
  cancelled thread parked in the `while` loop (waiting for a slot
  to free or a value to arrive) will spin indefinitely: `park`
  returns immediately, the condition is still false, `park` again,
  returns again, etc. This must be addressed when cancellation is
  implemented — the park loop needs to check for interruption and
  bail out cleanly (recycle the slot, throw CancellationException).

  **Proposed resolution: handle interruption alongside alt.** The
  alt design introduces burned slot mechanics — a slot claimed but
  never consumed/produced, restored to free by the opposite side.
  Interrupted threads reuse this same mechanism:

    - *Interrupted producer*: wakes from park, checks interrupt flag,
      writes a burned marker (alt-style payload), exits. Consumer
      arriving at the slot sees the marker, restores slot to free,
      moves on.

    - *Interrupted consumer*: the producer arriving at the slot writes
      its value, then checks `Thread.isInterrupted()` on the parked
      consumer thread. If interrupted: treat as burned — restore
      `avail[pIdx] = -(gen+1)`, clear registry, loop with the value
      to the next slot. If not interrupted: unpark as normal. The race
      (interrupted between check and unpark) is harmless — consumer
      wakes, takes the value, discovers interruption on its next
      operation. Value delivered, no loss. Post-delivery interruption
      is the consumer's responsibility, not the channel's.

  Implement after alt, since the burned slot codepath is shared.

- **Slot oversubscription** (partially addressed): When concurrent
  producers (or consumers) exceed buffer size, multiple logical slots
  map to the same physical registry entry. The tier-3 gate handles
  this by parking generationally-far-ahead threads on a shared
  Condition rather than competing for the same registry slot. For
  further scaling, escalating waiter chaining in the registry is
  planned:

    1. **No waiter**: `registry[pIdx] = CLEARED`. Fast path.
    2. **One waiter**: CAS `registry[pIdx]` from `CLEARED` to raw
       `Thread`. No allocation. This is the common case (buffer >=
       concurrency).
    3. **Second waiter arrives**: CAS `registry[pIdx]` from `Thread` to
       `ConcurrentLinkedQueue` containing the incumbent thread and self.
    4. **Subsequent arrivals**: enqueue into existing CLQ.

  Sizing guidance: buffer should be at least the expected concurrency
  level for optimal performance. The `avail[]` generation check
  ensures correctness either way.

- **Buffer sizing and naming**: Buffer size must be a power of 2
  (enables bitwise AND indexing on the hot path). Requested sizes are
  rounded up to the next power of 2. This is exposed as `bounded` —
  communicating "approximately this size" rather than "exactly this
  size." If exact sizing is ever needed (e.g. `(chan (fixed 3))` for
  exactly 3 slots), a separate `fixed` buffer type can be introduced
  using modulo indexing, transparently upgrading to the fast
  `bounded` implementation when the requested size happens to be a
  power of 2. Until a real use case surfaces, only `bounded` exists.

- **Rendezvous channel (buffer = 0)** (deferred): Separate
  implementation — no value buffer, purely direct handoff. Dual
  queue semantics: either producers or consumers are waiting, never
  both. Pre-allocated registry sized to expected concurrency
  (default: physical core count) for allocation-free parking. CLQ
  overflow if concurrency exceeds array size. Same escalation as
  fixed buffer: CLEARED → Thread → CLQ. Unlike fixed buffer, sizing
  is purely about concurrency (no throughput/buffering dimension).
