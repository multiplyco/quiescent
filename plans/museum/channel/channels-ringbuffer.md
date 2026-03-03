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

Benchmark findings (1M ops, M1 Max):

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

## Buffer Sizing and Throughput

Buffer size controls more than capacity — it determines how often
threads park and how well the system absorbs scheduling jitter.
Larger buffers keep operations on the tier 1 fast path (volatile
read matches → no parking), reducing park/wake cycles.

### Isolated channels

In isolation, buffer size has a dramatic effect on throughput.
Benchmark: single 4P→pipe→4C pipeline, 1M ops (M1 Max):

| Buffer | Mean     | vs core.async |
|--------|----------|---------------|
| 16     | 1481 ms  | 3.1×          |
| 64     | 752 ms   | 3.9×          |
| 1024   | 232 ms   | 9.6×          |

Each 16× increase in buffer roughly halves latency. The pipe task
(sole consumer of source, sole producer of sink) runs wait-free —
the buffer determines how long producers and consumers run
uninterrupted before the pipe must park waiting for more work or
free slots.

With a transducer on the source channel (`map inc`), throughput
improves further — the xfLock serializes producers, reducing XADD
contention on `producerSeq`:

| Buffer | Plain    | XF       |
|--------|----------|----------|
| 16     | 1481 ms  | 1168 ms  |
| 64     | 752 ms   | 630 ms   |
| 1024   | 232 ms   | 209 ms   |

### Under contention

In a contended system (many channels, many threads), the buffer
size advantage narrows. The scheduler is already interrupting
sequential runs for other work, and cache lines are being evicted
by unrelated channels. The buffer still helps but with diminishing
returns.

Benchmark: 20 concurrent 4P→pipe→4C pipelines, 100K ops each
(180 virtual threads total):

| Buffer | Mean    | vs core.async |
|--------|---------|---------------|
| 64     | 55 ms   | 73×           |
| 1024   | 25 ms   | 29×           |

buf=1024 is still 2× faster in absolute terms, but the speedup
ratio over core.async is actually higher at buf=64 (73× vs 29×).
This is because core.async's mutex-based implementation degrades
catastrophically under thread contention — 20 pipelines × 9 go
blocks each overwhelm the lock. Quiescent's lock-free XADD scales
cleanly.

### Aggregate throughput ceiling

Across different topologies, the system saturates around 80–90M
ops/sec aggregate on M1 Max:

| Scenario          | Total ops | Time   | Ops/sec |
|-------------------|-----------|--------|---------|
| 1P1C buf=1024     | 1M        | 32 ms  | 31M     |
| 50×4P4C buf=64    | 5M        | 57 ms  | 88M     |
| 20×pipe buf=1024  | 2M        | 25 ms  | 80M     |

Single-channel throughput peaks at ~31M ops/sec. Multi-channel
workloads reach ~80–90M ops/sec by keeping all cores fed via the
ForkJoinPool work-stealing scheduler. Beyond this point, the
channel implementation is no longer the bottleneck — the virtual
thread scheduler is.

For reference, the LMAX Disruptor advertises ~100M ops/sec with
dedicated pinned threads and busy-spinning (no parking). Reaching
80–90% of that throughput on virtual threads with park/unpark
scheduling overhead suggests the channel design is efficient enough
that further gains require scheduler-level changes, not channel-
level changes.

### Guidance

Two regimes:

**Dedicated pipelines** (isolated, high-throughput, few channels):
large buffers (256–1024+) and `:padded true`. The channel is a
system-level conveyor belt with dedicated threads feeding it.
Buffer size and stride padding compound — larger buffers reduce
parking, padding eliminates adjacent-slot false sharing. This is
the regime where the channel approaches Disruptor-class throughput.

**Regular channels** (many channels, shared virtual thread pool):
modest buffers (32–128), no stride padding. The scheduler is
already interrupting sequential runs for other work, and cache
lines are evicted by unrelated channels. Larger buffers still help
but with diminishing returns, and stride padding adds memory
pressure without measurable benefit.

In both cases, buffer size should be at least the expected
concurrency level (number of concurrent producers + consumers) to
avoid tier 3 gate contention.

## Producer Side (BoundedChannel)

Always MPMC from the start. `getAndAdd` (XADD) claims a unique slot
in one atomic instruction — wait-free, no CAS loop:

```
put(V):
  slot = PRODUCER_SEQ.getAndAdd(this, 1)  // XADD, claim unique slot
  if slot < 0: return false               // sealed or cancelled
  pIdx = (slot & mask) << ashft
  gen  = slot >>> sizeShift
  if avail[pIdx] != -gen:                  // slot not yet freed
    if !park(producerThreads, ...):        // three-tier parking (returns false on cancel)
      return false
  values[pIdx] = V                         // volatile write
  avail[pIdx] = gen                        // volatile write (publish)
  tryWake(consumerThreads, pIdx)           // Dekker: wake parked consumer
  signalGate(consumerGate)                 // wake gated consumers
  return true
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
  if slot < 0: return CANCELLED            // cancelled
  pIdx = (slot & mask) << ashft
  gen  = slot >>> sizeShift
  if avail[pIdx] != gen:                    // not published for my generation
    if !park(consumerThreads, ...):         // three-tier parking (returns false on cancel/seal)
      return CANCELLED
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
park(registry, pIdx, expectedAvail, prevStep, gateLock, gate, gateWaiters) → boolean:
  spins = 0
  loop:
    current = avail[pIdx]                              // volatile read
    if current == expectedAvail: return true            // Tier 1: ready
    if current == CANCELLED_AVAIL: return false         // cancelled

    if current == prevStep:                             // Tier 2: next in line
      witness = CEX(registry[pIdx], CLEARED, self)     //   compareAndExchange
      if witness == CLEARED: break                     //   registered for Dekker
      if witness == CANCELLED:                         //   sealed — spin for in-flight
        spin SPIN_LIMIT checking avail[pIdx]
        return avail[pIdx] == expectedAvail
      onSpinWait(); continue                           //   prev gen clearing, brief spin

    if spins < SPIN_LIMIT: spins++; onSpinWait; continue
    gateLock.lock()                                     // Tier 3: gate
    gateWaiters++
    if avail[pIdx] == CANCELLED_AVAIL: return false     //   cancelled under lock
    if avail[pIdx] != expectedAvail && != prevStep:
      gate.await()
    gateWaiters--
    gateLock.unlock()
    spins = 0; continue                                 // retry from Tier 1

  // Dekker park (registered):
  spin SPIN_LIMIT times checking avail[pIdx]
    if CANCELLED_AVAIL: clear registry, return false
  if still not ready:
    while true:
      if avail[pIdx] == expectedAvail: break
      if avail[pIdx] == CANCELLED_AVAIL: return false   // cancelled while parked
      LockSupport.park()
      if registry[pIdx] != self: return false            // seal replaced our entry
  registry[pIdx] = CLEARED                              // clear for next gen
  return true
```

### tryWake and signalGate

```
tryWake(registry, pIdx):
  occupant = registry[pIdx]          // volatile read
  if occupant != CLEARED && occupant != CANCELLED:
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
  if PRODUCER_SEQ < 0: return false   // sealed or cancelled (volatile read)
  xfLock.lock()
  try:
    if PRODUCER_SEQ < 0: return false // re-check under lock
    result = rf.invoke(this, V)       // xform step — may call putDirect 0/1/N times
    if isReduced(result):
      rf.invoke(this)                 // xform complete (flush stateful xforms)
      seal channel                    // (stub: throws UnsupportedOperationException)
  finally:
    xfLock.unlock()
  return true
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
    if avail[pIdx] == CANCELLED_AVAIL: return // cancelled
    spin SPIN_LIMIT times
      if CANCELLED_AVAIL: return
    if still blocked:
      producerThreads[pIdx] = self           // volatile write (no CAS — single producer)
      spin SPIN_LIMIT times                  // spin after registration
        if CANCELLED_AVAIL: clear, return
      if still blocked:
        while avail[pIdx] != -gen:
          if avail[pIdx] == CANCELLED_AVAIL: clear, return
          LockSupport.park()
          if producerThreads[pIdx] != self: return  // cancel replaced entry
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

## Cancellation and Sealing

The channel is a state machine that emits sentinel values. The
Clojure API layer (`take!`/`put!`, `poll`) interprets sentinels and
translates them into the appropriate response. The channel itself
does not know about callsites.

### Sentinels

- `CANCELLED_AVAIL = Long.MIN_VALUE` — long sentinel for `avail[]`
- `IChannel.CANCELLED` — Object sentinel for registry arrays and
  `take()` return value

### Cancel — stop everything, discard buffered values

Signals are inserted into data paths already read on the hot path,
so non-cancelled operations pay only for branch prediction on
always-false conditions (zero new memory access).

```
cancel(msg):
  prev = CONSUMER_SEQ.getAndSet(Long.MIN_VALUE)
  if prev < 0: return false          // already cancelled
  PRODUCER_SEQ.setVolatile(Long.MIN_VALUE)
  fill avail[] with CANCELLED_AVAIL  // volatile writes, strided
  wakeAllAndPoison(producerThreads)  // getAndSet(CANCELLED), unpark
  wakeAllAndPoison(consumerThreads)
  signalAll on both gates
  return true
```

Detection points (all on existing reads):

1. `slot = SEQ.getAndAdd(1)` → negative → bail. Sign check on a
   value already in a register.
2. `avail[pIdx]` in every park tier → `CANCELLED_AVAIL` → bail.
   No new volatile load.
3. `compareAndExchange(registry, CLEARED, self)` → witness is
   `CANCELLED` → bail.

### Seal — no more puts, drain buffered values

```
seal():
  prev = PRODUCER_SEQ.getAndSet(Long.MIN_VALUE)
  if prev < 0: return false          // already sealed or cancelled
  wakeAllAndPoison(consumerThreads)  // blocks new consumer parking
  wakeAllAndPoison(producerThreads)  // wake parked producers
  signalAll on both gates
  return true
```

1. New puts: `slot = producerSeq.getAndAdd(1)` → negative → return
   false.
2. Published values drain normally — `avail[pIdx]` matches, tier 1.
3. Once drained: consumer enters tier 2, `compareAndExchange(
   consumerThreads[pIdx], CLEARED, self)` returns `CANCELLED` →
   return CANCELLED sentinel. Same hardware instruction as CAS.
4. Already-parked consumers: after `park()`, `registry[pIdx] != self`
   → true (canceller replaced entry) → return CANCELLED.

Cancel uses `avail[]` — the one array every parking tier reads.
Seal uses `consumerThreads[]` — preserves published values while
blocking new consumer parks. Both insert signals into existing reads.

### Query

```
isCancelled(): CONSUMER_SEQ < 0
isSealed():    PRODUCER_SEQ < 0    // true for both seal and cancel
count():       if p < 0 || c < 0: return 0; else: existing logic
```

### Clojure API Semantics

**Cancellation-propagating (default):**

- `(take! ch)` — parks, returns value. On CANCELLED: cancels
  `impl/*this*` (the current task, if any), throws
  `CancellationException`.
- `(put! ch v)` — parks, returns true. On sealed/cancelled: cancels
  `impl/*this*`, throws `CancellationException`.
- `(put! ch v false)` — opt out: returns false instead of throwing.

**Non-propagating:**

- `(poll [v ch] then else)` — macro, if-let shape. Binds `v` in
  then-branch when value available, evaluates else-branch (no binding)
  on CANCELLED. No cancellation propagation, no throw.

**Lifecycle:**

- `(cancel! ch)` / `(cancel! ch msg)` — cancel, returns boolean
- `(seal! ch)` — seal, returns boolean
- `(cancelled? ch)` / `(sealed? ch)` — query

### Interrupt spin (known issue)

`LockSupport.park()` returns immediately when the interrupt flag is
set, causing a spin loop in `while(avail != expected) park()`. Both
cancel and seal resolve this: the avail sentinel (cancel) or registry
sentinel (seal) is detected on the next loop iteration after the
spurious wake, breaking the loop. For non-cancelled interruption, the
park loop should additionally check `Thread.interrupted()` and bail —
to be addressed alongside alt's burned slot mechanics.

## Open Questions

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

- **Pipeline composition**: `pipe` is implemented. Transfers values
  from source to sink via a `poll`/`put!` loop on a virtual thread.
  Propagates lifecycle: seal source → seal sink, cancel source →
  cancel sink. Configurable via `(pipe src sink false)` to leave
  sink open.

  ```clojure
  (pipe source sink)         ; propagates seal/cancel (default)
  (pipe source sink false)   ; transfer only, sink stays open
  ```

  Returns the pipe task (deref to wait, cancel to stop).

  The pipe task is wait-free when it is the sole consumer of source
  and sole producer of sink — every `take`/`put` hits tier 1. Under
  contention the pipe competes via XADD like any other participant.

  For parallel pipes, create N pipes with `qfor`:
  `(qfor [_ (range 4)] (pipe source sink))`. Ordering is lost but
  throughput scales. The channel's MPMC design handles it natively.

  See "Buffer Sizing and Throughput" for pipe benchmark results.

- **Rendezvous channel (buffer = 0)** (deferred): Separate
  implementation — no value buffer, purely direct handoff. Dual
  queue semantics: either producers or consumers are waiting, never
  both. Pre-allocated registry sized to expected concurrency
  (default: physical core count) for allocation-free parking. CLQ
  overflow if concurrency exceeds array size. Same escalation as
  fixed buffer: CLEARED → Thread → CLQ. Unlike fixed buffer, sizing
  is purely about concurrency (no throughput/buffering dimension).
