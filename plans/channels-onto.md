# Atomic `onto(Iterable)` and `seal(Iterable)` Design

Batch put and batch-put-then-seal operations with single-lock-acquisition
atomicity. The entire batch appears as a contiguous sequence in the
buffer with no interleaving from other producers.

See `channels-adaptive.md` for the adaptive channel design this builds on.

## Motivation

Common patterns like "load initial data then seal" or "transfer a
collection onto a channel" currently require per-element `put!` calls.
Each call is individually atomic but the batch can be interleaved with
other producers. `onto` makes the batch atomic. `seal(coll)` composes
batch put with seal under a single lock acquisition — guaranteeing no
values slip in between the last batch element and the seal.

Analogous to core.async's `onto-chan!`, but atomic rather than
sequential.

## Interface: IChannel

Two new default methods:

```java
default boolean onto(Iterable<?> coll) {
    for (Object val : coll) {
        if (!put(val)) return false;
    }
    return true;
}

default boolean seal(Iterable<?> coll) {
    try { return onto(coll); }
    finally { seal(); }
}
```

Non-atomic defaults. Each `put()` is independent, interleaving is
possible. Bounded channels override with atomic versions.

## Channel Hierarchy

### AbstractBoundedChannel

**`ontoDirect(Iterable<?>)`** — caller holds `putLock`:

```java
boolean ontoDirect(Iterable<?> coll) {
    for (Object val : coll) {
        int c = putDirect(val);
        if (c < 0) return false;
        if (c == 0) signalNotEmpty();
    }
    return true;
}
```

**Critical: `signalNotEmpty()` inside the loop.** When `c == 0`
(buffer was empty), consumers may be parked on `notEmpty`. If the
batch exceeds buffer capacity, `putDirect` parks on
`notFull.await()` (which releases `putLock`). Without waking
consumers first, nobody drains the buffer — deadlock.

**`sealInternal()`** — caller holds `putLock`:

```java
boolean sealInternal() {
    if (this.state != OPEN) return false;
    this.state = SEALED;
    notFull.signalAll();
    // Lock ordering: putLock → takeLock (caller holds putLock)
    takeLock.lock();
    try { notEmpty.signalAll(); }
    finally { takeLock.unlock(); }
    return true;
}
```

This also enables fixing the `Reduced` handling in
`BoundedChannelXf` — replace
`throw new UnsupportedOperationException("Seal not yet implemented")`
with `rf.invoke(this); sealInternal(); return false`.

**Override `onto` and `seal(Iterable)`:**

```java
@Override
public boolean onto(Iterable<?> coll) {
    putLock.lock();
    try { return ontoDirect(coll); }
    finally { putLock.unlock(); }
}

@Override
public boolean seal(Iterable<?> coll) {
    putLock.lock();
    try {
        if (!ontoDirect(coll)) return false;
        return sealInternal();
    } finally { putLock.unlock(); }
}
```

While we hold `putLock`, no other thread can seal or cancel (both
acquire `putLock`). So `ontoDirect` can only fail if interrupted.

### BoundedChannel

Mirrors `put()`'s three-path adaptive protocol. The Dekker
handshake brackets the entire batch (one entry/exit for N elements).

**`onto(Iterable<?>)`:**

```java
@Override
public boolean onto(Iterable<?> coll) {
    Thread owner = this.producerOwner;
    if (owner == CONTENDED) return ontoLocked(coll);

    Thread self = Thread.currentThread();
    if (owner == self) {
        this.putFastActive = 1;
        if (this.producerOwner == self) return ontoFast(coll);
        this.putFastActive = 0;
        return ontoLocked(coll);
    }
    return ontoSlow(coll, self);
}
```

**`ontoFast`** — lock-free, `putFastActive` stays 1 for the batch.
Per-element cost: one acquire read (count), one array write, one
release add (count). No per-element Dekker overhead.

```java
private boolean ontoFast(Iterable<?> coll) {
    Iterator<?> iter = coll.iterator();
    while (iter.hasNext()) {
        Object val = iter.next();
        if ((int) COUNT.getAcquire(this) >= capacity) {
            this.putFastActive = 0;
            return ontoFastPark(val, iter);
        }
        if (this.state != OPEN) {
            this.putFastActive = 0;
            return false;
        }
        buffer[(int)(tail++ & mask)] = val;
        int c = (int) COUNT.getAndAddRelease(this, 1);
        if (c == 0) signalNotEmpty();
    }
    this.putFastActive = 0;
    return true;
}
```

**`ontoFastPark(val, iter)`** — buffer full mid-batch. Flag is
already cleared (before acquiring the lock — same pattern as
`putFastPark`, prevents deadlock with upgrader spinning on the flag
while holding the lock). Acquires `putLock`, puts the current value
via `putDirect`, then loops `putDirect` for the remaining iterator
elements.

**`ontoSlow(coll, self)`** — first call or contention detected.
Acquires `putLock`, participates in the ownership protocol (claim
if unclaimed, upgrade + spin if different owner), then `ontoDirect`
for the batch. Mirrors `putSlow`.

**`ontoLocked(coll)`** — post-upgrade. Acquires `putLock`,
`ontoDirect`.

**`seal(Iterable<?>)`** — same three-path structure. The fast path
writes batch elements lock-free, then clears `putFastActive`,
acquires `putLock`, and calls `sealInternal()`. The locked/slow
paths do `ontoDirect` + `sealInternal` under `putLock`.

Alternative for `seal(Iterable)`: always use the locked path since
seal is a one-time terminal operation and lock overhead is
negligible.

### BoundedChannelXf

Override `onto` and `seal(Iterable)` to route through the
transducer. No adaptive fast path — producer side is always locked
(same as existing `put()`).

```java
@Override
public boolean onto(Iterable<?> coll) {
    putLock.lock();
    try {
        for (Object val : coll) {
            if (state >= SEALED) return false;
            Object result = rf.invoke(this, val);
            if (RT.isReduced(result)) {
                rf.invoke(this);        // flush
                sealInternal();
                return true;
            }
        }
        return true;
    } finally { putLock.unlock(); }
}

@Override
public boolean seal(Iterable<?> coll) {
    putLock.lock();
    try {
        for (Object val : coll) {
            if (state >= SEALED) return false;
            Object result = rf.invoke(this, val);
            if (RT.isReduced(result)) {
                rf.invoke(this);
                sealInternal();
                return true;
            }
        }
        rf.invoke(this);                // flush stateful xforms
        return sealInternal();
    } finally { putLock.unlock(); }
}
```

The `baseRf` step function already calls `putDirect` +
`signalNotEmpty`, so consumer signaling is handled within the
transducer pipeline.

## Clojure API

```clojure
(defn onto!
  "Put all values from coll onto the channel atomically (as a
   contiguous batch). Parks if the buffer is full mid-batch."
  [ch coll]
  (IChannel/.onto ch coll))

(defn seal!
  "Seal the channel. No more puts accepted; existing values drain
   normally. With coll, atomically puts all values then seals.
   Returns true if this call performed the seal."
  ([ch]
   (IChannel/.seal ch))
  ([ch coll]
   (IChannel/.seal ch coll)))
```

## Return Semantics

- `onto(coll)` → `true` if all values were put, `false` if
  channel sealed/cancelled/interrupted mid-batch.
- `seal(coll)` → `true` if batch was put and channel was sealed,
  `false` if already sealed/cancelled/interrupted.

For the atomic variants (locked/adaptive), while `putLock` is held
no other thread can seal or cancel. So failure is only possible if
the thread is interrupted during `notFull.await()` parking.

## Files to Modify

1. `java/.../channel/IChannel.java` — default `onto`, `seal(Iterable)`
2. `java/.../channel/AbstractBoundedChannel.java` — `ontoDirect`, `sealInternal`, overrides
3. `java/.../channel/BoundedChannel.java` — adaptive three-path `onto`/`seal`
4. `java/.../channel/BoundedChannelXf.java` — transducer `onto`/`seal`, fix Reduced
5. `src/.../channel.clj` — `onto!`, 2-arity `seal!`

## Open Questions

- **`seal()` zero-arg and transducer flush**: should the zero-arg
  `seal()` also flush the transducer (call the completion arity)?
  core.async's `close!` does flush. Currently our `seal()` does
  not. Adding flush to `seal()` would make stateful transducers
  like `(partition-all n)` emit their final partial batch on seal.

- **`seal(Iterable)` fast path**: is the adaptive fast path worth
  the complexity for a one-time terminal operation? The locked path
  is simpler and the lock overhead is negligible for a single call.
