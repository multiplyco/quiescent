# Fixed Buffer Channel: Linked List Design

Lock-free dual queue design for fixed buffer channels. This was the
original approach, explored before the ring buffer design. Preserved
as an alternative — may still be relevant for rendezvous channels
(buffer = 0) or as a reference for the Payload/Taker concepts used
in alt.

See `channels.md` for channel semantics. See `channels-ringbuffer.md`
for the current preferred design.

## Dual Queue

A fixed buffer channel is backed by a single lock-free queue (inspired
by `co.multiply/conc`'s concurrent Deque). The queue is a **dual queue**:
it contains either Takers waiting for values, or Putters holding values
— but never both at the same time.

If a value is available when a taker arrives, the taker takes directly
(no enqueue). If a taker is available when a putter arrives, the putter
fulfills the taker directly. This mutual exclusion is a structural
invariant.

## Link Types

Two distinct types extending a common base, so the link type
unambiguously identifies the queue's current mode. Once you see one
`Taker`, you know every link is a `Taker`. Once you see one `Putter`,
every link is a `Putter`.

## Link Fields

Each link has three CAS-able fields:

```
Link {
  value:   Object   (CAS — the value being transferred)
  thread:  Thread   (CAS — thread to unpark on claim)
  claimed: boolean  (CAS — signals logical deletion)
}
```

The fields serve orthogonal purposes:
- **`claimed`**: taker CAS's `false → true` to claim a value. Signals
  logical deletion. Any scanner encountering a claimed link treats it as
  dead.
- **`thread`**: putter CAS's `nil → T` to attach a thread for later
  unparking. Taker reads thread after claiming to unpark if non-nil.
- **`value`**: the payload. For Taker links, starts as a sentinel
  (`EMPTY`) and is CAS'd `EMPTY → V` by the fulfilling putter. A
  sentinel is needed because `nil` is a valid Clojure value — without
  it, delivering `nil` would be indistinguishable from "unfulfilled."
  For Putter links, set at creation (always a real value, no sentinel).

The separation of `claimed` and `thread` eliminates contention between
takers (who claim) and putters (who attach threads). They operate on
different fields and can both succeed concurrently. After both CAS:
- Putter sees `claimed = true` → skip parking (value already consumed).
- Taker sees `thread != nil` → unpark it.
At least one side will see the other's write. All orderings resolve
correctly.

## Link Lifecycles

**Taker link:**
1. `{value: EMPTY, thread: T, claimed: false}` — parked taker waiting
2. Putter CAS's value `EMPTY → V` — fulfilled
3. Putter unparks T. Taker wakes, reads V.
4. Claimed, physically unlinked.

**Putter link (buffer available):**
1. `{value: V, thread: nil, claimed: false}` — buffered value
2. Later, a parked putter may CAS thread `nil → T` to attach
3. Taker CAS's claimed `false → true` — claims value
4. Taker reads thread; if non-nil, unparks it (frees attached putter)
5. Physically unlinked (by a putter during traversal).

**Putter link (buffer full, parking):**
Putter enqueues `{value: V, thread: nil, claimed: false}` — the value
goes to the tail. The putter's thread is attached to an **earlier** link
(see Thread Attachment below), not its own link. The putter then parks.

## Queue Layout and Pointers

The queue maintains two pointer hints:

```
true head → [dead] → [dead] → taker head → [live] → [live] → tail
             graveyard            ↑             buffer
             (claimed links)     takers start here
```

- **True head**: start of the physical list, including the graveyard
  (claimed but not yet unlinked links).
- **Taker head hint**: points to the first live (unclaimed) link. Takers
  start here — O(1) to the next value. Never see the graveyard.
- **Tail**: where new links are enqueued.

Additionally, a **thread-available hint** tracks the first link with
`thread = nil`, used by putters to find attachment points quickly.

All hints are best-effort. They may be stale, but minimize traversal in
the common case. Scanners advance past stale hints naturally.

## Buffer Capacity: Dead Links as Permits

Buffer capacity is encoded in the queue structure itself — no atomic
counter needed. On creation, the channel is initialized with N dead
(claimed) Putter links, where N is the buffer capacity:

```
Buffer size 3. Initial state:
  true head → [dead] → [dead] → [dead] → taker head
               3 permits
```

Each dead link is a **permit**. The put path is uniform — every putter
scans for a dead link:

- Found dead link with `thread = nil` → CAS thread, attach, skip
  parking. **Permit consumed.**
- No dead links available → attach to a live link, park.

Takes replenish permits: claiming a live link (CAS `claimed → true`)
turns it dead. The next putter discovers it and skips parking.

```
put:
  enqueue Putter{value: V, thread: nil, claimed: false} at tail
  scan from true head for a link with thread = nil:
    if dead link found → CAS thread, skip parking (permit consumed)
    if live link found → CAS thread, park (no permits left)
```

The buffer capacity is fully self-regulating. Puts consume dead links,
takes produce them. The total link count (dead + live) stays roughly
constant at buffer capacity, with some churn variability — similar to
the memory profile of a pre-allocated array.

**Rendezvous** (buffer = 0) falls out naturally: no initial dead links,
so every putter immediately has no permits and must park.

## Thread Attachment (Putter Unparking)

The putter's value and thread go to **different places**:
- Value: enqueued at the tail (preserves FIFO ordering).
- Thread: CAS'd onto the `thread` field of an earlier link with
  `thread = nil` (the first available permit or live link).

Example with buffer size 3:

```
Initial:       [dead] → [dead] → [dead]

Putter 1:      enqueue V1 at tail. Scan: find dead link, attach T1.
               Dead link has thread now → eligible for unlink.
               T1 sees claimed = true → skip parking.
               [dead T1] → [dead] → [dead] → [V1]

Putter 2:      enqueue V2. Attach T2 to next dead link. Skip parking.
               [dead T1] → [dead T2] → [dead] → [V1] → [V2]

Putter 3:      same. All 3 permits consumed.
               [dead T1] → [dead T2] → [dead T3] → [V1] → [V2] → [V3]

Putter 4:      enqueue V4. Scan: no dead links with free thread.
               Attach T4 to V1 (first live link with thread = nil). Park.
               ... → [V1 T4] → [V2] → [V3] → [V4]

Take V1:       CAS claimed → true. Read thread = T4, unpark T4.
               V1 is now dead — a new permit.
               ... → [V1 dead T4] → [V2] → [V3] → [V4]

Putter 5:      enqueue V5. Scan: find V1 dead, thread occupied. Skip.
               Find V2, thread = nil. Attach. Check: not dead → park.
               ... → [V2 T5] → [V3] → [V4] → [V5]
```

The chain is self-sustaining: each take creates a permit (dead link),
each put consumes a permit. The queue is a conveyor belt — live links
enter at the tail, dead links accumulate at the head, putters clean
them up.

The **thread-available hint** tracks the first link with `thread = nil`,
so putters find attachment points in O(1) amortized rather than scanning
from the head.

## Contention: Taker Claims Slot Before Putter Attaches

If a taker CAS's `claimed` on a link before a putter CAS's its `thread`
onto the same link:

1. Putter CAS's `thread nil → T` on the link (succeeds — different field)
2. Putter reads `claimed` → true (link is dead)
3. Putter skips parking — a take happened, buffer space was freed.

The putter attached to a dead link and immediately discovered it doesn't
need to park. No wasted work, no stuck thread. The orthogonal CAS fields
make this race harmless.

## Physical Unlinking

**Only putters perform physical unlinking.** Takers set `claimed = true`
(logical deletion) but do not unlink. This preserves dead links as
evidence of takes — putters discover them during traversal.

A link can only be physically unlinked if its `thread` field has been
occupied (non-nil). This ensures every dead link is "accounted for" —
either a putter attached and was unparked (taker saw thread), or a putter
attached and saw it was dead (skip parking). No take event is lost.

**Putter traversal and cleanup:**

A putter scans rightward from true head looking for a free thread slot.
Along the way, it encounters the graveyard — dead links left of the taker
head. Its primary goal is finding a `thread = nil` slot to attach to.
Unlinking is opportunistic:

```
Putter scans from true head:
  L1 dead, thread occupied → skip (already claimed by another putter)
  L2 dead, thread occupied → skip
  L3 dead, thread nil → CAS thread → attach. Found my slot.
  Try to unlink L1→L2→L3 in one CAS (snip the prefix traversed).
  Done.
```

One CAS attempt to snip the path it just walked. If it fails, move on.
The putter doesn't go back for a full sweep.

Under contention: individual putters may fail to unlink, but other putters
succeed — potentially snipping multiple links in one CAS. Failed attempts
leave longer chains of dead links, and the next successful CAS leapfrogs
all of them. Aggregate cleanup is the same or better under contention.

The graveyard stays bounded because putters clean it up incrementally on
every pass. The more dead links, the more evidence putters find, the more
likely they skip parking. Heavy consumption → large graveyard → strong
signal for putters. Light consumption → small graveyard → putters park
normally. The system self-balances.

## Cancellation

A cancelled taker or putter marks its own link for logical deletion (CAS
`claimed → true`). Any competing putter/taker that tries to CAS on that
link's fields will see it's claimed and move on.

A cancelled putter's dead link becomes a permit — equivalent to a take
having happened. This is correct: the value was never consumed, so buffer
space is effectively freed.

## `alt` Design

`alt` works for both taking and putting. When taking, it selects the
channel from which to consume. When putting, it runs the same logic in
reverse: selecting the channel on which to insert.

### Shared Payload

Cross-channel coordination uses a **shared Payload** object. Rather than
intrusive queue entries, each queue link holds a reference to a Payload.
For a normal `take`/`put`, one link references one Payload. For `alt`,
multiple links (one per channel) reference the **same** Payload.

```
Queue Link  →  Payload (shared across channels for alt)
  - next         - value
  - mark         - thread
                 - claimed
```

A CAS on the shared Payload is the single serialization point. Only one
channel can claim the alt — all other channels' Takers pointing to the
same Payload become logically dead.

### Algorithm

The alt algorithm interleaves speculative takes with Taker injection.
After failing to take from a channel, a Taker is immediately injected so
it can be fulfilled concurrently while the alt moves on to try the next
channel:

```
alt(ch1, ch2, ch3):
  loop:
    create fresh Payload{value: nil, thread: self}

    for each channel ch[i]:
      value available on ch[i]?
        → CAS Payload from PENDING → CLAIMED
        → if CAS succeeds: try take value from ch[i]
          → if take succeeds: return value
          → if take fails: start over (loop with fresh Payload)
        → if CAS fails: another channel already won (shouldn't happen
          during speculative phase unless a Taker was fulfilled
          concurrently — start over)
      no value available?
        → inject Taker(Payload) into ch[i]
        (a putter may fulfill this Taker while we continue to ch[i+1])

    all Takers placed:
      → check: Payload already claimed? → return value
      → not claimed → park
      → wake → return Payload's value
```

**Key invariant**: the Payload CAS must happen **before** consuming a
value from any channel. This prevents the race where a speculative take
succeeds on ch2 while a putter simultaneously fulfills the Taker on ch1,
resulting in two values being consumed for one alt.

**Recovery**: if the Payload is claimed but the subsequent take from the
channel fails (lost to a concurrent non-alt taker), the current Payload
and all its Takers are abandoned. A fresh Payload is created and the
process restarts. Dead Takers are cleaned up by Harris-style deletion on
the next traversal. This follows the "always progress / move forward"
principle — no lock, no repair, just retry.

### Fairness Modes

`alt` supports two ordering modes:

- **`:prio`** — always tries channels in declaration order (start at
  index 0).
- **`:fair`** — round-robin. Rotates the starting channel on each take.

```clojure
(alt :prio (alt :fair ch1 ch2) ch3)
;; Inner alt: round-robin between ch1 and ch2
;; Outer alt: prioritize inner over ch3
```

Fair scheduling uses an `AtomicInteger` on the alt channel object:

```
idx = Math.floorMod(counter.getAndIncrement(), numChannels)
try channels starting from ch[idx]
```

`Math.floorMod` is used instead of `%` because Java's `%` returns
negative values for negative operands. `Math.floorMod` always returns a
non-negative result. This means `AtomicInteger` wrapping from
`Integer.MAX_VALUE` to `Integer.MIN_VALUE` is harmless — the mod still
produces a valid index. No need for `AtomicLong`.

For `:prio`, the counter is not used — always start at index 0. The two
modes are the same loop with only the starting index varying.

## Trade-offs vs Ring Buffer

**Advantages:**
- Rendezvous (buffer = 0) falls out naturally
- Alt/Payload integration is native to the structure
- No stuck slot problem — nodes are fully formed before linking
- Cancellation is clean (mark and move on)

**Disadvantages:**
- Allocation per put (new Link object)
- Pointer chasing (poor cache locality)
- GC pressure from short-lived links
- Graveyard maintenance complexity
- No SPSC fast path (always CAS)
