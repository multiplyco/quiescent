# Channel memory ordering review

The current JVM channel needs an ordering argument for ownership transfer,
buffer publication and reuse, operation completion, sealing, and notification.
This is the issue list for working through that argument together. The agreed
queued-sealing and put-handoff fixes are recorded below. Transducer support has
been removed; the remaining plain-channel issues are open.

Reviewed source: `channel/main` at
`03a7ebfc3d1de16a7c7e9f0e813c3c76f3a91d33`, on 2026-11-02. Scope is the current
`BoundedChannel`, `RelayLock`, their interfaces, and the Clojure channel API.
Alt, channel cancellation, and the earlier experimental branches are excluded.

On 2026-11-06, transducer support was removed from the active implementation,
Clojure constructor, tests, and executable benchmark cases. Commit `2115c15`
retains the preceding transducer implementation. The current channel runs no
user reducing function under ownership and has no completion or `Reduced` path.
Issue 4 therefore no longer applies to this implementation; failure semantics
and implementation boundaries for a future transducing variant remain undecided.
The proposed failure contract is not being added to the plain channel.

The plain-channel revision passes 467 JVM tests / 4,298 assertions, and 8 benchmark
checks / 128 assertions covering all 16 remaining cases against both libraries.
The sealing, handoff, publication, lifecycle, and slot tests also pass with one
virtual-thread carrier (11 tests / 3,135 assertions). The sealing and handoff
regressions use ordinary payload nodes and full-buffer backpressure instead of
transducer callbacks. Against the Java classes from `2115c15`, the rewritten
handoff test still fails exactly its two interruption-ownership assertions.

**Confirmed** means the source admits the described execution or directly
violates the stated ownership rule. These are source-level findings, not claims
that new runtime reproducers were run for this revision. **Proof gap** means an
argument is missing, not that a failing execution has been established.
**Contract question** identifies behavior whose acceptability needs a decision.

## Sequence terminology

The review uses the current sequence names, including in examples of the earlier
findings. Historical source line references still refer to the reviewed revision.

| Field | Previous name | Meaning |
|---|---|---|
| `publishedPutSequence` | `tail` / `TAIL` | Production published to consumers |
| `publishedTakeSequence` | `head` / `HEAD` | Consumption published to producers, releasing capacity |
| `putSequence` | `putTail` | Producer owner's working position, including unpublished writes |

The VarHandles are `PUBLISHED_PUT_SEQUENCE` and `PUBLISHED_TAKE_SEQUENCE`.
These are wrapping positions. For a coherent observation,
`publishedPutSequence - publishedTakeSequence` is the published buffer occupancy.

## Issue index

The numbers are stable discussion references. The finding descriptions refer to
the reviewed revision above. Issues 1, 2, and 6 are addressed by the queued-sealing
change; issue 3 is addressed by capturing the owner's results before handoff.
Issue 4 is excluded by removing transducer support.
This does not establish correctness of the entire channel.

| Issue | Subject | Classification | Status |
|---|---|---|---|
| 1 | Empty observation followed by sealing can report premature exhaustion | Confirmed | Addressed by terminal sequence |
| 2 | A combined put can complete before its values are published | Confirmed sealing conflict | Sealing waits for publication |
| 3 | Put scratch fields are read after ownership is released | Confirmed ownership violation | Captured before handoff |
| 4 | A transducer exception can strand the put chain | Confirmed in removed implementation | Deferred with transducer support |
| 5 | Conditional notification lacks a complete ordering argument | Proof gap | Address during the protocol proof |
| 6 | Concurrent seal calls can both report success | Confirmed API mismatch | Serialized by put ownership |
| 7 | Buffer inspection can report a negative count and saturation | Confirmed observation; contract question | Next; settle before the proof |
| 8 | Interruption is deferred until another event permits progress | Contract question | Redesign deferred until after the proof |
| 9 | FIFO service does not establish eventual return of the combiner | Progress proof question | Settle contract before the proof |

Issues 1 and 2 concern related exhaustion guarantees, but they are distinct.
Rechecking a published cursor would address the observation in issue 1 without
necessarily addressing the unpublished accepted values in issue 2.

## Agreed work order

The plain-channel baseline is commit `8179173`. Continue discussing and handling
one issue at a time, retaining the issue numbers above. The next discussion is
issue 7; issue 8's redesign remains parked, then issue 9's contract comes next.

Before constructing the proof:

1. **Issue 7:** settle the inspection contract and fix the range mismatch. The
   proposed contract keeps `buf-count` approximate but within zero to capacity,
   and `saturation` within zero to one. The implementation choice remains for
   that discussion; an exact snapshot is not required by this proposal.
2. **Issue 9:** decide whether fairness includes eventual return of every caller
   under continued arrivals, assuming scheduling and sufficient counterpart
   progress. If required, the current unbounded combining needs a limit on the
   work an owner accepts before handing over. Choose that mechanism separately;
   the progress proof must account for any resulting change.

Then construct the proof outline for the whole plain-channel protocol: contract,
state, invariants, assumptions, and obligations for ownership, transfer,
combining, sealing, and notification. Issue 5 belongs in this phase. Establish
the algorithmic argument and then justify the Java access modes it depends on.
Safety and progress are separate obligations; tests support those arguments.

After the baseline proof, revisit **issue 8's prompt-interruption proposal**,
including transfer to a stand-in virtual thread. The initial proof must explicitly
describe the existing deferred-interruption behavior. Transducer support and its
failure semantics also remain deferred to a later design effort. These later
changes will need their own proof obligations when introduced.

## Agreed queued sealing design

External sealing takes its place in the producer queue using a `SealNode`.
`putCombine()` stops before that node. Its owner flushes accumulated output and
releases the last processed node, handing ownership to the sealing caller.
Puts preceding that boundary finish; later puts acquire in the normal order
and are rejected. A seal can wait indefinitely behind an earlier put if no
consumer provides the required capacity.

Under put ownership, `seal()` writes `sealedSequence = publishedPutSequence`,
then publishes `state = SEALED`, and notifies consumers. Only the first sealing
operation writes the sequence or returns true. The terminal sequence remains
immutable. It is outside the buffer, needs no slot, and reserves no numeric
cursor value.

The former transducer path could seal directly under producer ownership after
`Reduced` and completion. That path and its `sealOwned()` helper have been removed;
sealing now always goes through the producer queue.

An apparently empty take that observes `SEALED` reports exhaustion only when its
`publishedTakeSequence` equals `sealedSequence`. Otherwise it retries the buffer
read: the earlier empty observation preceded the final publication. The end
marker is persistent, so exhaustion does not advance `publishedTakeSequence`.

### Ordering argument for the terminal sequence

This argument depends on the existing RelayLock ownership handoff and ordinary
buffer publication. It does not discharge the open notification proof in issue 5.

1. All producers write and flush while owning the put side. An external seal
   cannot acquire ownership until every operation ahead of its queue node has
   been processed and its owner has flushed. Combining cannot cross the node.
2. The releasing predecessor's node exchange and the sealing caller's acquire
   establish visibility of that work. The sealer records the final
   `publishedPutSequence` under ownership.
3. The plain write of `sealedSequence` precedes the volatile write of `SEALED`.
   A consumer reads the sequence only after observing that volatile publication.
   Its observation therefore includes the sequence and all preceding output.
4. Every subsequent producer holds the same put lock and observes the sealed
   state before accepting output. No later operation advances
   `publishedPutSequence` or changes the terminal sequence.
5. A consumer holds take ownership while comparing its `publishedTakeSequence`
   to the terminal sequence. Equality establishes exhaustion. Inequality makes
   it reread `publishedPutSequence` after acquiring the seal publication; it
   cannot then reuse the earlier empty observation to return exhaustion.

The state publication also precedes `takeLock.resume()` and its volatile exchange.
The existing registered-waiter/banked-signal protocol handles takes that need to
wait. Queued puts are released through normal ownership handoff; sealing no longer
resumes a producer suspended ahead of it. There is no additional `fullFence()`
on this path: sequence publication uses the state release/acquire edge.

Normal successful takes retain their existing cursor reads and publication
operations. The terminal sequence is read on the sealed/empty path only. Combining
adds a node-type check at each successor; sealing adds one ordinary queue node.
No per-value allocation or cursor packing is introduced. Performance still needs
measurement rather than inference from these operation counts.

### Regression coverage

`channel/sealing_test.clj` checks an ordered queue with puts on both sides of the
seal, a combiner paused after acknowledging unflushed output, concurrent seals,
sealing a full buffer, nil/false payloads, and numeric cursor wrap. Repeated
concurrent empty/publish/seal trials exercise issue 1 without claiming to force its exact interleaving. Existing
lifecycle tests now expect puts ahead of a seal to finish, while queued empty
takes still wake on sealing.

Benchmark failure cleanup requests sealing asynchronously so a failed trial's
deadline is not blocked by this newly queued operation. Its regression test
holds producer ownership while verifying that the original timeout escapes.

At the queued-sealing revision, validation passed: 482 JVM tests / 4,402 assertions,
7 benchmark checks / 179 assertions, and 11 focused tests / 3,198 assertions with one virtual-thread
carrier. The separate before/after performance capture measures this exact
state-plus-sequence representation; its results do not apply to the alternatives
below.

### Open representation choice

The state and terminal sequence are logically redundant. A single nullable
boxed sequence could represent both openness and the final position, with
allocation at sealing rather than per value. Alternatively, a primitive
terminal sequence could reserve a value such as -1 for open, provided that value
is excluded from valid cursor sequences and wraparound is defined consistently.
The current full-width counters do not reserve such a value. A floating-point
NaN would trade away exact representation of the full integer sequence range.
No representation change has been selected or measured yet.

## Issue 1 Empty observation followed by sealing

**Confirmed.** In `BoundedChannel.take()` (lines 254–279), the consumer first
reads `publishedPutSequence`, decides that the buffer is empty, and then reads
`state`. It can return `IChannel.SEALED` without checking the buffer again.

An execution starting with an open, empty channel:

1. Consumer C reads `publishedTakeSequence = 0` and `publishedPutSequence = 0`.
2. Producer P writes a value, publishes `publishedPutSequence = 1`, and completes
   its put.
3. P seals the channel.
4. C reads `state = SEALED` and returns exhaustion using its earlier empty
   observation.
5. A subsequent take can return P's value.

This execution needs no stale read or weak memory effect. It is possible under
sequential consistency. A stronger mode on the existing reads cannot make two
separate observations into one decision about the same state.

**Obligation:** exhaustion must establish that production has ended and that
there are no remaining accepted values to deliver. Once a take has reported
exhaustion, a newly invoked take must not yield a value. A previously started
take may still be returning a value it already consumed.

**Discussion:** how should the observation of sealing relate to the final
buffer check? That decision also needs to account for issue 2.

## Issue 2 Combined put completion precedes publication

**Confirmed sealing conflict.** `putCombine()` writes `succ.combined = true`
and wakes the successor (lines 230–234). The successor can return `true` from
`put()` at line 183. The combiner may not publish the corresponding
`publishedPutSequence` until its final flush at lines 196–200 or a backpressure
flush.

A concrete execution with sufficient buffer space and a transducer that emits
one output per input:

1. P owns the put lock. Q and R are queued behind it.
2. P accumulates its own output and Q's output without flushing
   `publishedPutSequence`.
3. P marks Q combined; Q's put returns `true`.
4. P starts processing R and pauses inside the transducer before emitting R's
   output.
5. Another thread seals the channel. A take sees the still-empty committed
   buffer and returns exhaustion.
6. R's transducer finishes. P eventually flushes the previously accumulated
   outputs, including Q's, which become available to later takes.

This is different from issue 1: Q's put has already returned before sealing,
but its output is absent from the published cursor. Merely rereading
`publishedPutSequence` after observing `SEALED` cannot discover that output while
P is paused.

External `seal()` also does not acquire the put lock. Even an ordinary put can
pass the `OPEN` check in `putAccumulate()`, pause, and write after another thread
seals. Overlapping operations need an explicit rule; the completed combined
put above gives the stronger case that the rule must handle.

**Obligation:** define the relationship between acceptance, publication,
sealing, and exhaustion, including values accumulated by another caller.

**Discussion:** what must be committed before a put can report success, and
what must sealing establish about production already in progress? Expanding
transducers also need a rule for partial output when sealing intervenes.

## Issue 3 Put scratch fields outlive lock ownership

**Confirmed ownership violation.** `putInterrupted` and `putSealed` are plain
fields documented as belonging to the put lock. `put()` releases that lock at
line 205, then reads `putSealed` at line 209 and `putInterrupted` at line 213.
The next owner resets both fields at lines 185–186.

For example, P records `putInterrupted = true`, releases ownership, and pauses.
Q acquires ownership and resets the flag. P resumes and reads `false`, losing
the recorded interruption. P can also observe a flag belonging to Q's operation.

The problem exists even with sequentially consistent field accesses. Making
these fields volatile would not make the value belong to the right operation.

**Obligation:** values used to finish one operation must remain that operation's
values after ownership is handed off.

**Resolution:** `put()` captures `interrupted = putInterrupted` before calling
`last.release()`. The flush decision is also local. Consumer notification and
interrupt restoration happen afterward using only locals. Before transducer
removal, the notification decision also captured `!putSealed`; that field and
condition are now gone.

All reads of the shared put scratch fields now occur under put ownership. The
successor may reset them once it acquires ownership, but cannot change the
outgoing caller's local results. The fields can remain plain; this change adds
no allocation, volatile access, or fence, and preserves notification order.

`channel/handoff_test.clj` forces the successor to run before the former owner
returns. A test-only queued node overrides `release()` to pause the combiner
after the real handoff; production code needs no test hook. Actual thread
interruptions during full-buffer waits exercise both loss of the old owner's
interrupt and incorrect inheritance of its successor's interrupt. The regression
fails both cases against the previous implementation and passes with the fix.

Before transducer removal, validation passed: 483 JVM tests / 4,416 assertions
and 7 benchmark workload checks / 179 assertions. The handoff regression also passes with one
virtual-thread carrier (14 assertions); the previous implementation fails its
two interruption-ownership assertions under the same controlled handoff.

## Issue 4 Transducer exceptions strand the put chain

**Confirmed.** Calls to `rf.invoke()` in `putXf()` (lines 166 and 168) can throw
while the put lock is owned. Neither that method nor `put()`/`putCombine()`
provides exceptional cleanup that releases the chain.

If P's transducer throws, P can leave its node unreleased. Q, already queued or
arriving later, cannot acquire the lock. In the reviewed revision, sealing's
cross-lock signal did not repair that handoff. With queued sealing, the sealing
caller also waits behind the stranded owner.

**Current scope:** the failing calls and all transducer support have been removed.
This finding is retained as an obligation for a future transducing implementation,
not an outstanding exceptional path in the plain channel. No fault state or
exception-routing protocol has been added.

Combining makes cleanup more involved: an exception may occur after some
successors have already been completed, and some output may already have been
published. Releasing the original node unconditionally is not automatically a
correct cleanup rule.

**Obligation:** exceptional paths must preserve ownership and release or settle
the queued operations consistently with the chosen failure semantics.

**Discussion:** what happens to the channel, partial output, and the affected
callers when a transducer throws? Which node owns the remaining handoff at that
point?

## Issue 5 Conditional notification needs an ordering proof

**Proof gap, not an established missed wakeup.** The producer's full-buffer
path publishes `publishedPutSequence`, calls `takeLock.resume()`, rereads
`publishedTakeSequence`, and may suspend (lines 138–144). The consumer publishes
`publishedTakeSequence`, releases its node, rereads `publishedPutSequence`, and
only resumes producers if the buffer was observably full (lines 260–270).

The required property is that a producer cannot remain parked for capacity
while the consumer has made capacity available but omitted the necessary
notification. The argument must include repeated suspensions, ownership
changes, batching, and the banked `SIGNALED` state.

The class comment appeals to sequential consistency, but the cursor rereads
use `getAcquire`. A volatile store followed by an acquire read alone does not
establish the usual two-variable Dekker guarantee. The intervening default
`getAndSet` operations are relevant; their exact contribution must be proved,
not assumed equivalent to placing a full fence anywhere in the path.

Publication and notification are separate obligations. Release/acquire can
publish a buffer value correctly even if a notification protocol is incomplete.
Likewise, a correct `RelayLock.resume()` cannot help when its caller decides
not to call it.

**Discussion:** which ordering edges rule out both sides missing the other's
progress? Every path that depends on those edges must retain them when an
access mode is weakened.

The [VarHandle contract](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/lang/invoke/VarHandle.html)
defines the access modes. Doug Lea's
[mixed-mode discussion](https://gee.cs.oswego.edu/dl/html/j9mm.html) specifically
distinguishes volatile-store/acquire-read Dekker constructions from the fully
volatile version. [LockSupport](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/concurrent/locks/LockSupport.html)
specifies the permit and parking guarantees that the signal proof must use.

## Issue 6 Concurrent seal calls can both return true

**Confirmed API mismatch.** `seal()` separately checks `state` and writes
`SEALED` (lines 315–316). Two callers can both read `OPEN`, both write `SEALED`,
and both return `true`.

The Clojure API says the return value indicates that this call performed the
seal. If that means winning the single open-to-sealed transition, the current
check and write do not implement it. The channel does remain sealed; the issue
is the reported transition ownership.

**Discussion:** retain the single-winner meaning of the return value, or define
a different meaning explicitly. Stronger ordering alone cannot make the
check-and-write atomic.

## Issue 7 Inspection combines different buffer states

**Confirmed observation; contract question.** `count()` reads
`publishedPutSequence` and then `publishedTakeSequence` independently (line 339).
During the interval between those reads, producers and consumers can advance
both cursors.

For a capacity-one channel, an observer can read `publishedPutSequence = 1`,
pause while the channel transfers two more values, then read
`publishedTakeSequence = 3`. It reports `-2`, even though the buffer always
contained zero or one value. `saturation()` then reports `-2.0`.

`count()` is documented as approximate, but `saturation()` promises a value
from zero to one. Volatile access to each cursor does not give a joint snapshot.

**Discussion:** what bounds and consistency do these diagnostic methods promise?
This is separate from correctness of put and take.

## Issue 8 Interruption does not independently end a wait

**Contract question.** `RelayLock.awaitPredecessor()` and `suspend()` record and
clear interruptions while continuing to wait for their normal conditions
(lines 113–119 and 135–147). An interruption alone therefore does not remove a
queued operation or release an owner suspended on an empty/full buffer.

An already-interrupted caller is rejected at entry to `put()`/`take()`, but a
caller interrupted after entering may remain blocked until another channel
event permits progress. This is distinct from issue 3's loss of a recorded
interrupt, and from the deliberately removed channel cancellation semantics.

**Discussion:** how should an individual blocked operation respond to thread
interruption? The current deferred behavior must either be part of the contract
or be changed while preserving the queue handoff.

**Deferred proposal:** on interruption, transfer responsibility for the same
queue entry to a stand-in virtual thread so the original caller can promptly
throw and retire. The primary goal is prompt caller shutdown, independently of
whether the operation still completes. Preventing a take from consuming a value
is a separate question, with its own race against commitment.

Keeping the operation node and its queue position could allow combining to
continue without knowing that its waiting thread changed. The transfer protocol
would still need to preserve ownership, completion, and wakeups. With transducers
removed, executing user callbacks is no longer an obstacle in the current channel;
transfer of the active channel operation still needs its own protocol.

This proposal is parked for later consideration. It is independent of issue 3:
prompt caller retirement does not establish the lifetime of shared scratch
fields, and fixing those fields does not make blocked operations interruptible.

## Issue 9 FIFO service and completion of the combiner

**Progress proof question.** `putCombine()` has no fixed batch limit. An owner
can continue processing newly arriving successors as long as the channel stays
open and the successor chain remains populated. With consumers making room,
successors can keep completing while the original owner remains in the loop.

That does not demonstrate barging: service still follows the queue. It does
mean that FIFO service alone is insufficient to prove that the original
caller's put eventually returns under an unbounded stream of arrivals.

**Discussion:** does fairness include eventual return for each caller under
continuing load, in addition to FIFO service? The progress argument also needs
explicit assumptions about scheduling and consumers continuing to provide
capacity.

## Ordering strengths to revisit after the obligations are settled

The plain buffer slots and `Node.value` fields are not, by themselves, defects.
Their publication paths go through cursor updates, predecessor-node exchanges,
and the `combined` flag. The proof must trace those paths, including when a
combiner acts on another caller's behalf.

The former explicit `fullFence()` in `seal()` is absent from the new queued
protocol for the reason above. Other candidates include reads of a side's own
cursor while holding its lock and volatile publication where release/acquire
may suffice. These remain candidates for justification or weakening, not accepted
changes. Cursor publication also serves the notification protocol in issue 5.

The proof should state its construction and usage assumptions: safe publication
of the channel, fresh nodes for queue entries, cursor arithmetic across overflow,
and permitted buffer sizes. These are proof boundaries to check, not additional
diagnosed bugs.

As each issue is discussed, record the agreed contract, reasoning, resolution,
and relevant checks here. Tests can corroborate the reasoning; passing stress
tests alone cannot establish that every execution allowed by the memory model
is safe.
