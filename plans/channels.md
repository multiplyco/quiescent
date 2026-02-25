# Channels for Quiescent

Design notes from exploration of adding channels — continuous computation
primitives — to Quiescent.

## Motivation

Tasks represent single-value, finite-lifecycle computations (start-run-end).
Channels represent multi-value, ongoing computations — a foundation for
reactivity. The goal is a primitive powerful enough to build Electric-style
distributed reactive computation upon.

## Two Worlds

There are two fundamentally different computation models:

| | Discrete | Continuous |
|---|---|---|
| **What matters** | Every value | Latest value |
| **Read** | `take` (consuming) | `@deref` (non-consuming) |
| **Notification** | Every put | Only on change (deduplication) |
| **Duplicates** | Delivered | Skipped |
| **Buffer types** | Fixed, sliding, dropping | Memo |
| **Analogy** | Event stream | Atom with watches |

## Buffer Types

- **Fixed(n)**: Bounded buffer. Producer parks when full. Every value is
  delivered.
- **Sliding(n)**: Drops oldest when full. Producer never parks.
- **Dropping(n)**: Drops newest (incoming) when full. Producer never parks.
- **Memo**: Stores latest value only. Deduplicates — putting the same value
  is a no-op. Optionally initialized with a default: `(memo :default)` is
  non-blocking from the start, `(memo)` parks until first value.
- **None (rendezvous)**: No buffer. Both producer and consumer must be
  present simultaneously.

Memo is not sliding(1). Sliding(1) notifies on every put, even duplicates.
Memo only notifies when the value actually changes. This distinction is
critical for reactive computation efficiency.

## Core Operations

### `take`

Parks until a value is available. Returns a raw value (not a task). The
virtual thread parks and resumes — no wrapping needed.

**Cancellation propagation**: `take` on a cancelled channel propagates
cancellation to the surrounding task scope and throws CancellationException.
This means a channel's lifecycle is coupled to any task consuming from it
via `take`.

### `deref` (`@`)

Non-blocking read of the current value. Only meaningful for memo channels.

On a cancelled channel: propagates cancellation, same as `take`.

### `poll`

Safe alternative to `take`. Two-branch form:

```clojure
(q/poll [v ch]
  (recur (+ acc v))   ;; got a value
  acc)                 ;; channel is done
```

Parks until either a value arrives or the channel is cancelled. Does not
propagate cancellation — the second branch handles it explicitly.

| Operation | Channel alive | Channel cancelled |
|-----------|--------------|-------------------|
| `take` | Park, return value | Propagate cancellation |
| `@deref` | Return current value (memo) | Propagate cancellation |
| `poll` | Park, first branch | Second branch |

### `put`

Place a value on the channel. Parks if buffer is full (fixed buffer).

### `fail`

Send an exception through the channel, like failing a promise. Channel
enters a failed state. Downstream `then` chains skip, `catch` intercepts.

### `seal` / Cancellation

Open question: whether `seal` is needed as a separate verb, or whether
cancellation semantics can be defined as "cancelled after buffer is empty."

- Memo/rendezvous: no buffer, cancel is immediate.
- Fixed/sliding/dropping: cancel prevents new puts, buffer drains, then done.

Structured concurrency cascade cancellation may need to be immediate
(discard buffer) regardless. To be determined.

## Structured Concurrency

Channels follow the same scoping rules as tasks:

```clojure
;; Channel cancels with task scope
(q/task
  (let [ch (chan ...)]
    ...))

;; Channel outlives the task; only operations on it are cancelled
(let [ch (chan ...)]
  (q/task ...))
```

Things created inside a task are children of that task. Channels created
inside a task scope are closed when the parent settles. Channels created
outside are independent — but `take`/`put` operations from within a task
are cancelled if that task is cancelled.

Returning a channel from a task scope is a no-go: the channel would be
cancelled when the task settles (dead on arrival).

## Channels Are Not Groundable

Unlike tasks, channels should **not** participate in grounding. Reasons:

1. **Deadlock risk**: `(q/task (let [ch (chan)] ch))` — no producer exists,
   parks forever.
2. **Ambiguity**: sometimes you want to return the channel itself as a value,
   not take from it.
3. **Fundamental difference**: tasks produce values on their own; channels
   need an external producer.

`take` is the explicit bridge from channel-world to value-world.

## Two Patterns for Channel Usage

### Pattern 1: Return a task (recommended)

```clojure
(defn do-something []
  (q/task
    (let [ch (chan)]
      (start-producer! ch)
      (loop []
        (println (take ch))
        (recur)))))
```

Clean structured concurrency. Cancel the task, everything tears down. The
channel is an implementation detail.

### Pattern 2: Return a channel

```clojure
(defn do-something []
  (let [ch (chan)]
    (q/task
      (start-producer! ch)
      (loop []
        (println (take ch))
        (recur)))
    ch))
```

The caller gets the data stream. The backing task is internal. Caller must
manage the channel lifecycle. Use when the stream is the public interface.

## Task API on Channels

Channels implement the task API (partially). The combinators gain streaming
semantics:

### `then` — Reactive Composition

`then` on channels is the primary composition primitive. Its behavior
depends on input channel types:

```clojure
;; memo + memo = combine-latest
;; Fires when either changes, uses latest from both
(q/then memo-a memo-b f)

;; fixed + fixed = zip
;; Fires when both have a new value
(q/then fixed-a fixed-b f)

;; fixed + memo = with-latest-from
;; Fires on each fixed value, samples memo for latest
(q/then fixed-a memo-b f)
```

`then` returns a new channel object (it cannot mutate the originals). It
consumes eagerly — takes from each input as values arrive, computes once
all are available.

Single-input `then` is a map. Multi-input `then` is combine/zip depending
on buffer types.

### `catch`

Intercepts exceptions flowing through the channel. Implies channels can
carry errors (via `fail`).

### `ok`

Side-effect observer per value. Pass-through. Useful for logging/metrics.

### `finally`

Runs when the channel ends, however it ends (cancellation, drain, error).
Resource cleanup.

### Mixed task/channel inputs

```clojure
(q/then ch (q/task (fetch-config))
  (fn [v config] (process v config)))
```

The task resolves once (becoming a constant). The channel keeps varying.
Output is a channel that updates when the channel input changes. A task is
effectively a memo channel that receives one value and never changes.

## `alt` — Multiplexing

`alt` returns a channel. Non-destructive — does not cancel inputs.

```clojure
;; One value: whichever fires first
(take (alt ch1 ch2 ch3))

;; Merge: all values from all channels
(loop []
  (let [v (take (alt ch1 ch2 ch3))]
    (process v)
    (recur)))

;; Tasks: results in completion order
(let [results (alt t1 t2 t3)]
  (take results)    ;; first to complete
  (take results)    ;; second to complete
  (take results))   ;; third to complete
```

`alt`'s lifecycle is derived from its inputs. Done when all inputs are done.
No explicit lifecycle management needed.

`alt` replaces the need for a separate `merge` primitive. `merge` is just
`alt` with repeated takes.

Naming/tagging via transducers:

```clojure
(alt
  (chan (memo) (map #(vector :a %)) ch-a)
  (chan (memo) (map #(vector :b %)) ch-b))
```

## `race` vs `alt`

Both work on both tasks and channels:

| | Cancels losers | Keeps losers |
|---|---|---|
| **Tasks** | `race` | `alt` |
| **Channels** | `race` (valid but unusual) | `alt` (natural fit) |

The distinction is destructive vs non-destructive, not task vs channel.

## Reactive Computation Graph

The composition of memo channels forms a reactive DAG. Each node is a memo
channel. `then` chains define the edges. Deduplication at each node means
changes only propagate as far as they affect values — natural firebreaks.

### Scheduling / Tick Model

For UI, the right model is dirty-flag + tick (like React / requestAnimationFrame):

1. `put` updates the memo buffer immediately and marks dirty.
2. A scheduler tick collects all dirty channels.
3. Propagates in topological order (no glitches).
4. Samples `@channel` at each node (consistent snapshot).
5. Clears dirty flags.

No glitches because recomputation follows the DAG, not individual change
notifications. Memo buffers enable this naturally — reading is non-consuming
and always available.

### Compilation Target

User code could compile into a graph of memo channels connected by `then`:

```clojure
;; User writes:
(let [x (server (fetch-data))
      y (* x 2)
      z (+ x y)]
  z)

;; Compiler emits:
(let [x (chan (memo) ...)
      y (q/then x #(* % 2))
      z (q/then x y +)]
  z)
```

This is `qlet`-style dependency analysis targeting `then` on channels.
Transducers could also serve as the compilation target for single-input
transformations.

## Distributed Computation (`peer`)

Channels + tasks generalize naturally to distributed computing:

```clojure
(let [numbers (range 1000)]
  (qfor [n numbers]
    (q/peer (mod n q/connected-peers)
      (+ n n))))
```

`peer` is the boundary where execution context switches:

- `q` = run here, synchronously
- `task` = run here, virtual thread
- `cpu-task` = run here, CPU thread
- `peer` = run there, remote machine

A browser is a special case of `peer` with additional constraints
(security, sandboxing). This subsumes Electric's `e/client`/`e/server`
model — they become `(q/peer :client ...)` and `(q/peer :server ...)`.

Structured concurrency extends across the network: cancel the parent, all
remote computations cancel. Error handling and retry work as normal.

Requirements:
- Same compiled codebase on all peers (like Electric).
- Peers with different code versions cannot participate together
  (enables blue/green deployment).
- Cluster membership via code hash / build ID handshake.

Remote memo channels enable distributed reactive state — a memo channel on
peer A that peer B can `@deref` is a replicated cell.

## Relationship to Missionary

Missionary separates tasks and flows because they need different lifecycle
semantics. Tasks fit a parent-child tree; flows/channels don't always.
Missionary solves this by making flows purely compositional (descriptions,
not objects) — you never hold a flow in your hand.

Quiescent takes the opposite approach: tasks are concrete objects you hold,
pass around, deref, and cancel. Channels would follow the same philosophy.
This is more flexible but means lifecycle is the user's responsibility
(guided by structured concurrency defaults and the two patterns above).

## Implementation Designs

Two implementation approaches have been explored for fixed buffer
channels. The ring buffer design is the current preferred approach.

- **[Ring Buffer Design](channels-ringbuffer.md)** (preferred) —
  Adaptive Disruptor-inspired ring buffer. Pre-allocated arrays, zero
  allocation per operation. SPSC fast path with biased locking, lock
  on producer side for MP/transducers, XADD (getAndAdd) on consumer
  side for MC. Dekker pattern for parking notification. Requires
  buffer > 0; rendezvous is a separate implementation.

- **[Linked List Design](channels-linkedlist.md)** (historical) —
  Lock-free dual queue with dead-links-as-permits. More complex but
  handles rendezvous naturally. Contains the Payload/Taker design for
  `alt` which is still relevant to the ring buffer design.

## Open Questions

### Channel Semantics
- Exact cancel semantics for buffered channels (immediate vs drain).
- Whether cascade cancellation (structured concurrency) is always immediate.
- Output buffer type for `then`: memo if any input is memo? Fixed if all
  fixed?
- `get-now` equivalent for channels (non-throwing, non-parking read with
  default).
- Equality checking for memo deduplication: built into buffer (`=` by
  default) or configurable?
- How `alt` interacts with memo channels: does it emit the changed value,
  or `[value source-channel]`?
- Conditional reactivity: dynamic subscription/unsubscription in `if`/`when`
  forms.
