# Isolated channel experiments

The combined node-orientation experiment has been split into three sibling
branches from `channel/main`. Each changes one concern relative to the same
implementation, shared regression checks, and benchmark harness.

| Branch | Operation layout | HEAD/TAIL publication | Consumed slots |
|---|---|---|---|
| `channel/main` | Operations in `BoundedChannel`/`Alt` | Volatile stores | Cleared |
| `channel/node-orientation` | `PutNode`, `TakeNode`, `AltNode` | Volatile stores | Cleared |
| `channel/release-stores` | Same as main | Release stores | Cleared |
| `channel/retain-consumed-slots` | Same as main | Volatile stores | Retained by default; clearing configurable |

These are independent experiments, not a sequence of prerequisites. Compare
each with their common base before considering combinations. The branches may
need source-level reconciliation when combined because they touch the same
channel operations. No experiment is an accepted performance improvement.

## Node orientation

Move per-operation execution and combining into dedicated nodes, with a common
`Node` and a contention callback used by alt. Retain the existing volatile
publication strength and unconditional slot clearing. The obsolete nested-node
test import is removed with the refactor.

This keeps the structural part of the earlier experiment, including helper
methods and take-side accumulator fields. It does not carry the configurable
clearing constructor or weaker publication stores.

## Release stores

Change the HEAD/TAIL publication stores in the existing operation layout:
put backpressure flush, final put flush, ordinary take, combined take, and alt
take. Acquire reads and the RelayLock exchange operations remain unchanged.
Lifecycle fences and slot clearing are unchanged.

The experiment's ordering argument relies on the intervening default
`getAndSet` operations in `resume()` or node `release()`: put backpressure
publishes TAIL, resumes the take side, then checks HEAD; take publishes HEAD,
releases its node, then checks TAIL. Final put flush precedes node release and
consumer notification. Keep those operations and their order together when
reviewing the weaker stores. This separation preserves the old hypothesis;
passing stress tests alone is not a proof of correctness on every JVM/CPU.

## Retained consumed slots

Add the original `nullOnRead` constructor policy without changing operation
layout or memory ordering. One- and two-argument constructors default to
retaining consumed references, matching the combined experiment. The Java
three-argument constructor accepts `true` to restore clearing:

```java
new BoundedChannel(64, null, true);
```

The policy applies to owner reads, combined reads, and alt reads. HEAD/TAIL
still determine whether an entry exists; retained slots must not redispatch
old values after wrapping or exhaustion. A live channel can retain consumed
object graphs until overwritten or the channel becomes unreachable. Allocation
per message and retained heap are different quantities; evaluate both when
assessing this experiment. No Clojure API option is added in this split.

## Provenance and validation

The complete earlier experiment is preserved at annotated tag
`archive/channel-node-orientation-combined`, pointing to
`7c028be8ae97b449899a2a7b26114fd304f08606`. It includes the replay of original commit `52fd592` and the stale
test-import fix. Its old benchmark tables describe that combined experiment;
they are retained there rather than being attributed to the isolated variants.

Shared tests exercise slot clearing/retention through ordinary, queued, and
alt reads, ring wrap/exhaustion, and publication of newly initialized mutable
payloads under backpressure. The retention branch extends the policy cases to
cover both constructor settings. The existing channel, FIFO lock, cancellation,
transducer, alt, and whole-library tests remain applicable.

Each variant must compile from a clean class directory, pass the JVM library
tests and benchmark workload checks, and pass the focused publication/slot
checks with a single virtual-thread carrier. Those checks are correctness
validation, not performance measurements. Use matching source/harness/runtime
controls for any later performance comparison; the original historical tables
are not controls for these isolated variants.
