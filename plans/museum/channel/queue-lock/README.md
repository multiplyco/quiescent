# Queue-lock channel experiment

Historical snapshot of the `throwaway` branch, preserved on 2026-10-29.
The active channel development line is `channel/main`; this implementation
predates the RelayLock direction selected for continued development.

- Source: `ba12d2380a9f1e9c9a6141d63101d36239a26530` (`WIP throwaway`, 2026-03-04).
- Fork: `c6f5ee93d25fee29da3c11421d4baf5993c3ed4b`.
- Six commits after the fork: `f0096d6`, `1b0a5b7`, `10b1929`, `d659a0a`,
  `ecc2dfb`, and `ba12d23`.

The design puts separate producer/consumer ownership fields and intrusive
`QNode` queues directly on a bounded ring-buffer channel. Acquisition uses CAS,
with configurable spinning before parking and queue handoff between waiters.
Transducer handling lives in the same `BoundedChannel` class as plain puts.
See [the original design notes](plans/channels-queuelock.md), alongside the
adaptive and small-channel notes they reference. The notes describe the design
as it evolved and need not match every detail of the final source.

The snapshot retains the Java implementation and interfaces, Clojure facade,
benchmark harness, design notes, result tables, dependency/build definitions,
and test configuration. Paths beneath this directory mirror the original
repository. Every copied file is byte-for-byte unchanged; [manifest.json](manifest.json)
records its original Git blob and SHA-256 digest. Unchanged tests and the rest
of the library remain available through the original commit rather than being
duplicated here.

This is research material, outside the active source and test paths. It is not
a standalone project or a supported implementation. In particular, the
transducer's `Reduced` path still throws `UnsupportedOperationException` with
"Seal not yet implemented". The old harness also uses the historical parallel
meaning of `qdo`; it should not be run against current Quiescent unchanged.
To inspect or reproduce the complete historical environment, use a separate
checkout of the source commit.

The benchmark tables are historical observations from the old harness. They
do not establish a controlled comparison with the current channel benchmark
suite and must not be pooled with its timing or allocation results.

`channel/node-orientation` is a separate, later RelayLock experiment based on
`channel/main`. Its refactor, memory-ordering changes, and optional buffer-slot
clearing remain bundled for now; they are unrelated to this earlier archive.
