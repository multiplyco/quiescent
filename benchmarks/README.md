# JVM channel benchmarks

The primary question is how channels behave together under scheduler and memory
pressure. Independent channels run concurrently *within* each measurement;
benchmark JVMs run sequentially. Single-channel results are diagnostic controls.
This suite is JVM-only. CLJS performance is outside its scope.

```sh
bb bench:check
clojure -Srepro -X:bench :list? true
bb bench :profile :smoke :forks 1 :suite :all
bb bench :cases '[:fifty :fan-in-32]' :profile :quick :forks 1
bb bench :forks 4 :label '"Initial channel baseline"'
```

`bb bench` compiles Java, runs JVM library correctness tests and benchmark checks,
then runs the selected comparisons. `:list?` with the direct Clojure command lists
the selection without tests or output creation. `:cases` explicitly selects any
case in the catalog, overriding the suite selection. Unknown options, duplicate
selections and misspelled case names fail before a run starts. An optional
`:output` is an EDN string naming a **new** directory.

The nine primary cases are single-channel 1P1C, 50×1P1C, 50×4P4C, 200×1P1C with
buffer 1, the mixed 40-workload case, 24×32P1C, 24×1P32C, 50×mapcat, and 20×map
relay. The original 25 topologies remain available with `:suite :all`.
`bench/quiescent/scenarios.clj` records every buffer size, message count and
producer/consumer count. The mixed case contains 30 ordinary channels and ten
two-channel ping-pong pairs. A relay uses two channels and one forwarding actor;
its loop uses the same put/take primitives in both implementations. It does not
measure the public `pipe` helper's setup/lifecycle machinery.

## Measurement boundary

Every trial constructs channels, prebuilt integer payloads and all workers before
starting the clock. Every worker reports readiness and awaits a start gate.
The controller records the start time and releases one gate per workload, in the
same order for both implementations. Separate gates avoid core.async's limit on
pending operations on a single channel. Gate release and scheduling are timed.
The end is the last actor's transfer/drain completion timestamp. There is no
clock read or shared counter per message.

Timed work includes puts/takes, scheduler waits, transducers, per-consumer count
and sum accumulation, producer completion coordination, sealing/closing and
waking drained consumers. Channel/worker/payload creation, result validation,
artifact serialization and task teardown are outside timing. Each trial uses
fresh channels, so this is a finite-batch transfer benchmark, not an indefinitely
running steady-state stream. Setup allocates memory and can influence subsequent
GC even though its elapsed time is excluded. GC occurring during transfer counts.

Both arms run the same generated loops. Quiescent uses `q/task` and its channel
API; core.async uses `go` with `>!`/`<!`. The comparison includes each library's
execution model. It does not isolate the channel data structure from scheduling.
Dependencies are pinned by `:bench` (Clojure 1.12.4, core.async 1.9.865), user
Clojure configuration is excluded with `-Srepro`, and heap size is fixed at 2 GiB.
Default schedulers are preserved and their override properties are recorded.
Do not assume the old `clojure.core.async.pool-size` property controls this version.

Each fresh worker JVM runs one topology and implementation. The full profile
warms up for at least ten seconds of measured channel work and at least three
trials, then records twelve timing trials and five separate allocation trials. A trial always completes its fixed workload;
it is not cut short at a time limit. The default four forks per arm/case give
72 JVMs for the primary suite. Forward/reverse arm order alternates between
forks (AB, BA, AB, BA), with adjacent arms on the same topology. Full runs may take
hours, especially the buffer-one and contention cases. `:quick` uses full
workload sizes with reduced warmup/repetition; `:smoke` also reduces messages per
producer while retaining worker/channel counts. Neither is a performance baseline.

`:timeout-ms` (default 120000) bounds readiness and transfer separately. A
coordinator deadline also kills a stalled worker JVM; a timeout fails the batch
and preserves previous results. Increase the timeout explicitly if a valid slow
case needs it; never silently omit a slow or failing case. No measurements overlap.

## Correctness and interpretation

Every topology/implementation has an exact-content preflight and postflight with
small message counts but the original concurrency. These compare the complete
multiset of received values against an independently generated expectation.
Every timed trial verifies expected counts and sums for every workload. Those
checks are cheaper but cannot prove absence of compensating loss/duplication;
the exact checks and library tests supply the complementary evidence. Counts and
expectations include filter drops and mapcat expansion. All producers must finish,
and all consumers drain before a sample is accepted.

The library's FIFO lock/channel tests remain correctness prerequisites. Throughput
alone does not establish no-barging or starvation freedom. Per-channel completion
times and per-consumer delivered counts are retained to expose uneven progress;
they are not per-operation tail latency or a proof of fairness. Fixed-duration
fairness/latency experiments can be added separately without instrumenting the
throughput hot path. `alt` is excluded from this initial reference: its current
implementation is still experimental and is not an equivalent full alts suite.

The report retains every JVM mean, the median/min/max across JVMs, descriptive
spread, every adjacent per-fork ratio, and the ratio of implementation medians.
Ratios are always core.async / Quiescent elapsed time. Inspect variation before
interpreting small changes; no outliers are removed and no automatic pass/fail
performance threshold is applied. These are batch durations, not operation
latencies. Dividing delivered values by elapsed time yields aggregate delivered
values/sec, not per-channel throughput or a universal operation count: ping-pong
and relay perform more channel transfers per delivered value.

Process CPU and GC counters bracket each transfer phase through worker teardown.
They cover the whole JVM and include a little measurement/coordination work.
CPU time per completed workload helps distinguish efficient progress from using
more CPU.

Allocation uses separate trials **after** timing, with the JDK 21+
[`getTotalThreadAllocatedBytes`](https://docs.oracle.com/en/java/javase/26/docs/api/jdk.management/com/sun/management/ThreadMXBean.html#getTotalThreadAllocatedBytes())
counter. This approximates total allocated heap bytes across all JVM threads,
including terminated threads, rather than sampling retained heap or summing only
currently live threads. Each worker first verifies that the runtime counts at
least 1 MiB of escaping byte-array payload allocated by a virtual thread and still
counts it after termination. Unsupported runtimes report `:unsupported`; a failed
probe fails the run. Totem's actual runtime must pass this probe for an allocation
baseline.

Every allocation trial records setup bytes separately from transfer bytes.
Setup starts before channel/payload/worker construction and ends once all workers
are ready. Transfer spans gate release through worker teardown, before result
validation. It includes channel operations, scheduler/runtime activity, count/sum
accumulation, result handoff and small measurement costs. Whole-JVM background
allocation is included. There is no per-message allocation instrumentation.
The sum is also retained; it excludes validation, serialization and final channel
cleanup. Transfer bytes per delivered value and setup bytes per workload trial
are reported for every JVM. Filter/mapcat change delivered counts; relay and
ping-pong perform extra transfers, so compare like topologies, not these
normalizations across unrelated cases.

Five read-pair control deltas are retained without subtraction. These controls
quantify counter-read noise, not the entire coordination harness. Allocation
trials have the same workload and count/sum validation as timing trials. All five
full-profile observations are retained; quick uses three and smoke uses one.
These are allocated bytes, not object counts, retained memory or a direct forecast
of GC pauses. Read them alongside process CPU, GC time/count, throughput and
between-JVM variation. Use separate profiled runs to attribute allocations to
classes/stacks; do not mix profiling overhead into the timing reference.

## Artifacts and repeatability

Every invocation creates a new `benchmarks/results/<run>/` directory. Retained
captures are tracked in Git, including raw measurements and supporting artifacts:

```
batch.edn                 Options, schedule, context and checkpointed progress
summary.md                All JVM observations and paired comparisons
git.patch                 Tracked changes from the recorded commit
sources.zip               Source, test, build and harness snapshot
fork-1/<case>/<framework>/
  request.edn             Exact worker request and expected context
  console.log             Worker output
  process.edn             Command, PID and exit status
  run.edn                 Raw timing/allocation trials, warmup and validation
  summary.md              Per-trial throughput, CPU/GC, completion and allocation
```

The context includes Git revision/status, source/harness/compiled-class hashes,
resolved dependency versions, loaded channel/core.async locations, Java build,
JVM options, CPU model/count, OS, heap, collectors and scheduler properties.
Workers must match the coordinator's context before and after measurement.
Results are accepted only after verifying the request, process, source/settings,
fixture, sample count, warmup and validation. Output directories are never reused.
Only a complete full-profile capture with all repetitions is a baseline candidate;
review host activity and between-JVM variability before accepting it.

Keep the same hardware, JDK, OS, dependencies, heap/scheduler settings, harness,
case definitions and measurement profile for comparisons. Rerun fresh controls
alongside candidates for small or surprising changes. Older Markdown tables used
different timing boundaries, core.async versions and a shared JVM; they remain
historical exploratory results and are not directly comparable to this reference.

The dedicated server's launcher, checkout paths, lock and operational records
are documented on the server. Use its account-wide lock for correctness preflight
and measurements. Keep server setup/run notes there, outside library Git history.
Local smoke/correctness runs do not require the server. Results from different
machines must not be pooled.

`bb bench:legacy` retains the former exploratory Criterium command. Its concurrent
coordination uses `qjoin`; the former `qdo` calls predated sequential `qdo` semantics.
