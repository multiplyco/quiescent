# Channel execution model and performance rationale

The JVM channel targets systems with many virtual threads working across many
channels. The objective is useful system throughput with FIFO service and
backpressure, while keeping carrier threads available for runnable work. The
[memory ordering review](channel-memory-ordering-review.md) tracks the contracts
and proof obligations for the implementation.

## Virtual threads and progress

A virtual thread is a logical participant. A carrier is the platform thread on
which the JVM schedules it. In an ordinary unpinned wait, a virtual thread can
unmount and release its carrier for another runnable virtual thread. Parking
does not imply a direct handoff to a particular waiter; the JVM chooses what to
schedule. Calling the channel from a platform thread, or from a pinned virtual
thread, is outside this performance assumption.
[JVM scheduling model](https://docs.oracle.com/en/java/javase/26/core/virtual-threads.html)

Distinguish three properties when reasoning about progress:

- **Carrier availability:** waiting channel operations yield execution resources
  so other virtual threads can run. This is the operational sense of avoiding
  waiting that motivates the design, rather than the formal definition of a
  wait-free operation with a bounded number of its own steps.
- **Channel progress:** given the required producers and consumers and continued
  scheduling of runnable participants, the protocol permits useful work. It must
  not strand participants through a missed notification or ownership handoff.
- **Individual completion:** each caller receives the service and eventual
  return promised by the fairness contract. Global throughput alone does not
  establish this property; unbounded combining is the open question in issue 9.

For example, with one carrier, one full channel, and takers continuing to request
values, the protocol should permit consumption without requiring that carrier
to block. A taker waiting for ownership can yield to a runnable owner; combining
can perform several callers' operations in one execution. Producers may become
runnable as space is released. This is a target for the channel protocol under
the stated scheduling assumptions, not a guarantee about every JVM or OS
scheduling decision.

Waiting for values on an empty buffer, or capacity on a full buffer, is intended
backpressure. Many parked producers can coexist with productive carriers. With
many channels, runnable work can also come from an unrelated channel.

## Why channel waits do not spin

A virtual thread spinning for another participant's progress occupies a carrier
that could execute that participant or unrelated work. With one carrier, a wait
protocol must not depend on preemption of a spinning virtual thread to schedule
its peer. With many carriers and channels, spinning can spend processor time
without increasing useful system work.

RelayLock therefore uses queued handoff and parking for waits. Its loops check
conditions and park; they do not deliberately busy-wait for ownership or buffer
progress. Suspension, scheduling, wakeups, and resumption still have costs.
Combining and buffering can amortize those costs across values.

The notification protocol remains essential. A wakeup requests another condition
check: a stored signal may predate the current wait, and sealing can wake an
empty channel. LockSupport also permits spurious returns. Safe conservative
observations must be paired with a proof that relevant progress cannot be missed
when deciding to wait. That is issue 5.
[LockSupport contract](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/concurrent/locks/LockSupport.html)

## Conservative operational observations and diagnostics

A take owner knows its consumption position. An older observation of published
production can underestimate available values. A put owner knows its working
production position. An older observation of published consumption can
underestimate free capacity. Under the sequence arithmetic assumptions, these
directions avoid reading unpublished values or overwriting unconsumed ones;
notification and retry must establish progress separately.

An outside observer owns neither position. `buf-count` samples published puts,
then published takes, and bounds their difference to zero through capacity
before narrowing to `int`. `saturation` derives a ratio from that estimate.
These queries are exact when operations are quiescent, but provide no atomic
snapshot during activity. The put-first order can underestimate occupancy;
arithmetic bounding also covers extreme sampling gaps and counter wrap.

Sampling supports monitoring and sizing decisions. Neither an observed empty
buffer nor an observed free slot reserves anything for a subsequent operation.
Occupancy samples should be interpreted alongside rates and workload phases.

Disruptor is a useful reference for conservative sequence caching and separate
capacity observations. Its multi-producer cursor tracks claims, and its wait
strategies differ from RelayLock, so those mechanisms require separate ordering
and progress arguments here.
[Single-producer sequencing](https://github.com/LMAX-Exchange/disruptor/blob/master/src/main/java/com/lmax/disruptor/SingleProducerSequencer.java),
[multi-producer sequencing](https://github.com/LMAX-Exchange/disruptor/blob/master/src/main/java/com/lmax/disruptor/MultiProducerSequencer.java)

## Buffer capacity and workload regimes

Capacity expresses how far production may get ahead of consumption. With roughly
balanced rates and enough space to absorb bursts and scheduling variation, the
two sides can progress independently and batch publication. Remaining away from
the empty/full boundaries, for example within a 10–90% occupancy band, describes
a favorable regime; it is not a prescribed sizing rule or a proven optimum.

With sustained producer surplus, throughput is limited by consumption. A larger
buffer mainly permits more backlog, retention, and queueing latency. A smaller
buffer can impose backpressure sooner. Temporary bursts present a different
tradeoff: enough spare capacity may reduce repeated boundary waits. Once those
waits cease to be important, extra capacity may add little throughput.

The ring's reference array may be inexpensive, but a larger capacity can also
retain more payload memory. Capacity one is often chosen for application
semantics; it must remain a supported case regardless of its throughput.

A separate mechanism for capacities such as 1, 2, or 4 remains deferred. A
central lock, direct value slot, or linked structure could reduce handoff costs,
but its allocation and coordination costs must be measured. Any heuristic
selection must preserve the same effective capacity, FIFO, and sealing contract.

## Benchmark implications

The [benchmark guide](../benchmarks/README.md) defines the current measurement
boundary, validation, and repeatability requirements. The execution model adds
these interpretation and future coverage requirements:

- Judge throughput with many channels active within each trial. Single-channel
  results are useful controls, but cannot establish performance under scheduler
  and memory pressure from the wider system.
- Include scheduling, channel waits, and wakeups in elapsed transfer time. Keep
  channel, worker, and payload setup outside that timing boundary, and report
  setup allocation separately from transfer allocation.
- Interpret elapsed time alongside process CPU and allocation/GC measurements.
  Parked-VT counts alone do not measure wasted carrier capacity. A spinning
  implementation must account for processor time it consumes while waiting.
- Compare like buffer sizes and producer/consumer configurations. Distinguish
  sustained backlog, balanced transfer, and bursts when developing sizing
  guidance. The current finite-batch suite does not enforce a target occupancy
  band or independently controlled arrival rates; those are future experiments.
- Keep occupancy sampling and detailed scheduler instrumentation in separate
  diagnostic runs so they do not change the throughput measurement. Do not infer
  actual park counts or carrier utilization from completion times alone.
- Exercise correctness with one carrier as well as normal parallelism. Any
  performance experiment with a scheduler override is a separate configuration,
  not interchangeable with the default-scheduler baseline.
- Validate values, FIFO behavior, and sealing independently of throughput.
  Per-channel completion times and consumer counts can expose uneven progress;
  they do not prove eventual return of every caller.

Future sizing guidance should give workload-specific ranges and the conditions
under which additional capacity helps. A single throughput number cannot select
capacity independently of application backpressure semantics.
