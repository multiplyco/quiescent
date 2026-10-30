# JVM channel baseline

Status: `complete`. Profile: `smoke`. Completed 2 / 2 fresh JVMs.

Exploratory measurements; not a performance baseline.

Primary metric: common start release through all transfer/drain work, including scheduler waits, transducers, checksum/count accumulation, channel closing and wakeups. Channel, payload and worker creation, validation and task teardown are outside timing. These are finite-batch durations, not per-operation latency.

Each row summarizes independent JVM means. All raw trials and JVMs are retained. Spread is (max - min) / median; it is not a confidence interval. One fork does not establish JVM variability.

| Case | Implementation | JVMs | Median ms | Min ms | Max ms | Spread % |
|---|---|---:|---:|---:|---:|---:|
| wide-fan-in | core-async | 1 | 202.127 | 202.127 | 202.127 | — |
| wide-fan-in | quiescent | 1 | 12.268 | 12.268 | 12.268 | — |

## Comparison

Ratio is core.async / Quiescent elapsed time (>1 favors Quiescent). Per-fork ratios pair adjacent implementation workers; ratio of medians is reported separately.

| Case | Ratio of medians | Per-fork ratios |
|---|---:|---|
| wide-fan-in | 16.476x | 16.476x |

## Individual JVM observations

| Fork | Case | Implementation | Mean ms | Details |
|---:|---|---|---:|---|
| 1 | wide-fan-in | quiescent | 12.268 | [Trials](fork-1/wide-fan-in/quiescent/summary.md) |
| 1 | wide-fan-in | core-async | 202.127 | [Trials](fork-1/wide-fan-in/core-async/summary.md) |

Per-worker run.edn files retain samples, channel completion times, consumer counts, CPU/GC counters, warmup, exact pre/postflight validation and process identity. CPU/GC observations bracket the timed phase and include some coordination/measurement overhead. Allocation uses separate whole-JVM passes.

## Allocated heap bytes

Each observation is a separate allocation pass after timing. Transfer includes release through worker teardown; setup includes payloads/channels/workers. All JVM threads are counted, including background activity; controls are not subtracted.

| Fork | Case | Implementation | Transfer bytes/delivered value | Setup bytes/trial |
|---:|---|---|---:|---:|
| 1 | wide-fan-in | quiescent | 182.59 | 14829384 |
| 1 | wide-fan-in | core-async | 518.07 | 10208264 |
