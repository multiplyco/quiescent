# JVM channel baseline

Status: `complete`. Profile: `smoke`. Completed 4 / 4 fresh JVMs.

Exploratory measurements; not a performance baseline.

Primary metric: common start release through all transfer/drain work, including scheduler waits, transducers, checksum/count accumulation, channel closing and wakeups. Channel, payload and worker creation, validation and task teardown are outside timing. These are finite-batch durations, not per-operation latency.

Each row summarizes independent JVM means. All raw trials and JVMs are retained. Spread is (max - min) / median; it is not a confidence interval. One fork does not establish JVM variability.

| Case | Implementation | JVMs | Median ms | Min ms | Max ms | Spread % |
|---|---|---:|---:|---:|---:|---:|
| fan-in-128 | core-async | 1 | 365.933 | 365.933 | 365.933 | — |
| fan-in-128 | quiescent | 1 | 14.359 | 14.359 | 14.359 | — |
| single | core-async | 1 | 0.313 | 0.313 | 0.313 | — |
| single | quiescent | 1 | 0.169 | 0.169 | 0.169 | — |

## Comparison

Ratio is core.async / Quiescent elapsed time (>1 favors Quiescent). Per-fork ratios pair adjacent implementation workers; ratio of medians is reported separately.

| Case | Ratio of medians | Per-fork ratios |
|---|---:|---|
| fan-in-128 | 25.485x | 25.485x |
| single | 1.850x | 1.850x |

## Individual JVM observations

| Fork | Case | Implementation | Mean ms | Details |
|---:|---|---|---:|---|
| 1 | single | quiescent | 0.169 | [Trials](fork-1/single/quiescent/summary.md) |
| 1 | single | core-async | 0.313 | [Trials](fork-1/single/core-async/summary.md) |
| 1 | fan-in-128 | quiescent | 14.359 | [Trials](fork-1/fan-in-128/quiescent/summary.md) |
| 1 | fan-in-128 | core-async | 365.933 | [Trials](fork-1/fan-in-128/core-async/summary.md) |

Per-worker run.edn files retain samples, channel completion times, consumer counts, CPU/GC counters, warmup, exact pre/postflight validation and process identity. CPU/GC observations bracket the timed phase and include some coordination/measurement overhead. Allocation uses separate whole-JVM passes.

## Allocated heap bytes

Each observation is a separate allocation pass after timing. Transfer includes release through worker teardown; setup includes payloads/channels/workers. All JVM threads are counted, including background activity; controls are not subtracted.

| Fork | Case | Implementation | Transfer bytes/delivered value | Setup bytes/trial |
|---:|---|---|---:|---:|
| 1 | single | quiescent | 296.00 | 30128 |
| 1 | single | core-async | 510.00 | 19312 |
| 1 | fan-in-128 | quiescent | 143.58 | 12605200 |
| 1 | fan-in-128 | core-async | 734.99 | 8673320 |
