# JVM channel baseline

Status: `complete`. Profile: `full`. Completed 8 / 8 fresh JVMs.

Primary metric: common start release through all transfer/drain work, including scheduler waits, transducers, checksum/count accumulation, channel closing and wakeups. Channel, payload and worker creation, validation and task teardown are outside timing. These are finite-batch durations, not per-operation latency.

Each row summarizes independent JVM means. All raw trials and JVMs are retained. Spread is (max - min) / median; it is not a confidence interval. One fork does not establish JVM variability.

| Case | Implementation | JVMs | Median ms | Min ms | Max ms | Spread % |
|---|---|---:|---:|---:|---:|---:|
| wide-fan-in | core-async | 4 | 39093.396 | 38436.524 | 40910.138 | 6.33 |
| wide-fan-in | quiescent | 4 | 66.879 | 62.689 | 69.848 | 10.70 |

## Comparison

Ratio is core.async / Quiescent elapsed time (>1 favors Quiescent). Per-fork ratios pair adjacent implementation workers; ratio of medians is reported separately.

| Case | Ratio of medians | Per-fork ratios |
|---|---:|---|
| wide-fan-in | 584.539x | 613.125x, 570.720x, 585.704x, 598.637x |

## Individual JVM observations

| Fork | Case | Implementation | Mean ms | Details |
|---:|---|---|---:|---|
| 1 | wide-fan-in | quiescent | 62.689 | [Trials](fork-1/wide-fan-in/quiescent/summary.md) |
| 1 | wide-fan-in | core-async | 38436.524 | [Trials](fork-1/wide-fan-in/core-async/summary.md) |
| 2 | wide-fan-in | core-async | 38550.177 | [Trials](fork-2/wide-fan-in/core-async/summary.md) |
| 2 | wide-fan-in | quiescent | 67.547 | [Trials](fork-2/wide-fan-in/quiescent/summary.md) |
| 3 | wide-fan-in | quiescent | 69.848 | [Trials](fork-3/wide-fan-in/quiescent/summary.md) |
| 3 | wide-fan-in | core-async | 40910.138 | [Trials](fork-3/wide-fan-in/core-async/summary.md) |
| 4 | wide-fan-in | core-async | 39636.615 | [Trials](fork-4/wide-fan-in/core-async/summary.md) |
| 4 | wide-fan-in | quiescent | 66.211 | [Trials](fork-4/wide-fan-in/quiescent/summary.md) |

Per-worker run.edn files retain samples, channel completion times, consumer counts, CPU/GC counters, warmup, exact pre/postflight validation and process identity. CPU/GC observations bracket the timed phase and include some coordination/measurement overhead. Allocation uses separate whole-JVM passes.

## Allocated heap bytes

Each observation is a separate allocation pass after timing. Transfer includes release through worker teardown; setup includes payloads/channels/workers. All JVM threads are counted, including background activity; controls are not subtracted.

| Fork | Case | Implementation | Transfer bytes/delivered value | Setup bytes/trial |
|---:|---|---|---:|---:|
| 1 | wide-fan-in | quiescent | 100.16 | 77174853 |
| 1 | wide-fan-in | core-async | 873.64 | 76407419 |
| 2 | wide-fan-in | core-async | 873.78 | 76406670 |
| 2 | wide-fan-in | quiescent | 100.37 | 76976877 |
| 3 | wide-fan-in | quiescent | 99.79 | 77373054 |
| 3 | wide-fan-in | core-async | 873.10 | 76307238 |
| 4 | wide-fan-in | core-async | 873.32 | 76408174 |
| 4 | wide-fan-in | quiescent | 100.55 | 76976664 |
