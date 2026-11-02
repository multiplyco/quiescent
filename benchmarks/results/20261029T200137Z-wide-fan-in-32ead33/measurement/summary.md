# JVM channel baseline

Status: `complete`. Profile: `full`. Completed 8 / 8 fresh JVMs.

Primary metric: common start release through all transfer/drain work, including scheduler waits, transducers, checksum/count accumulation, channel closing and wakeups. Channel, payload and worker creation, validation and task teardown are outside timing. These are finite-batch durations, not per-operation latency.

Each row summarizes independent JVM means. All raw trials and JVMs are retained. Spread is (max - min) / median; it is not a confidence interval. One fork does not establish JVM variability.

| Case | Implementation | JVMs | Median ms | Min ms | Max ms | Spread % |
|---|---|---:|---:|---:|---:|---:|
| wide-fan-in | core-async | 4 | 12139.455 | 12043.728 | 12238.876 | 1.61 |
| wide-fan-in | quiescent | 4 | 97.516 | 91.756 | 102.891 | 11.42 |

## Comparison

Ratio is core.async / Quiescent elapsed time (>1 favors Quiescent). Per-fork ratios pair adjacent implementation workers; ratio of medians is reported separately.

| Case | Ratio of medians | Per-fork ratios |
|---|---:|---|
| wide-fan-in | 124.486x | 133.226x, 117.159x, 126.141x, 122.886x |

## Individual JVM observations

| Fork | Case | Implementation | Mean ms | Details |
|---:|---|---|---:|---|
| 1 | wide-fan-in | quiescent | 91.756 | [Trials](fork-1/wide-fan-in/quiescent/summary.md) |
| 1 | wide-fan-in | core-async | 12224.246 | [Trials](fork-1/wide-fan-in/core-async/summary.md) |
| 2 | wide-fan-in | core-async | 12054.664 | [Trials](fork-2/wide-fan-in/core-async/summary.md) |
| 2 | wide-fan-in | quiescent | 102.891 | [Trials](fork-2/wide-fan-in/quiescent/summary.md) |
| 3 | wide-fan-in | quiescent | 97.025 | [Trials](fork-3/wide-fan-in/quiescent/summary.md) |
| 3 | wide-fan-in | core-async | 12238.876 | [Trials](fork-3/wide-fan-in/core-async/summary.md) |
| 4 | wide-fan-in | core-async | 12043.728 | [Trials](fork-4/wide-fan-in/core-async/summary.md) |
| 4 | wide-fan-in | quiescent | 98.008 | [Trials](fork-4/wide-fan-in/quiescent/summary.md) |

Per-worker run.edn files retain samples, channel completion times, consumer counts, CPU/GC counters, warmup, exact pre/postflight validation and process identity. CPU/GC observations bracket the timed phase and include some coordination/measurement overhead. Allocation uses separate whole-JVM passes.

## Allocated heap bytes

Each observation is a separate allocation pass after timing. Transfer includes release through worker teardown; setup includes payloads/channels/workers. All JVM threads are counted, including background activity; controls are not subtracted.

| Fork | Case | Implementation | Transfer bytes/delivered value | Setup bytes/trial |
|---:|---|---|---:|---:|
| 1 | wide-fan-in | quiescent | 98.75 | 76937162 |
| 1 | wide-fan-in | core-async | 879.09 | 76477728 |
| 2 | wide-fan-in | core-async | 879.15 | 76301824 |
| 2 | wide-fan-in | quiescent | 98.82 | 76933850 |
| 3 | wide-fan-in | quiescent | 98.63 | 76933830 |
| 3 | wide-fan-in | core-async | 879.43 | 76401069 |
| 4 | wide-fan-in | core-async | 878.76 | 76479429 |
| 4 | wide-fan-in | quiescent | 98.63 | 76983366 |
