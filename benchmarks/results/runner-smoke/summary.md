# JVM channel baseline

Status: `complete`. Profile: `smoke`. Completed 4 / 4 fresh JVMs.

Exploratory measurements; not a performance baseline.

Primary metric: common start release through all transfer/drain work, including scheduler waits, transducers, checksum/count accumulation, channel closing and wakeups. Channel, payload and worker creation, validation and task teardown are outside timing. These are finite-batch durations, not per-operation latency.

Each row summarizes independent JVM means. All raw trials and JVMs are retained. Spread is (max - min) / median; it is not a confidence interval. One fork does not establish JVM variability.

| Case | Implementation | JVMs | Median ms | Min ms | Max ms | Spread % |
|---|---|---:|---:|---:|---:|---:|
| fan-in-128 | core-async | 1 | 347.881 | 347.881 | 347.881 | — |
| fan-in-128 | quiescent | 1 | 20.915 | 20.915 | 20.915 | — |
| single | core-async | 1 | 0.216 | 0.216 | 0.216 | — |
| single | quiescent | 1 | 0.162 | 0.162 | 0.162 | — |

## Comparison

Ratio is core.async / Quiescent elapsed time (>1 favors Quiescent). Per-fork ratios pair adjacent implementation workers; ratio of medians is reported separately.

| Case | Ratio of medians | Per-fork ratios |
|---|---:|---|
| fan-in-128 | 16.633x | 16.633x |
| single | 1.332x | 1.332x |

## Individual JVM observations

| Fork | Case | Implementation | Mean ms | Details |
|---:|---|---|---:|---|
| 1 | single | quiescent | 0.162 | [Trials](fork-1/single/quiescent/summary.md) |
| 1 | single | core-async | 0.216 | [Trials](fork-1/single/core-async/summary.md) |
| 1 | fan-in-128 | quiescent | 20.915 | [Trials](fork-1/fan-in-128/quiescent/summary.md) |
| 1 | fan-in-128 | core-async | 347.881 | [Trials](fork-1/fan-in-128/core-async/summary.md) |

Per-worker run.edn files retain samples, channel completion times, consumer counts, CPU/GC counters, warmup, exact pre/postflight validation and process identity. CPU/GC observations bracket the timed phase and include some coordination/measurement overhead. No current-thread allocation estimate is used.
