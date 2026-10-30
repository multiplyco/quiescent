# JVM channel baseline

Status: `complete`. Profile: `full`. Completed 72 / 72 fresh JVMs.

Primary metric: common start release through all transfer/drain work, including scheduler waits, transducers, checksum/count accumulation, channel closing and wakeups. Channel, payload and worker creation, validation and task teardown are outside timing. These are finite-batch durations, not per-operation latency.

Each row summarizes independent JVM means. All raw trials and JVMs are retained. Spread is (max - min) / median; it is not a confidence interval. One fork does not establish JVM variability.

| Case | Implementation | JVMs | Median ms | Min ms | Max ms | Spread % |
|---|---|---:|---:|---:|---:|---:|
| fan-in-32 | core-async | 4 | 9023.973 | 8959.168 | 9102.484 | 1.59 |
| fan-in-32 | quiescent | 4 | 170.461 | 162.240 | 225.306 | 37.00 |
| fan-out-32 | core-async | 4 | 9117.374 | 9053.903 | 9293.107 | 2.62 |
| fan-out-32 | quiescent | 4 | 208.102 | 205.969 | 222.171 | 7.79 |
| fifty | core-async | 4 | 9560.916 | 9446.549 | 9670.514 | 2.34 |
| fifty | quiescent | 4 | 193.538 | 189.443 | 195.706 | 3.24 |
| fifty-mpmc | core-async | 4 | 9971.203 | 9836.541 | 10065.780 | 2.30 |
| fifty-mpmc | quiescent | 4 | 231.683 | 222.985 | 236.711 | 5.92 |
| mapcat | core-async | 4 | 7103.204 | 7039.250 | 7164.893 | 1.77 |
| mapcat | quiescent | 4 | 172.579 | 168.630 | 179.504 | 6.30 |
| mixed | core-async | 4 | 6389.438 | 6282.057 | 6449.742 | 2.62 |
| mixed | quiescent | 4 | 150.874 | 140.250 | 154.961 | 9.75 |
| pipe-map | core-async | 4 | 5589.187 | 5550.571 | 5622.048 | 1.28 |
| pipe-map | quiescent | 4 | 147.915 | 142.861 | 164.238 | 14.45 |
| single | core-async | 4 | 4277.034 | 4231.334 | 4293.700 | 1.46 |
| single | quiescent | 4 | 178.224 | 171.392 | 179.853 | 4.75 |
| two-hundred-buffer-one | core-async | 4 | 33638.929 | 33451.163 | 33846.884 | 1.18 |
| two-hundred-buffer-one | quiescent | 4 | 2867.823 | 2728.729 | 3112.711 | 13.39 |

## Comparison

Ratio is core.async / Quiescent elapsed time (>1 favors Quiescent). Per-fork ratios pair adjacent implementation workers; ratio of medians is reported separately.

| Case | Ratio of medians | Per-fork ratios |
|---|---:|---|
| fan-in-32 | 52.939x | 56.105x, 53.755x, 51.924x, 39.932x |
| fan-out-32 | 43.812x | 44.214x, 43.448x, 43.926x, 41.829x |
| fifty | 49.401x | 50.519x, 49.865x, 48.743x, 48.978x |
| fifty-mpmc | 43.038x | 43.173x, 45.141x, 41.555x, 42.903x |
| mapcat | 41.159x | 41.744x, 39.317x, 41.449x, 41.492x |
| mixed | 42.350x | 44.792x, 41.615x, 41.622x, 43.119x |
| pipe-map | 37.787x | 37.690x, 38.853x, 34.231x, 37.884x |
| single | 23.998x | 24.125x, 24.006x, 23.527x, 24.912x |
| two-hundred-buffer-one | 11.730x | 10.747x, 12.153x, 12.323x, 11.409x |

## Individual JVM observations

| Fork | Case | Implementation | Mean ms | Details |
|---:|---|---|---:|---|
| 1 | single | quiescent | 177.978 | [Trials](fork-1/single/quiescent/summary.md) |
| 1 | single | core-async | 4293.700 | [Trials](fork-1/single/core-async/summary.md) |
| 1 | fifty | quiescent | 191.423 | [Trials](fork-1/fifty/quiescent/summary.md) |
| 1 | fifty | core-async | 9670.514 | [Trials](fork-1/fifty/core-async/summary.md) |
| 1 | fifty-mpmc | quiescent | 231.983 | [Trials](fork-1/fifty-mpmc/quiescent/summary.md) |
| 1 | fifty-mpmc | core-async | 10015.292 | [Trials](fork-1/fifty-mpmc/core-async/summary.md) |
| 1 | two-hundred-buffer-one | quiescent | 3112.711 | [Trials](fork-1/two-hundred-buffer-one/quiescent/summary.md) |
| 1 | two-hundred-buffer-one | core-async | 33451.163 | [Trials](fork-1/two-hundred-buffer-one/core-async/summary.md) |
| 1 | mixed | quiescent | 140.250 | [Trials](fork-1/mixed/quiescent/summary.md) |
| 1 | mixed | core-async | 6282.057 | [Trials](fork-1/mixed/core-async/summary.md) |
| 1 | fan-in-32 | quiescent | 162.240 | [Trials](fork-1/fan-in-32/quiescent/summary.md) |
| 1 | fan-in-32 | core-async | 9102.484 | [Trials](fork-1/fan-in-32/core-async/summary.md) |
| 1 | fan-out-32 | quiescent | 205.969 | [Trials](fork-1/fan-out-32/quiescent/summary.md) |
| 1 | fan-out-32 | core-async | 9106.771 | [Trials](fork-1/fan-out-32/core-async/summary.md) |
| 1 | mapcat | quiescent | 168.630 | [Trials](fork-1/mapcat/quiescent/summary.md) |
| 1 | mapcat | core-async | 7039.250 | [Trials](fork-1/mapcat/core-async/summary.md) |
| 1 | pipe-map | quiescent | 149.081 | [Trials](fork-1/pipe-map/quiescent/summary.md) |
| 1 | pipe-map | core-async | 5618.906 | [Trials](fork-1/pipe-map/core-async/summary.md) |
| 2 | single | core-async | 4284.380 | [Trials](fork-2/single/core-async/summary.md) |
| 2 | single | quiescent | 178.471 | [Trials](fork-2/single/quiescent/summary.md) |
| 2 | fifty | core-async | 9446.549 | [Trials](fork-2/fifty/core-async/summary.md) |
| 2 | fifty | quiescent | 189.443 | [Trials](fork-2/fifty/quiescent/summary.md) |
| 2 | fifty-mpmc | core-async | 10065.780 | [Trials](fork-2/fifty-mpmc/core-async/summary.md) |
| 2 | fifty-mpmc | quiescent | 222.985 | [Trials](fork-2/fifty-mpmc/quiescent/summary.md) |
| 2 | two-hundred-buffer-one | core-async | 33652.697 | [Trials](fork-2/two-hundred-buffer-one/core-async/summary.md) |
| 2 | two-hundred-buffer-one | quiescent | 2769.032 | [Trials](fork-2/two-hundred-buffer-one/quiescent/summary.md) |
| 2 | mixed | core-async | 6424.597 | [Trials](fork-2/mixed/core-async/summary.md) |
| 2 | mixed | quiescent | 154.381 | [Trials](fork-2/mixed/quiescent/summary.md) |
| 2 | fan-in-32 | core-async | 9051.067 | [Trials](fork-2/fan-in-32/core-async/summary.md) |
| 2 | fan-in-32 | quiescent | 168.377 | [Trials](fork-2/fan-in-32/quiescent/summary.md) |
| 2 | fan-out-32 | core-async | 9127.976 | [Trials](fork-2/fan-out-32/core-async/summary.md) |
| 2 | fan-out-32 | quiescent | 210.088 | [Trials](fork-2/fan-out-32/quiescent/summary.md) |
| 2 | mapcat | core-async | 7057.583 | [Trials](fork-2/mapcat/core-async/summary.md) |
| 2 | mapcat | quiescent | 179.504 | [Trials](fork-2/mapcat/quiescent/summary.md) |
| 2 | pipe-map | core-async | 5550.571 | [Trials](fork-2/pipe-map/core-async/summary.md) |
| 2 | pipe-map | quiescent | 142.861 | [Trials](fork-2/pipe-map/quiescent/summary.md) |
| 3 | single | quiescent | 179.853 | [Trials](fork-3/single/quiescent/summary.md) |
| 3 | single | core-async | 4231.334 | [Trials](fork-3/single/core-async/summary.md) |
| 3 | fifty | quiescent | 195.652 | [Trials](fork-3/fifty/quiescent/summary.md) |
| 3 | fifty | core-async | 9536.599 | [Trials](fork-3/fifty/core-async/summary.md) |
| 3 | fifty-mpmc | quiescent | 236.711 | [Trials](fork-3/fifty-mpmc/quiescent/summary.md) |
| 3 | fifty-mpmc | core-async | 9836.541 | [Trials](fork-3/fifty-mpmc/core-async/summary.md) |
| 3 | two-hundred-buffer-one | quiescent | 2728.729 | [Trials](fork-3/two-hundred-buffer-one/quiescent/summary.md) |
| 3 | two-hundred-buffer-one | core-async | 33625.161 | [Trials](fork-3/two-hundred-buffer-one/core-async/summary.md) |
| 3 | mixed | quiescent | 154.961 | [Trials](fork-3/mixed/quiescent/summary.md) |
| 3 | mixed | core-async | 6449.742 | [Trials](fork-3/mixed/core-async/summary.md) |
| 3 | fan-in-32 | quiescent | 172.544 | [Trials](fork-3/fan-in-32/quiescent/summary.md) |
| 3 | fan-in-32 | core-async | 8959.168 | [Trials](fork-3/fan-in-32/core-async/summary.md) |
| 3 | fan-out-32 | quiescent | 206.115 | [Trials](fork-3/fan-out-32/quiescent/summary.md) |
| 3 | fan-out-32 | core-async | 9053.903 | [Trials](fork-3/fan-out-32/core-async/summary.md) |
| 3 | mapcat | quiescent | 172.862 | [Trials](fork-3/mapcat/quiescent/summary.md) |
| 3 | mapcat | core-async | 7164.893 | [Trials](fork-3/mapcat/core-async/summary.md) |
| 3 | pipe-map | quiescent | 164.238 | [Trials](fork-3/pipe-map/quiescent/summary.md) |
| 3 | pipe-map | core-async | 5622.048 | [Trials](fork-3/pipe-map/core-async/summary.md) |
| 4 | single | core-async | 4269.688 | [Trials](fork-4/single/core-async/summary.md) |
| 4 | single | quiescent | 171.392 | [Trials](fork-4/single/quiescent/summary.md) |
| 4 | fifty | core-async | 9585.233 | [Trials](fork-4/fifty/core-async/summary.md) |
| 4 | fifty | quiescent | 195.706 | [Trials](fork-4/fifty/quiescent/summary.md) |
| 4 | fifty-mpmc | core-async | 9927.113 | [Trials](fork-4/fifty-mpmc/core-async/summary.md) |
| 4 | fifty-mpmc | quiescent | 231.383 | [Trials](fork-4/fifty-mpmc/quiescent/summary.md) |
| 4 | two-hundred-buffer-one | core-async | 33846.884 | [Trials](fork-4/two-hundred-buffer-one/core-async/summary.md) |
| 4 | two-hundred-buffer-one | quiescent | 2966.614 | [Trials](fork-4/two-hundred-buffer-one/quiescent/summary.md) |
| 4 | mixed | core-async | 6354.279 | [Trials](fork-4/mixed/core-async/summary.md) |
| 4 | mixed | quiescent | 147.366 | [Trials](fork-4/mixed/quiescent/summary.md) |
| 4 | fan-in-32 | core-async | 8996.879 | [Trials](fork-4/fan-in-32/core-async/summary.md) |
| 4 | fan-in-32 | quiescent | 225.306 | [Trials](fork-4/fan-in-32/quiescent/summary.md) |
| 4 | fan-out-32 | core-async | 9293.107 | [Trials](fork-4/fan-out-32/core-async/summary.md) |
| 4 | fan-out-32 | quiescent | 222.171 | [Trials](fork-4/fan-out-32/quiescent/summary.md) |
| 4 | mapcat | core-async | 7148.824 | [Trials](fork-4/mapcat/core-async/summary.md) |
| 4 | mapcat | quiescent | 172.295 | [Trials](fork-4/mapcat/quiescent/summary.md) |
| 4 | pipe-map | core-async | 5559.468 | [Trials](fork-4/pipe-map/core-async/summary.md) |
| 4 | pipe-map | quiescent | 146.749 | [Trials](fork-4/pipe-map/quiescent/summary.md) |

Per-worker run.edn files retain samples, channel completion times, consumer counts, CPU/GC counters, warmup, exact pre/postflight validation and process identity. CPU/GC observations bracket the timed phase and include some coordination/measurement overhead. Allocation uses separate whole-JVM passes.

## Allocated heap bytes

Each observation is a separate allocation pass after timing. Transfer includes release through worker teardown; setup includes payloads/channels/workers. All JVM threads are counted, including background activity; controls are not subtracted.

| Fork | Case | Implementation | Transfer bytes/delivered value | Setup bytes/trial |
|---:|---|---|---:|---:|
| 1 | single | quiescent | 97.53 | 29446130 |
| 1 | single | core-async | 441.95 | 29437368 |
| 1 | fifty | quiescent | 96.78 | 147524005 |
| 1 | fifty | core-async | 319.13 | 147403909 |
| 1 | fifty-mpmc | quiescent | 98.59 | 148337526 |
| 1 | fifty-mpmc | core-async | 326.04 | 148253936 |
| 1 | two-hundred-buffer-one | quiescent | 144.03 | 295868256 |
| 1 | two-hundred-buffer-one | core-async | 629.60 | 295355942 |
| 1 | mixed | quiescent | 101.19 | 91669499 |
| 1 | mixed | core-async | 355.12 | 91621117 |
| 1 | fan-in-32 | quiescent | 104.49 | 70095883 |
| 1 | fan-in-32 | core-async | 765.22 | 70052638 |
| 1 | fan-out-32 | quiescent | 109.07 | 70180208 |
| 1 | fan-out-32 | core-async | 710.40 | 70026365 |
| 1 | mapcat | quiescent | 96.75 | 73930848 |
| 1 | mapcat | core-async | 298.78 | 73858742 |
| 1 | pipe-map | quiescent | 187.02 | 59428962 |
| 1 | pipe-map | core-async | 606.41 | 59385698 |
| 2 | single | core-async | 432.91 | 29437320 |
| 2 | single | quiescent | 96.80 | 29446046 |
| 2 | fifty | core-async | 319.13 | 147403939 |
| 2 | fifty | quiescent | 96.79 | 147539845 |
| 2 | fifty-mpmc | core-async | 326.04 | 148253917 |
| 2 | fifty-mpmc | quiescent | 98.50 | 148369706 |
| 2 | two-hundred-buffer-one | core-async | 629.42 | 295354803 |
| 2 | two-hundred-buffer-one | quiescent | 144.03 | 295591738 |
| 2 | mixed | core-async | 354.98 | 91621072 |
| 2 | mixed | quiescent | 101.20 | 91689712 |
| 2 | fan-in-32 | core-async | 772.87 | 70052709 |
| 2 | fan-in-32 | quiescent | 104.50 | 70146558 |
| 2 | fan-out-32 | core-async | 711.16 | 70025968 |
| 2 | fan-out-32 | quiescent | 109.14 | 70205514 |
| 2 | mapcat | core-async | 298.77 | 73858662 |
| 2 | mapcat | quiescent | 96.76 | 73916992 |
| 2 | pipe-map | core-async | 606.44 | 59385666 |
| 2 | pipe-map | quiescent | 187.00 | 59453422 |
| 3 | single | quiescent | 96.82 | 29446083 |
| 3 | single | core-async | 427.06 | 29437368 |
| 3 | fifty | quiescent | 96.81 | 147532632 |
| 3 | fifty | core-async | 318.79 | 147403896 |
| 3 | fifty-mpmc | quiescent | 98.60 | 148340939 |
| 3 | fifty-mpmc | core-async | 325.95 | 148253891 |
| 3 | two-hundred-buffer-one | quiescent | 144.03 | 295398472 |
| 3 | two-hundred-buffer-one | core-async | 629.19 | 295355968 |
| 3 | mixed | quiescent | 101.19 | 91671859 |
| 3 | mixed | core-async | 355.10 | 91621072 |
| 3 | fan-in-32 | quiescent | 104.40 | 70108594 |
| 3 | fan-in-32 | core-async | 773.32 | 70052741 |
| 3 | fan-out-32 | quiescent | 109.15 | 70180150 |
| 3 | fan-out-32 | core-async | 710.76 | 70026094 |
| 3 | mapcat | quiescent | 96.76 | 73915392 |
| 3 | mapcat | core-async | 298.78 | 73858682 |
| 3 | pipe-map | quiescent | 186.98 | 59470771 |
| 3 | pipe-map | core-async | 606.70 | 59385717 |
| 4 | single | core-async | 449.52 | 29437362 |
| 4 | single | quiescent | 97.34 | 29446142 |
| 4 | fifty | core-async | 318.71 | 147403909 |
| 4 | fifty | quiescent | 96.81 | 147503512 |
| 4 | fifty-mpmc | core-async | 326.15 | 148253891 |
| 4 | fifty-mpmc | quiescent | 98.61 | 148325686 |
| 4 | two-hundred-buffer-one | core-async | 630.01 | 295355757 |
| 4 | two-hundred-buffer-one | quiescent | 144.04 | 295452872 |
| 4 | mixed | core-async | 354.94 | 91621027 |
| 4 | mixed | quiescent | 101.19 | 91661635 |
| 4 | fan-in-32 | core-async | 774.51 | 70078251 |
| 4 | fan-in-32 | quiescent | 104.46 | 70121266 |
| 4 | fan-out-32 | core-async | 710.80 | 70025942 |
| 4 | fan-out-32 | quiescent | 109.13 | 70180210 |
| 4 | mapcat | core-async | 298.77 | 73858643 |
| 4 | mapcat | quiescent | 96.76 | 73913632 |
| 4 | pipe-map | core-async | 606.42 | 59385686 |
| 4 | pipe-map | quiescent | 187.05 | 59428930 |
