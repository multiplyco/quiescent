# Initial JVM channel capture

Integrity verified: 72 fresh JVM workers, 864 timing trials and 360 allocation trials.
All worker requests, contexts, process IDs, exit statuses, samples, warmups and validation matched.
All 125 source inputs, archived source hashes and compiled classes matched. No samples were removed.

Ratios are core.async / Quiescent. Timing and allocation are separate measurements.
This is an automated integrity/statistical report; inspect host activity and variability before accepting small differences.

| Case | Quiescent ms | core.async ms | Time ratio | Q bytes/value | async bytes/value | Allocation ratio | Q spread % | async spread % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fan-in-32 | 170.461 | 9023.973 | 52.939x | 104.47 | 773.09 | 7.400x | 37.00 | 1.59 |
| fan-out-32 | 208.102 | 9117.374 | 43.812x | 109.14 | 710.78 | 6.513x | 7.79 | 2.62 |
| fifty | 193.538 | 9560.916 | 49.401x | 96.80 | 318.96 | 3.295x | 3.24 | 2.34 |
| fifty-mpmc | 231.683 | 9971.203 | 43.038x | 98.60 | 326.04 | 3.307x | 5.92 | 2.30 |
| mapcat | 172.579 | 7103.204 | 41.159x | 96.76 | 298.77 | 3.088x | 6.30 | 1.77 |
| mixed | 150.874 | 6389.438 | 42.350x | 101.19 | 355.04 | 3.509x | 9.75 | 2.62 |
| pipe-map | 147.915 | 5589.187 | 37.787x | 187.01 | 606.43 | 3.243x | 14.45 | 1.28 |
| single | 178.224 | 4277.034 | 23.998x | 97.08 | 437.43 | 4.506x | 4.75 | 1.46 |
| two-hundred-buffer-one | 2867.823 | 33638.929 | 11.730x | 144.03 | 629.51 | 4.371x | 13.39 | 1.18 |

Times are medians of four independent JVM means. Bytes/value are medians of four JVM allocation means.
Transfer allocation includes worker teardown and JVM background activity; setup allocation is retained separately.
Keep all raw observations, source/host snapshots and vmstat logs. Historical local Markdown tables are not comparable.

[Every JVM and trial](measurement/summary.md) · [Portable observations](review.json)
