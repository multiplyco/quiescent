# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 83.102 | 27724895 | 22.829 | 83.102 | 708.816 | 0 |
| 2 | 90.326 | 25507594 | 23.829 | 90.326 | 788.290 | 0 |
| 3 | 67.288 | 34240748 | 21.814 | 67.288 | 595.583 | 0 |
| 4 | 96.236 | 23941207 | 27.526 | 96.236 | 847.044 | 6 |
| 5 | 63.787 | 36119953 | 18.266 | 63.787 | 492.730 | 0 |
| 6 | 57.799 | 39862626 | 20.980 | 57.799 | 463.571 | 0 |
| 7 | 43.984 | 52383280 | 20.363 | 43.983 | 379.388 | 0 |
| 8 | 81.548 | 28253299 | 27.243 | 81.548 | 693.986 | 6 |
| 9 | 72.655 | 31711695 | 21.390 | 72.655 | 625.725 | 0 |
| 10 | 57.386 | 40149048 | 20.144 | 57.386 | 496.326 | 0 |
| 11 | 41.389 | 55666965 | 20.830 | 41.389 | 347.542 | 0 |
| 12 | 55.060 | 41845545 | 24.834 | 55.060 | 467.035 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 231585488 | 100.51 | 76977024 | 308562512 |
| 2 | 232035504 | 100.71 | 76976992 | 309012496 |
| 3 | 231805392 | 100.61 | 76976704 | 308782096 |
| 4 | 231103392 | 100.31 | 76976896 | 308080288 |
| 5 | 229710232 | 99.70 | 76976768 | 306687000 |
