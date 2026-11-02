# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 136.155 | 14689141 | 80.789 | 136.155 | 540.000 | 0 |
| 2 | 139.986 | 14287128 | 80.679 | 139.986 | 560.000 | 0 |
| 3 | 138.938 | 14394938 | 80.862 | 138.938 | 550.000 | 0 |
| 4 | 137.024 | 14595937 | 81.030 | 137.024 | 550.000 | 0 |
| 5 | 138.666 | 14423162 | 81.278 | 138.666 | 560.000 | 0 |
| 6 | 158.431 | 12623793 | 82.424 | 158.431 | 630.000 | 0 |
| 7 | 136.873 | 14612096 | 80.330 | 136.873 | 550.000 | 0 |
| 8 | 137.177 | 14579694 | 80.593 | 137.177 | 550.000 | 0 |
| 9 | 140.384 | 14246642 | 80.426 | 140.384 | 560.000 | 0 |
| 10 | 137.818 | 14511922 | 80.846 | 137.818 | 550.000 | 0 |
| 11 | 138.376 | 14453396 | 80.557 | 138.376 | 550.000 | 0 |
| 12 | 174.503 | 11461144 | 104.567 | 174.503 | 580.000 | 10 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 374261176 | 187.13 | 59453384 | 433714560 |
| 2 | 373961776 | 186.98 | 59453448 | 433415224 |
| 3 | 373868336 | 186.93 | 59453448 | 433321784 |
| 4 | 373904328 | 186.95 | 59453448 | 433357776 |
| 5 | 373986976 | 186.99 | 59453384 | 433440360 |
