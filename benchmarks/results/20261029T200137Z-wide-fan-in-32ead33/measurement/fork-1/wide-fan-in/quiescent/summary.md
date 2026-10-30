# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 82.142 | 28048979 | 47.646 | 82.142 | 330.000 | 0 |
| 2 | 82.819 | 27819768 | 48.970 | 82.819 | 330.000 | 0 |
| 3 | 96.752 | 23813349 | 59.408 | 96.752 | 350.000 | 0 |
| 4 | 100.572 | 22909032 | 57.708 | 100.572 | 390.000 | 16 |
| 5 | 96.009 | 23997673 | 52.055 | 96.009 | 350.000 | 0 |
| 6 | 110.388 | 20871920 | 52.791 | 110.388 | 420.000 | 0 |
| 7 | 92.778 | 24833440 | 49.046 | 92.778 | 360.000 | 9 |
| 8 | 82.018 | 28091254 | 47.429 | 82.018 | 330.000 | 0 |
| 9 | 91.536 | 25170368 | 54.056 | 91.536 | 360.000 | 0 |
| 10 | 97.954 | 23521360 | 46.912 | 97.953 | 380.000 | 15 |
| 11 | 83.223 | 27684600 | 49.767 | 83.223 | 330.000 | 0 |
| 12 | 84.878 | 27144781 | 48.316 | 84.878 | 340.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 228657504 | 99.24 | 76933824 | 305591328 |
| 2 | 227434304 | 98.71 | 76950448 | 304384752 |
| 3 | 227294808 | 98.65 | 76933856 | 304228664 |
| 4 | 227168864 | 98.60 | 76933824 | 304102688 |
| 5 | 227013144 | 98.53 | 76933856 | 303947000 |
