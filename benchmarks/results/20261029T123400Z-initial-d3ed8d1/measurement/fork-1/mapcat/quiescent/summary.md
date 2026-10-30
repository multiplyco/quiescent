# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 159.215 | 31404147 | 82.359 | 159.215 | 630.000 | 0 |
| 2 | 197.951 | 25258726 | 115.820 | 197.951 | 740.000 | 32 |
| 3 | 161.599 | 30940772 | 87.429 | 161.599 | 640.000 | 0 |
| 4 | 157.791 | 31687479 | 82.461 | 157.791 | 620.000 | 0 |
| 5 | 166.785 | 29978658 | 83.312 | 166.785 | 660.000 | 7 |
| 6 | 161.916 | 30880301 | 87.536 | 161.916 | 630.000 | 0 |
| 7 | 178.996 | 27933535 | 106.945 | 178.996 | 690.000 | 21 |
| 8 | 159.205 | 31406065 | 83.782 | 159.205 | 630.000 | 0 |
| 9 | 166.212 | 30082039 | 88.787 | 166.212 | 640.000 | 0 |
| 10 | 196.666 | 25423866 | 122.713 | 196.666 | 710.000 | 27 |
| 11 | 158.906 | 31465104 | 82.895 | 158.906 | 630.000 | 0 |
| 12 | 158.312 | 31583210 | 82.199 | 158.312 | 630.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 483744688 | 96.75 | 73930624 | 557675312 |
| 2 | 483754456 | 96.75 | 73930624 | 557685080 |
| 3 | 483758776 | 96.75 | 73930656 | 557689432 |
| 4 | 483778048 | 96.76 | 73931712 | 557709760 |
| 5 | 483743872 | 96.75 | 73930624 | 557674496 |
