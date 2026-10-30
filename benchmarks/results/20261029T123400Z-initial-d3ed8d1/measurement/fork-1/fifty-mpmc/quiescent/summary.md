# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 229.460 | 21790277 | 126.417 | 229.460 | 920.000 | 0 |
| 2 | 221.823 | 22540513 | 116.955 | 221.823 | 880.000 | 0 |
| 3 | 290.179 | 17230756 | 182.661 | 290.179 | 1150.000 | 54 |
| 4 | 217.970 | 22938941 | 112.841 | 217.970 | 870.000 | 0 |
| 5 | 221.886 | 22534084 | 117.652 | 221.886 | 880.000 | 0 |
| 6 | 219.544 | 22774476 | 113.583 | 219.544 | 870.000 | 5 |
| 7 | 217.228 | 23017246 | 114.296 | 217.228 | 870.000 | 0 |
| 8 | 233.833 | 21382791 | 115.739 | 233.833 | 930.000 | 17 |
| 9 | 216.463 | 23098681 | 114.013 | 216.463 | 870.000 | 0 |
| 10 | 264.520 | 18902129 | 161.421 | 264.520 | 1050.000 | 41 |
| 11 | 216.281 | 23118073 | 113.065 | 216.281 | 860.000 | 0 |
| 12 | 234.605 | 21312451 | 131.575 | 234.605 | 830.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 493236000 | 98.65 | 148350480 | 641586480 |
| 2 | 492987120 | 98.60 | 148350480 | 641337600 |
| 3 | 492890480 | 98.58 | 148328880 | 641219360 |
| 4 | 492800568 | 98.56 | 148328912 | 641129480 |
| 5 | 492842664 | 98.57 | 148328880 | 641171544 |
