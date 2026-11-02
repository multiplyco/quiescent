# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 174.667 | 28625956 | 90.745 | 174.667 | 690.000 | 0 |
| 2 | 174.857 | 28594840 | 90.541 | 174.857 | 700.000 | 0 |
| 3 | 275.634 | 18139972 | 180.959 | 275.634 | 870.000 | 65 |
| 4 | 171.769 | 29108884 | 90.116 | 171.769 | 680.000 | 0 |
| 5 | 184.604 | 27085034 | 101.875 | 184.604 | 740.000 | 0 |
| 6 | 173.731 | 28780165 | 90.027 | 173.731 | 690.000 | 0 |
| 7 | 183.405 | 27262023 | 101.439 | 183.405 | 730.000 | 0 |
| 8 | 198.143 | 25234264 | 103.446 | 198.143 | 710.000 | 9 |
| 9 | 173.637 | 28795716 | 89.930 | 173.637 | 690.000 | 0 |
| 10 | 198.895 | 25138920 | 89.560 | 198.895 | 790.000 | 25 |
| 11 | 173.292 | 28853053 | 89.872 | 173.292 | 690.000 | 0 |
| 12 | 190.681 | 26221803 | 102.667 | 190.681 | 750.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 483993816 | 96.80 | 147539832 | 631533648 |
| 2 | 483938952 | 96.79 | 147539832 | 631478784 |
| 3 | 483975376 | 96.80 | 147539832 | 631515208 |
| 4 | 483960312 | 96.79 | 147539832 | 631500144 |
| 5 | 483913368 | 96.78 | 147539896 | 631453264 |
