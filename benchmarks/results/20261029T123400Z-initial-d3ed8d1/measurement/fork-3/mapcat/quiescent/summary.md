# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 193.400 | 25853104 | 110.292 | 193.400 | 700.000 | 23 |
| 2 | 160.922 | 31070953 | 83.293 | 160.922 | 630.000 | 0 |
| 3 | 174.259 | 28692867 | 99.594 | 174.259 | 680.000 | 18 |
| 4 | 159.527 | 31342694 | 82.357 | 159.527 | 630.000 | 0 |
| 5 | 190.190 | 26289545 | 112.208 | 190.190 | 700.000 | 22 |
| 6 | 158.126 | 31620312 | 84.076 | 158.126 | 630.000 | 0 |
| 7 | 160.387 | 31174555 | 83.759 | 160.387 | 630.000 | 0 |
| 8 | 160.620 | 31129289 | 83.591 | 160.616 | 640.000 | 2 |
| 9 | 161.935 | 30876625 | 87.810 | 161.935 | 640.000 | 0 |
| 10 | 196.031 | 25506144 | 118.992 | 196.031 | 720.000 | 23 |
| 11 | 191.499 | 26109755 | 106.616 | 191.499 | 660.000 | 0 |
| 12 | 167.448 | 29860044 | 93.824 | 167.448 | 620.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 483778080 | 96.76 | 73915392 | 557693472 |
| 2 | 483804000 | 96.76 | 73915392 | 557719392 |
| 3 | 483819112 | 96.76 | 73915392 | 557734504 |
| 4 | 483825240 | 96.77 | 73915392 | 557740632 |
| 5 | 483818424 | 96.76 | 73915392 | 557733816 |
