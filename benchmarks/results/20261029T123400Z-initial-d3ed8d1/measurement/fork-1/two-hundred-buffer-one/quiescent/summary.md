# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3332.192 | 3001028 | 1694.943 | 3332.192 | 12580.000 | 314 |
| 2 | 3609.860 | 2770191 | 1675.600 | 3609.860 | 13180.000 | 386 |
| 3 | 3330.896 | 3002195 | 1563.309 | 3330.895 | 13010.000 | 295 |
| 4 | 3173.114 | 3151478 | 1645.530 | 3173.114 | 12510.000 | 198 |
| 5 | 3085.152 | 3241331 | 1578.246 | 3085.152 | 12120.000 | 114 |
| 6 | 2868.496 | 3486147 | 1438.335 | 2868.496 | 11420.000 | 73 |
| 7 | 2964.799 | 3372910 | 1526.484 | 2964.799 | 11750.000 | 74 |
| 8 | 3062.941 | 3264836 | 1620.898 | 3062.941 | 12060.000 | 100 |
| 9 | 3042.785 | 3286463 | 1543.051 | 3042.785 | 12040.000 | 55 |
| 10 | 2884.897 | 3466329 | 1511.651 | 2884.897 | 11400.000 | 76 |
| 11 | 3042.963 | 3286271 | 1604.738 | 3042.962 | 11940.000 | 117 |
| 12 | 2954.434 | 3384743 | 1496.958 | 2954.434 | 11710.000 | 57 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1440360096 | 144.04 | 295901480 | 1736261576 |
| 2 | 1440318416 | 144.03 | 295901544 | 1736219960 |
| 3 | 1440355392 | 144.04 | 295872712 | 1736228104 |
| 4 | 1440362640 | 144.04 | 295840736 | 1736203376 |
| 5 | 1440292064 | 144.03 | 295824808 | 1736116872 |
