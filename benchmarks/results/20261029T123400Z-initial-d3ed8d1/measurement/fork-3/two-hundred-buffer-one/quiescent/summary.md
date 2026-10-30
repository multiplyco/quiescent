# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2925.561 | 3418148 | 1581.613 | 2925.561 | 11480.000 | 286 |
| 2 | 2818.079 | 3548517 | 1492.069 | 2818.079 | 11200.000 | 187 |
| 3 | 2819.374 | 3546887 | 1474.106 | 2819.374 | 11170.000 | 214 |
| 4 | 2832.991 | 3529838 | 1484.905 | 2832.991 | 11210.000 | 171 |
| 5 | 2913.052 | 3432826 | 1489.647 | 2913.052 | 11570.000 | 218 |
| 6 | 2680.180 | 3731093 | 1437.112 | 2680.180 | 10580.000 | 129 |
| 7 | 2656.929 | 3763743 | 1345.111 | 2656.929 | 10500.000 | 135 |
| 8 | 2614.004 | 3825549 | 1290.389 | 2614.004 | 10410.000 | 46 |
| 9 | 2694.497 | 3711267 | 1405.581 | 2694.497 | 10640.000 | 86 |
| 10 | 2579.978 | 3876002 | 1314.878 | 2579.978 | 10220.000 | 61 |
| 11 | 2657.619 | 3762766 | 1295.155 | 2657.619 | 10460.000 | 52 |
| 12 | 2552.483 | 3917754 | 1359.197 | 2552.483 | 10080.000 | 62 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1440347912 | 144.03 | 295411912 | 1735759824 |
| 2 | 1440313520 | 144.03 | 295411944 | 1735725464 |
| 3 | 1440341232 | 144.03 | 295411880 | 1735753112 |
| 4 | 1440265784 | 144.03 | 295392744 | 1735658528 |
| 5 | 1440200744 | 144.02 | 295363880 | 1735564624 |
