# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 32222.826 | 71502 | 32039.639 | 32222.826 | 269878.686 | 26 |
| 2 | 33182.564 | 69434 | 33023.353 | 33182.564 | 285662.127 | 30 |
| 3 | 34417.782 | 66942 | 34126.351 | 34417.782 | 292191.832 | 33 |
| 4 | 36286.917 | 63494 | 36053.542 | 36286.917 | 304025.623 | 23 |
| 5 | 37639.983 | 61212 | 37469.145 | 37639.983 | 311172.481 | 25 |
| 6 | 38189.959 | 60330 | 37853.530 | 38189.959 | 318893.632 | 24 |
| 7 | 38936.884 | 59173 | 38698.465 | 38936.884 | 327951.210 | 30 |
| 8 | 39822.795 | 57856 | 39674.709 | 39822.795 | 333919.314 | 25 |
| 9 | 41060.814 | 56112 | 40895.164 | 41060.814 | 345082.178 | 27 |
| 10 | 42292.430 | 54478 | 42017.635 | 42292.430 | 355402.752 | 26 |
| 11 | 43811.133 | 52589 | 43698.108 | 43811.133 | 368998.702 | 31 |
| 12 | 44738.033 | 51500 | 44598.916 | 44738.033 | 375723.312 | 14 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2015373680 | 874.73 | 76408232 | 2091781912 |
| 2 | 2015003376 | 874.57 | 76401320 | 2091404696 |
| 3 | 2006405616 | 870.84 | 76405160 | 2082810776 |
| 4 | 2014110208 | 874.18 | 76409416 | 2090519624 |
| 5 | 2015036368 | 874.58 | 76409224 | 2091445592 |
