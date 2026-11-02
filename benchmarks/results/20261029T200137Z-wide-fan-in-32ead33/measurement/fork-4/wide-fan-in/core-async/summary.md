# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 11937.095 | 193012 | 11751.658 | 11937.095 | 47440.000 | 40 |
| 2 | 11969.470 | 192490 | 11713.010 | 11969.470 | 47630.000 | 65 |
| 3 | 11993.604 | 192102 | 11792.048 | 11993.604 | 47740.000 | 71 |
| 4 | 12010.906 | 191826 | 11877.080 | 12010.906 | 47810.000 | 69 |
| 5 | 12002.999 | 191952 | 11753.254 | 12002.999 | 47800.000 | 67 |
| 6 | 12046.938 | 191252 | 11867.182 | 12046.938 | 47970.000 | 59 |
| 7 | 12058.697 | 191065 | 11862.155 | 12058.697 | 47980.000 | 65 |
| 8 | 12108.105 | 190286 | 11991.321 | 12108.105 | 48200.000 | 58 |
| 9 | 12079.942 | 190729 | 11820.203 | 12079.942 | 48170.000 | 63 |
| 10 | 12098.633 | 190435 | 11939.577 | 12098.633 | 48170.000 | 61 |
| 11 | 12112.666 | 190214 | 11939.543 | 12112.666 | 48240.000 | 58 |
| 12 | 12105.684 | 190324 | 11922.358 | 12105.684 | 48220.000 | 59 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2023984928 | 878.47 | 76593824 | 2100578752 |
| 2 | 2025301216 | 879.04 | 76598304 | 2101899520 |
| 3 | 2024374552 | 878.63 | 76597312 | 2100971864 |
| 4 | 2024717808 | 878.78 | 76304704 | 2101022512 |
| 5 | 2024881128 | 878.85 | 76303000 | 2101184128 |
