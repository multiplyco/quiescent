# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8946.504 | 257531 | 8831.219 | 8946.504 | 35630.000 | 36 |
| 2 | 8930.023 | 258006 | 8800.378 | 8930.023 | 35510.000 | 32 |
| 3 | 8979.007 | 256599 | 8834.791 | 8979.007 | 35780.000 | 66 |
| 4 | 8983.982 | 256456 | 8808.887 | 8983.982 | 35710.000 | 69 |
| 5 | 9711.996 | 237232 | 9509.844 | 9711.996 | 34870.000 | 39 |
| 6 | 9520.730 | 241998 | 9332.344 | 9520.730 | 34650.000 | 32 |
| 7 | 8957.235 | 257222 | 8790.684 | 8957.235 | 35640.000 | 60 |
| 8 | 8950.098 | 257427 | 8825.732 | 8950.098 | 35640.000 | 32 |
| 9 | 8940.289 | 257710 | 8786.644 | 8940.289 | 35570.000 | 37 |
| 10 | 8894.685 | 259031 | 8788.772 | 8894.685 | 35440.000 | 32 |
| 11 | 8899.631 | 258887 | 8772.254 | 8899.631 | 35410.000 | 59 |
| 12 | 8898.623 | 258916 | 8814.465 | 8898.623 | 35450.000 | 61 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1776285216 | 770.96 | 70051608 | 1846336824 |
| 2 | 1780350360 | 772.72 | 70053304 | 1850403664 |
| 3 | 1785221584 | 774.84 | 70052856 | 1855274440 |
| 4 | 1788251752 | 776.15 | 70052920 | 1858304672 |
| 5 | 1773311920 | 769.67 | 70052856 | 1843364776 |
