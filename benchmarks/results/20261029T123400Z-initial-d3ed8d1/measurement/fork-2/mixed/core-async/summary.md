# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6374.980 | 486276 | 5883.266 | 6374.980 | 25350.000 | 0 |
| 2 | 6407.539 | 483805 | 5918.855 | 6407.539 | 25500.000 | 37 |
| 3 | 6420.769 | 482808 | 5917.614 | 6420.769 | 25540.000 | 40 |
| 4 | 6412.705 | 483415 | 5912.834 | 6412.705 | 25530.000 | 41 |
| 5 | 6428.063 | 482260 | 5932.068 | 6428.063 | 25600.000 | 50 |
| 6 | 6379.997 | 485894 | 5889.899 | 6379.997 | 25380.000 | 0 |
| 7 | 6407.156 | 483834 | 5909.426 | 6407.156 | 25510.000 | 39 |
| 8 | 6415.353 | 483216 | 5915.896 | 6415.353 | 25500.000 | 37 |
| 9 | 6459.688 | 479899 | 5946.998 | 6459.688 | 25700.000 | 43 |
| 10 | 6452.761 | 480414 | 5896.972 | 6452.761 | 25660.000 | 40 |
| 11 | 6473.649 | 478864 | 5948.445 | 6473.649 | 25670.000 | 55 |
| 12 | 6462.502 | 479690 | 5943.418 | 6462.502 | 25720.000 | 49 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1100887000 | 355.12 | 91621040 | 1192508040 |
| 2 | 1100074984 | 354.86 | 91621040 | 1191696024 |
| 3 | 1100187752 | 354.90 | 91621040 | 1191808792 |
| 4 | 1100296632 | 354.93 | 91621136 | 1191917768 |
| 5 | 1100716472 | 355.07 | 91621104 | 1192337576 |
