# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8955.110 | 257283 | 8785.258 | 8955.110 | 35650.000 | 32 |
| 2 | 8927.033 | 258092 | 8752.819 | 8927.033 | 35530.000 | 34 |
| 3 | 8996.730 | 256093 | 8943.546 | 8996.730 | 35840.000 | 72 |
| 4 | 8935.934 | 257835 | 8789.780 | 8935.934 | 35610.000 | 71 |
| 5 | 8885.529 | 259298 | 8748.321 | 8885.529 | 35390.000 | 33 |
| 6 | 8947.916 | 257490 | 8832.447 | 8947.916 | 35640.000 | 39 |
| 7 | 9043.653 | 254764 | 8894.151 | 9043.653 | 36040.000 | 66 |
| 8 | 8987.913 | 256344 | 8820.036 | 8987.913 | 35800.000 | 56 |
| 9 | 8910.088 | 258583 | 8789.081 | 8910.088 | 35530.000 | 33 |
| 10 | 8937.402 | 257793 | 8836.863 | 8937.402 | 35590.000 | 28 |
| 11 | 9007.155 | 255797 | 8831.548 | 9007.155 | 35860.000 | 59 |
| 12 | 8975.553 | 256697 | 8816.419 | 8975.553 | 35760.000 | 63 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1775107432 | 770.45 | 70051576 | 1845159008 |
| 2 | 1786452032 | 775.37 | 70053208 | 1856505240 |
| 3 | 1782065912 | 773.47 | 70052792 | 1852118704 |
| 4 | 1778883592 | 772.08 | 70052920 | 1848936512 |
| 5 | 1786118944 | 775.23 | 70053208 | 1856172152 |
