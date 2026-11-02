# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9091.118 | 253434 | 8985.264 | 9091.118 | 36220.000 | 65 |
| 2 | 9107.226 | 252986 | 8943.075 | 9107.226 | 36250.000 | 29 |
| 3 | 9157.739 | 251590 | 9025.816 | 9157.739 | 36490.000 | 62 |
| 4 | 9114.005 | 252798 | 9005.178 | 9114.005 | 36320.000 | 54 |
| 5 | 9130.403 | 252344 | 8971.904 | 9130.403 | 36340.000 | 33 |
| 6 | 9039.847 | 254872 | 8842.903 | 9039.847 | 36010.000 | 31 |
| 7 | 9130.952 | 252329 | 9002.416 | 9130.952 | 36400.000 | 65 |
| 8 | 9131.591 | 252311 | 9008.644 | 9131.591 | 36350.000 | 55 |
| 9 | 9089.602 | 253476 | 8952.112 | 9089.602 | 36210.000 | 29 |
| 10 | 9038.973 | 254896 | 8867.981 | 9038.973 | 35980.000 | 38 |
| 11 | 9100.630 | 253169 | 8960.250 | 9100.630 | 36240.000 | 36 |
| 12 | 9097.718 | 253250 | 8959.704 | 9097.718 | 36200.000 | 65 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1785642928 | 775.02 | 70051640 | 1855694568 |
| 2 | 1781853560 | 773.37 | 70053816 | 1851907376 |
| 3 | 1786518488 | 775.40 | 70052696 | 1856571184 |
| 4 | 1725269384 | 748.81 | 70052952 | 1795322336 |
| 5 | 1736039784 | 753.49 | 70052088 | 1806091872 |
