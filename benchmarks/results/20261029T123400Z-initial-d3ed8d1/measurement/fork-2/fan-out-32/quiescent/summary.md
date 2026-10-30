# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 203.265 | 11334973 | 115.510 | 203.265 | 790.000 | 0 |
| 2 | 192.703 | 11956226 | 109.186 | 192.703 | 770.000 | 0 |
| 3 | 193.835 | 11886419 | 111.405 | 193.835 | 770.000 | 0 |
| 4 | 199.505 | 11548591 | 110.905 | 199.505 | 790.000 | 0 |
| 5 | 198.607 | 11600805 | 111.380 | 198.607 | 800.000 | 0 |
| 6 | 201.163 | 11453403 | 109.185 | 201.163 | 810.000 | 0 |
| 7 | 224.054 | 10283230 | 131.351 | 224.054 | 790.000 | 0 |
| 8 | 227.837 | 10112510 | 138.140 | 227.837 | 790.000 | 0 |
| 9 | 211.979 | 10868984 | 119.679 | 211.979 | 790.000 | 0 |
| 10 | 242.431 | 9503751 | 156.300 | 242.431 | 860.000 | 15 |
| 11 | 204.786 | 11250764 | 110.137 | 204.786 | 810.000 | 0 |
| 12 | 220.892 | 10430456 | 136.116 | 220.892 | 750.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 251650672 | 109.22 | 70205488 | 321856160 |
| 2 | 250974288 | 108.93 | 70205520 | 321179808 |
| 3 | 251327408 | 109.08 | 70205552 | 321532960 |
| 4 | 252001808 | 109.38 | 70205488 | 322207296 |
| 5 | 251379320 | 109.11 | 70205520 | 321584840 |
