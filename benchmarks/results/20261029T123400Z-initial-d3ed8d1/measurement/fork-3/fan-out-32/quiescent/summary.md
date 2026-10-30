# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 194.921 | 11820172 | 111.873 | 194.921 | 770.000 | 0 |
| 2 | 252.434 | 9127133 | 137.545 | 252.434 | 750.000 | 0 |
| 3 | 192.825 | 11948635 | 107.304 | 192.825 | 770.000 | 0 |
| 4 | 197.264 | 11679756 | 109.594 | 197.264 | 790.000 | 0 |
| 5 | 203.249 | 11335835 | 109.436 | 203.249 | 810.000 | 0 |
| 6 | 238.062 | 9678160 | 148.073 | 238.062 | 810.000 | 0 |
| 7 | 189.219 | 12176347 | 108.910 | 189.219 | 760.000 | 0 |
| 8 | 188.433 | 12227162 | 108.649 | 188.433 | 750.000 | 0 |
| 9 | 198.070 | 11632269 | 108.715 | 198.070 | 790.000 | 0 |
| 10 | 229.521 | 10038281 | 143.500 | 229.521 | 840.000 | 15 |
| 11 | 196.636 | 11717096 | 108.394 | 196.636 | 790.000 | 0 |
| 12 | 192.750 | 11953309 | 108.541 | 192.750 | 770.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 251653640 | 109.22 | 70180144 | 321833784 |
| 2 | 251657976 | 109.23 | 70180176 | 321838152 |
| 3 | 251400928 | 109.11 | 70180144 | 321581072 |
| 4 | 251304152 | 109.07 | 70180144 | 321484296 |
| 5 | 251437464 | 109.13 | 70180144 | 321617608 |
