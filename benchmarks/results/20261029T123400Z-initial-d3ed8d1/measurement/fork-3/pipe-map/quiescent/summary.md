# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 184.803 | 10822311 | 114.520 | 184.803 | 650.000 | 0 |
| 2 | 160.307 | 12476079 | 92.505 | 160.307 | 640.000 | 0 |
| 3 | 159.886 | 12508945 | 95.226 | 159.886 | 640.000 | 0 |
| 4 | 159.504 | 12538848 | 94.023 | 159.504 | 640.000 | 0 |
| 5 | 175.556 | 11392401 | 103.628 | 175.556 | 660.000 | 0 |
| 6 | 159.375 | 12549028 | 93.614 | 159.375 | 630.000 | 0 |
| 7 | 157.414 | 12705348 | 93.245 | 157.414 | 630.000 | 0 |
| 8 | 159.967 | 12502580 | 93.433 | 159.967 | 640.000 | 0 |
| 9 | 160.721 | 12443946 | 92.913 | 160.721 | 640.000 | 0 |
| 10 | 171.287 | 11676316 | 100.288 | 171.287 | 680.000 | 0 |
| 11 | 160.619 | 12451844 | 93.240 | 160.619 | 640.000 | 0 |
| 12 | 161.419 | 12390146 | 93.740 | 161.419 | 640.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 373943088 | 186.97 | 59470760 | 433413848 |
| 2 | 374022608 | 187.01 | 59470760 | 433493368 |
| 3 | 373915176 | 186.96 | 59470728 | 433385904 |
| 4 | 373940768 | 186.97 | 59470760 | 433411528 |
| 5 | 374009888 | 187.00 | 59470848 | 433480736 |
