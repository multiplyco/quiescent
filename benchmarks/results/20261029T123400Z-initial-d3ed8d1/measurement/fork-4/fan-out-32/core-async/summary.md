# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9159.251 | 251549 | 9061.730 | 9159.251 | 36450.000 | 42 |
| 2 | 9210.770 | 250142 | 9153.791 | 9210.770 | 36720.000 | 77 |
| 3 | 9219.635 | 249901 | 9152.483 | 9219.635 | 36770.000 | 83 |
| 4 | 9130.267 | 252347 | 9043.452 | 9130.267 | 36390.000 | 30 |
| 5 | 9130.760 | 252334 | 9040.132 | 9130.760 | 36390.000 | 37 |
| 6 | 9130.204 | 252349 | 9076.851 | 9130.204 | 36370.000 | 37 |
| 7 | 9167.863 | 251313 | 9074.728 | 9167.863 | 36490.000 | 79 |
| 8 | 9162.057 | 251472 | 9082.949 | 9162.057 | 36540.000 | 62 |
| 9 | 10067.388 | 228858 | 9841.252 | 10067.388 | 36420.000 | 69 |
| 10 | 9875.941 | 233294 | 9740.516 | 9875.941 | 36300.000 | 33 |
| 11 | 9123.387 | 252538 | 9027.479 | 9123.387 | 36310.000 | 32 |
| 12 | 9139.758 | 252085 | 9014.476 | 9139.758 | 36400.000 | 35 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1636654064 | 710.35 | 70025456 | 1706679520 |
| 2 | 1637594256 | 710.76 | 70026160 | 1707620416 |
| 3 | 1637654296 | 710.79 | 70026032 | 1707680328 |
| 4 | 1638483544 | 711.15 | 70026096 | 1708509640 |
| 5 | 1637992968 | 710.93 | 70025968 | 1708018936 |
