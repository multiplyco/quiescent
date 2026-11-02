# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7140.578 | 700223 | 7107.263 | 7140.578 | 28480.000 | 35 |
| 2 | 7153.351 | 698973 | 7119.744 | 7153.351 | 28490.000 | 42 |
| 3 | 7150.857 | 699217 | 7114.674 | 7150.857 | 28500.000 | 40 |
| 4 | 7150.563 | 699246 | 7114.355 | 7150.563 | 28490.000 | 35 |
| 5 | 7175.517 | 696814 | 7143.391 | 7175.517 | 28570.000 | 70 |
| 6 | 7147.773 | 699519 | 7105.056 | 7147.773 | 28490.000 | 38 |
| 7 | 7184.213 | 695970 | 7136.650 | 7184.213 | 28630.000 | 72 |
| 8 | 7149.267 | 699372 | 7113.741 | 7149.267 | 28460.000 | 42 |
| 9 | 7230.294 | 691535 | 7191.551 | 7230.294 | 28820.000 | 37 |
| 10 | 7168.454 | 697501 | 7137.164 | 7168.454 | 28580.000 | 60 |
| 11 | 7155.785 | 698735 | 7122.546 | 7155.785 | 28510.000 | 38 |
| 12 | 7172.064 | 697149 | 7137.657 | 7172.064 | 28590.000 | 40 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1493933904 | 298.79 | 73858656 | 1567792560 |
| 2 | 1493906048 | 298.78 | 73858656 | 1567764704 |
| 3 | 1493819280 | 298.76 | 73858720 | 1567678000 |
| 4 | 1493922904 | 298.78 | 73858720 | 1567781624 |
| 5 | 1494035192 | 298.81 | 73858656 | 1567893848 |
