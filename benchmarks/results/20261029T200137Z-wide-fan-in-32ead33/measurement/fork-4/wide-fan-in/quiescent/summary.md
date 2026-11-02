# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 103.039 | 22360413 | 58.907 | 103.039 | 340.000 | 0 |
| 2 | 92.349 | 24948729 | 56.510 | 92.349 | 320.000 | 0 |
| 3 | 111.213 | 20716943 | 64.435 | 111.213 | 430.000 | 0 |
| 4 | 80.324 | 28683699 | 45.516 | 80.324 | 320.000 | 0 |
| 5 | 83.823 | 27486470 | 46.635 | 83.823 | 330.000 | 0 |
| 6 | 139.200 | 16551768 | 92.212 | 139.200 | 490.000 | 36 |
| 7 | 96.778 | 23807100 | 50.777 | 96.778 | 370.000 | 0 |
| 8 | 80.084 | 28769787 | 46.562 | 80.084 | 320.000 | 0 |
| 9 | 79.579 | 28952311 | 44.717 | 79.579 | 320.000 | 0 |
| 10 | 101.846 | 22622414 | 52.210 | 101.846 | 410.000 | 0 |
| 11 | 89.303 | 25799698 | 47.293 | 89.303 | 350.000 | 0 |
| 12 | 118.552 | 19434577 | 57.290 | 118.552 | 360.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 227417264 | 98.71 | 76983360 | 304400624 |
| 2 | 226992808 | 98.52 | 76983360 | 303976168 |
| 3 | 227024344 | 98.53 | 76983360 | 304007704 |
| 4 | 227458672 | 98.72 | 76983392 | 304442064 |
| 5 | 227289776 | 98.65 | 76983360 | 304273136 |
