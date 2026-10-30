# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6245.823 | 496332 | 5751.160 | 6245.808 | 24790.000 | 0 |
| 2 | 6378.066 | 486041 | 5884.294 | 6378.066 | 25370.000 | 45 |
| 3 | 6275.810 | 493960 | 5760.909 | 6275.810 | 24930.000 | 39 |
| 4 | 6273.860 | 494114 | 5787.144 | 6273.860 | 24970.000 | 39 |
| 5 | 6271.907 | 494268 | 5770.793 | 6271.907 | 24970.000 | 47 |
| 6 | 6278.598 | 493741 | 5743.967 | 6278.585 | 24940.000 | 41 |
| 7 | 6270.692 | 494363 | 5776.844 | 6270.692 | 24950.000 | 37 |
| 8 | 6288.667 | 492950 | 5794.493 | 6288.667 | 24980.000 | 42 |
| 9 | 6279.578 | 493664 | 5794.382 | 6279.578 | 25000.000 | 52 |
| 10 | 6270.427 | 494384 | 5776.891 | 6270.427 | 24950.000 | 40 |
| 11 | 6274.737 | 494045 | 5774.462 | 6274.737 | 24940.000 | 37 |
| 12 | 6276.525 | 493904 | 5784.369 | 6276.525 | 24970.000 | 51 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1101136264 | 355.21 | 91621040 | 1192757304 |
| 2 | 1100738856 | 355.08 | 91621104 | 1192359960 |
| 3 | 1100737056 | 355.08 | 91621104 | 1192358160 |
| 4 | 1100552968 | 355.02 | 91621168 | 1192174136 |
| 5 | 1101128408 | 355.20 | 91621168 | 1192749576 |
