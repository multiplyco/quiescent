# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 143.511 | 21601121 | 72.224 | 143.511 | 560.000 | 0 |
| 2 | 152.841 | 20282451 | 85.262 | 152.841 | 550.000 | 0 |
| 3 | 157.536 | 19678001 | 84.660 | 157.536 | 590.000 | 0 |
| 4 | 145.592 | 21292322 | 76.454 | 145.592 | 570.000 | 0 |
| 5 | 165.604 | 18719339 | 93.870 | 165.604 | 640.000 | 15 |
| 6 | 142.399 | 21769863 | 73.139 | 142.399 | 560.000 | 0 |
| 7 | 143.378 | 21621211 | 72.716 | 143.378 | 560.000 | 0 |
| 8 | 139.055 | 22293296 | 73.079 | 139.055 | 560.000 | 0 |
| 9 | 143.544 | 21596100 | 73.104 | 143.544 | 570.000 | 0 |
| 10 | 142.076 | 21819348 | 75.081 | 142.076 | 560.000 | 0 |
| 11 | 143.707 | 21571672 | 75.010 | 143.707 | 560.000 | 0 |
| 12 | 149.151 | 20784326 | 77.672 | 149.151 | 600.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 313672256 | 101.18 | 91661616 | 405333872 |
| 2 | 313759336 | 101.21 | 91661616 | 405420952 |
| 3 | 313661120 | 101.18 | 91661616 | 405322736 |
| 4 | 313655264 | 101.18 | 91661648 | 405316912 |
| 5 | 313730984 | 101.20 | 91661680 | 405392664 |
