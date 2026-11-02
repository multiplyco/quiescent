# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 89.383 | 25776607 | 50.740 | 89.383 | 350.000 | 0 |
| 2 | 100.732 | 22872500 | 54.154 | 100.732 | 390.000 | 0 |
| 3 | 118.417 | 19456712 | 64.855 | 118.417 | 390.000 | 0 |
| 4 | 89.651 | 25699552 | 51.146 | 89.651 | 360.000 | 0 |
| 5 | 89.517 | 25738093 | 51.278 | 89.517 | 350.000 | 0 |
| 6 | 115.643 | 19923410 | 61.902 | 115.643 | 380.000 | 0 |
| 7 | 104.841 | 21976101 | 58.408 | 104.841 | 410.000 | 15 |
| 8 | 103.111 | 22344957 | 58.495 | 103.111 | 370.000 | 0 |
| 9 | 124.229 | 18546391 | 63.324 | 124.229 | 390.000 | 0 |
| 10 | 119.202 | 19328513 | 63.724 | 119.202 | 430.000 | 9 |
| 11 | 87.756 | 26254494 | 51.526 | 87.755 | 350.000 | 0 |
| 12 | 92.214 | 24985457 | 51.235 | 92.214 | 360.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 227050160 | 98.55 | 76933856 | 303984016 |
| 2 | 227409736 | 98.70 | 76933824 | 304343560 |
| 3 | 228903016 | 99.35 | 76933856 | 305836872 |
| 4 | 227723632 | 98.84 | 76933856 | 304657488 |
| 5 | 227351032 | 98.68 | 76933856 | 304284888 |
