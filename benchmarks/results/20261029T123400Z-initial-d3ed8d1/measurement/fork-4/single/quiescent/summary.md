# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 143.385 | 6974243 | 143.385 | 143.385 | 290.000 | 0 |
| 2 | 184.492 | 5420299 | 184.492 | 184.492 | 400.000 | 0 |
| 3 | 182.733 | 5472455 | 182.733 | 182.733 | 400.000 | 0 |
| 4 | 184.767 | 5412227 | 184.767 | 184.767 | 380.000 | 0 |
| 5 | 180.576 | 5537825 | 180.576 | 180.576 | 400.000 | 0 |
| 6 | 183.473 | 5450404 | 183.473 | 183.473 | 390.000 | 0 |
| 7 | 181.366 | 5513723 | 181.366 | 181.366 | 380.000 | 0 |
| 8 | 189.438 | 5278779 | 189.438 | 189.438 | 410.000 | 0 |
| 9 | 153.302 | 6523087 | 153.302 | 153.302 | 360.000 | 0 |
| 10 | 180.576 | 5537842 | 180.576 | 180.576 | 390.000 | 0 |
| 11 | 144.531 | 6918950 | 144.531 | 144.531 | 290.000 | 0 |
| 12 | 148.064 | 6753856 | 148.064 | 148.064 | 300.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 96997120 | 97.00 | 29446104 | 126443224 |
| 2 | 97939464 | 97.94 | 29446168 | 127385632 |
| 3 | 97802808 | 97.80 | 29446104 | 127248912 |
| 4 | 97959408 | 97.96 | 29446168 | 127405576 |
| 5 | 96006216 | 96.01 | 29446168 | 125452384 |
