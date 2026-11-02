# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 64.695 | 35613423 | 23.638 | 64.695 | 526.738 | 0 |
| 2 | 59.074 | 39001930 | 23.023 | 59.074 | 478.921 | 0 |
| 3 | 68.074 | 33845624 | 19.481 | 68.074 | 550.155 | 6 |
| 4 | 59.933 | 38442795 | 19.669 | 59.933 | 513.248 | 0 |
| 5 | 58.706 | 39246498 | 20.413 | 58.706 | 487.090 | 0 |
| 6 | 65.567 | 35139852 | 23.309 | 65.567 | 477.420 | 0 |
| 7 | 74.073 | 31104521 | 31.574 | 74.068 | 628.593 | 0 |
| 8 | 74.414 | 30962124 | 20.632 | 74.414 | 551.937 | 6 |
| 9 | 54.569 | 42221646 | 25.818 | 54.569 | 438.536 | 0 |
| 10 | 72.330 | 31854204 | 24.010 | 72.330 | 602.247 | 0 |
| 11 | 81.878 | 28139584 | 23.996 | 81.877 | 640.381 | 0 |
| 12 | 61.227 | 37630586 | 23.474 | 61.227 | 499.295 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 231061896 | 100.29 | 76976760 | 308038656 |
| 2 | 231317016 | 100.40 | 76976536 | 308293552 |
| 3 | 230888232 | 100.21 | 76976728 | 307864960 |
| 4 | 231470040 | 100.46 | 76976696 | 308446736 |
| 5 | 233585928 | 101.38 | 76976600 | 310562528 |
