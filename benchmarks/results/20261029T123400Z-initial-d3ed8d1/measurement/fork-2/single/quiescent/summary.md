# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 154.162 | 6486686 | 154.162 | 154.162 | 310.000 | 0 |
| 2 | 168.487 | 5935177 | 168.487 | 168.487 | 370.000 | 0 |
| 3 | 184.632 | 5416167 | 184.632 | 184.632 | 390.000 | 0 |
| 4 | 166.098 | 6020537 | 166.098 | 166.098 | 330.000 | 0 |
| 5 | 174.510 | 5730340 | 174.510 | 174.510 | 370.000 | 0 |
| 6 | 187.679 | 5328259 | 187.679 | 187.679 | 440.000 | 0 |
| 7 | 183.689 | 5443979 | 183.689 | 183.689 | 400.000 | 0 |
| 8 | 203.576 | 4912174 | 203.576 | 203.576 | 440.000 | 0 |
| 9 | 176.566 | 5663593 | 176.566 | 176.566 | 380.000 | 0 |
| 10 | 189.960 | 5264271 | 189.960 | 189.960 | 440.000 | 0 |
| 11 | 179.302 | 5577183 | 179.302 | 179.302 | 370.000 | 0 |
| 12 | 172.992 | 5780603 | 172.992 | 172.992 | 340.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 96838696 | 96.84 | 29446072 | 126284768 |
| 2 | 97002920 | 97.00 | 29446072 | 126448992 |
| 3 | 97258800 | 97.26 | 29446008 | 126704808 |
| 4 | 96915248 | 96.92 | 29446072 | 126361320 |
| 5 | 96005504 | 96.01 | 29446008 | 125451512 |
