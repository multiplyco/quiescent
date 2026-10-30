# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 179.208 | 5580112 | 179.208 | 179.208 | 390.000 | 0 |
| 2 | 174.886 | 5718002 | 174.886 | 174.886 | 370.000 | 0 |
| 3 | 173.071 | 5777983 | 173.071 | 173.071 | 350.000 | 0 |
| 4 | 188.227 | 5312741 | 188.227 | 188.227 | 400.000 | 0 |
| 5 | 183.174 | 5459293 | 183.174 | 183.174 | 410.000 | 0 |
| 6 | 178.594 | 5599296 | 178.594 | 178.594 | 390.000 | 0 |
| 7 | 175.019 | 5713674 | 175.019 | 175.019 | 380.000 | 12 |
| 8 | 181.406 | 5512508 | 181.406 | 181.406 | 390.000 | 0 |
| 9 | 175.527 | 5697119 | 175.527 | 175.527 | 420.000 | 0 |
| 10 | 181.730 | 5502657 | 181.730 | 181.730 | 420.000 | 0 |
| 11 | 178.505 | 5602085 | 178.505 | 178.505 | 360.000 | 0 |
| 12 | 188.894 | 5293974 | 188.894 | 188.894 | 430.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 96832048 | 96.83 | 29446128 | 126278176 |
| 2 | 96964976 | 96.96 | 29446072 | 126411048 |
| 3 | 96024248 | 96.02 | 29446072 | 125470320 |
| 4 | 96909632 | 96.91 | 29446072 | 126355704 |
| 5 | 97369544 | 97.37 | 29446072 | 126815616 |
