# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.181 | 88459 | 0.181 | 0.181 | 1.015 | 0 |
| 2 | 0.157 | 101749 | 0.157 | 0.157 | 6.522 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 4736 | 296.00 | 30128 | 34864 |
