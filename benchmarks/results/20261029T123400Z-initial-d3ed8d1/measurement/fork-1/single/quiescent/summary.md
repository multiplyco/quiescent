# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 182.534 | 5478429 | 182.534 | 182.534 | 420.000 | 0 |
| 2 | 186.106 | 5373283 | 186.106 | 186.106 | 420.000 | 0 |
| 3 | 144.329 | 6928604 | 144.329 | 144.329 | 290.000 | 0 |
| 4 | 188.582 | 5302730 | 188.582 | 188.582 | 400.000 | 0 |
| 5 | 187.636 | 5329461 | 187.636 | 187.636 | 420.000 | 0 |
| 6 | 142.111 | 7036752 | 142.111 | 142.111 | 290.000 | 0 |
| 7 | 184.548 | 5418655 | 184.548 | 184.548 | 410.000 | 0 |
| 8 | 205.275 | 4871504 | 205.275 | 205.275 | 460.000 | 0 |
| 9 | 185.922 | 5378589 | 185.922 | 185.922 | 410.000 | 0 |
| 10 | 176.616 | 5662007 | 176.616 | 176.616 | 420.000 | 0 |
| 11 | 183.046 | 5463094 | 183.046 | 183.046 | 390.000 | 0 |
| 12 | 169.026 | 5916247 | 169.026 | 169.026 | 360.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 96863136 | 96.86 | 29446104 | 126309240 |
| 2 | 97006480 | 97.01 | 29446104 | 126452584 |
| 3 | 98097000 | 98.10 | 29446168 | 127543168 |
| 4 | 97718880 | 97.72 | 29446168 | 127165048 |
| 5 | 97980984 | 97.98 | 29446104 | 127427088 |
