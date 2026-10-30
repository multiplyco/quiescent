# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 141.084 | 21972777 | 71.961 | 141.084 | 550.000 | 0 |
| 2 | 155.816 | 19895285 | 93.200 | 155.816 | 600.000 | 21 |
| 3 | 141.097 | 21970695 | 71.584 | 141.097 | 550.000 | 0 |
| 4 | 137.475 | 22549621 | 71.624 | 137.475 | 550.000 | 0 |
| 5 | 140.746 | 22025493 | 71.985 | 140.746 | 540.000 | 0 |
| 6 | 134.507 | 23047044 | 71.429 | 134.507 | 530.000 | 0 |
| 7 | 138.164 | 22437141 | 71.408 | 138.164 | 550.000 | 0 |
| 8 | 138.024 | 22459865 | 71.762 | 138.024 | 540.000 | 0 |
| 9 | 142.136 | 21810170 | 79.636 | 142.136 | 570.000 | 0 |
| 10 | 139.202 | 22269740 | 72.839 | 139.202 | 550.000 | 0 |
| 11 | 135.031 | 22957727 | 71.831 | 135.031 | 540.000 | 0 |
| 12 | 139.715 | 22187977 | 71.696 | 139.715 | 560.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 313682552 | 101.19 | 91669520 | 405352072 |
| 2 | 313687736 | 101.19 | 91669488 | 405357224 |
| 3 | 313739440 | 101.21 | 91669488 | 405408928 |
| 4 | 313696184 | 101.19 | 91669456 | 405365640 |
| 5 | 313673768 | 101.19 | 91669544 | 405343312 |
