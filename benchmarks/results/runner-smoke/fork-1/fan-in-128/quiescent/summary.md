# Channel workload trials

Status: `complete`. Case: `fan-in-128`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 25.682 | 1913894 | 15.940 | 25.682 | 207.048 | 0 |
| 2 | 16.148 | 3043876 | 9.337 | 16.148 | 132.547 | 0 |
