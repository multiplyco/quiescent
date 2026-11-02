# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.136 | 118009 | 0.136 | 0.136 | 0.291 | 0 |
| 2 | 0.189 | 84731 | 0.189 | 0.189 | 0.681 | 0 |
