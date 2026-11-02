# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2953.457 | 3385863 | 1391.925 | 2953.457 | 11710.000 | 269 |
| 2 | 2725.238 | 3669404 | 1335.185 | 2725.238 | 10810.000 | 293 |
| 3 | 2892.605 | 3457091 | 1360.366 | 2892.605 | 11400.000 | 423 |
| 4 | 2984.381 | 3350779 | 1409.541 | 2984.381 | 11810.000 | 340 |
| 5 | 2801.389 | 3569657 | 1426.954 | 2801.389 | 11090.000 | 196 |
| 6 | 2946.409 | 3393962 | 1524.770 | 2946.409 | 11690.000 | 202 |
| 7 | 2826.940 | 3537394 | 1490.604 | 2826.940 | 11220.000 | 100 |
| 8 | 2704.134 | 3698042 | 1350.923 | 2704.134 | 10720.000 | 51 |
| 9 | 2578.804 | 3877767 | 1307.133 | 2578.804 | 10230.000 | 42 |
| 10 | 2671.077 | 3743809 | 1326.841 | 2671.076 | 10550.000 | 52 |
| 11 | 2576.253 | 3881607 | 1313.293 | 2576.253 | 10200.000 | 51 |
| 12 | 2567.700 | 3894537 | 1366.098 | 2567.700 | 10190.000 | 57 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1440337104 | 144.03 | 295597480 | 1735934584 |
| 2 | 1440325160 | 144.03 | 295597480 | 1735922640 |
| 3 | 1440374392 | 144.04 | 295597536 | 1735971928 |
| 4 | 1440299160 | 144.03 | 295597480 | 1735896640 |
| 5 | 1440323776 | 144.03 | 295568712 | 1735892488 |
