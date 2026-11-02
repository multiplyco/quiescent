# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6308.124 | 491430 | 5821.933 | 6308.124 | 25100.000 | 0 |
| 2 | 6354.950 | 487809 | 5823.748 | 6354.950 | 25290.000 | 42 |
| 3 | 6347.113 | 488411 | 5866.349 | 6347.113 | 25260.000 | 41 |
| 4 | 6334.866 | 489355 | 5839.533 | 6334.866 | 25200.000 | 34 |
| 5 | 6339.841 | 488971 | 5837.725 | 6339.841 | 25230.000 | 43 |
| 6 | 6323.660 | 490222 | 5812.194 | 6323.660 | 25080.000 | 0 |
| 7 | 6340.337 | 488933 | 5848.969 | 6340.337 | 25240.000 | 41 |
| 8 | 6348.615 | 488296 | 5866.664 | 6348.615 | 25250.000 | 40 |
| 9 | 6345.659 | 488523 | 5863.091 | 6345.659 | 25250.000 | 43 |
| 10 | 6308.531 | 491398 | 5818.946 | 6308.531 | 25090.000 | 0 |
| 11 | 6551.125 | 473201 | 6061.313 | 6551.125 | 26070.000 | 43 |
| 12 | 6348.531 | 488302 | 5867.310 | 6348.531 | 25250.000 | 49 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1100425264 | 354.98 | 91621040 | 1192046304 |
| 2 | 1100308160 | 354.94 | 91621040 | 1191929200 |
| 3 | 1100537480 | 355.01 | 91620976 | 1192158456 |
| 4 | 1099989128 | 354.84 | 91621040 | 1191610168 |
| 5 | 1100263760 | 354.92 | 91621040 | 1191884800 |
