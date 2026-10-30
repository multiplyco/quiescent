# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5552.979 | 360167 | 5514.135 | 5552.966 | 22120.000 | 78 |
| 2 | 5719.727 | 349667 | 5676.563 | 5719.716 | 21960.000 | 44 |
| 3 | 6213.627 | 321873 | 6185.841 | 6213.617 | 21920.000 | 37 |
| 4 | 5863.589 | 341088 | 5806.701 | 5863.589 | 21850.000 | 44 |
| 5 | 5812.370 | 344094 | 5783.378 | 5812.370 | 21950.000 | 40 |
| 6 | 5474.514 | 365329 | 5441.426 | 5474.504 | 21790.000 | 31 |
| 7 | 5459.832 | 366312 | 5437.607 | 5459.832 | 21760.000 | 30 |
| 8 | 5478.877 | 365038 | 5451.050 | 5478.877 | 21820.000 | 33 |
| 9 | 5474.219 | 365349 | 5416.454 | 5474.219 | 21800.000 | 29 |
| 10 | 5472.976 | 365432 | 5446.537 | 5472.976 | 21780.000 | 31 |
| 11 | 5452.983 | 366772 | 5426.404 | 5452.983 | 21740.000 | 30 |
| 12 | 5451.179 | 366893 | 5412.453 | 5451.179 | 21710.000 | 24 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1212980504 | 606.49 | 59385608 | 1272366112 |
| 2 | 1212898984 | 606.45 | 59385640 | 1272284624 |
| 3 | 1213261984 | 606.63 | 59385672 | 1272647656 |
| 4 | 1212636376 | 606.32 | 59385832 | 1272022208 |
| 5 | 1212367600 | 606.18 | 59385736 | 1271753336 |
