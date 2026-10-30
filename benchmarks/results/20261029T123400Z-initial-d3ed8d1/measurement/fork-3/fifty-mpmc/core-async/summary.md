# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9779.358 | 511281 | 9696.488 | 9779.358 | 38950.000 | 82 |
| 2 | 9777.230 | 511392 | 9650.377 | 9777.230 | 38950.000 | 78 |
| 3 | 9857.048 | 507251 | 9783.659 | 9857.048 | 39300.000 | 173 |
| 4 | 9852.241 | 507499 | 9695.956 | 9852.241 | 39270.000 | 167 |
| 5 | 9838.238 | 508221 | 9694.941 | 9838.238 | 39180.000 | 140 |
| 6 | 9929.913 | 503529 | 9646.822 | 9929.913 | 39520.000 | 167 |
| 7 | 9846.141 | 507813 | 9663.012 | 9846.141 | 39210.000 | 137 |
| 8 | 9841.356 | 508060 | 9710.842 | 9841.356 | 39240.000 | 153 |
| 9 | 9773.956 | 511564 | 9690.893 | 9773.946 | 38990.000 | 85 |
| 10 | 9774.520 | 511534 | 9678.064 | 9774.520 | 38960.000 | 75 |
| 11 | 9769.700 | 511786 | 9626.552 | 9769.700 | 38970.000 | 86 |
| 12 | 9998.788 | 500061 | 9948.412 | 9998.788 | 39040.000 | 81 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1629302232 | 325.86 | 148253744 | 1777555976 |
| 2 | 1628214408 | 325.64 | 148254064 | 1776468472 |
| 3 | 1630899480 | 326.18 | 148253936 | 1779153416 |
| 4 | 1629243344 | 325.85 | 148253808 | 1777497152 |
| 5 | 1631137616 | 326.23 | 148253904 | 1779391520 |
