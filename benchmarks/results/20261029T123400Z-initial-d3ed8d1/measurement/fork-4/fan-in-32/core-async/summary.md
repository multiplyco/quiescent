# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8963.429 | 257044 | 8818.883 | 8963.429 | 35710.000 | 54 |
| 2 | 8947.117 | 257513 | 8797.855 | 8947.117 | 35620.000 | 37 |
| 3 | 9008.749 | 255751 | 8910.503 | 9008.749 | 35870.000 | 59 |
| 4 | 9034.430 | 255024 | 8907.979 | 9034.430 | 35970.000 | 69 |
| 5 | 9017.849 | 255493 | 8922.525 | 9017.849 | 35920.000 | 41 |
| 6 | 8958.069 | 257198 | 8812.948 | 8958.069 | 35680.000 | 35 |
| 7 | 9008.295 | 255764 | 8864.934 | 9008.295 | 35870.000 | 63 |
| 8 | 9051.492 | 254544 | 8898.266 | 9051.492 | 36010.000 | 62 |
| 9 | 8999.229 | 256022 | 8887.756 | 8999.229 | 35840.000 | 33 |
| 10 | 8950.405 | 257419 | 8833.341 | 8950.405 | 35680.000 | 29 |
| 11 | 8995.708 | 256122 | 8838.149 | 8995.708 | 35750.000 | 33 |
| 12 | 9027.773 | 255212 | 8857.297 | 9027.773 | 35890.000 | 55 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1784524600 | 774.53 | 70076984 | 1854601584 |
| 2 | 1778466944 | 771.90 | 70078200 | 1848545144 |
| 3 | 1788178472 | 776.12 | 70078168 | 1858256640 |
| 4 | 1785395856 | 774.91 | 70078968 | 1855474824 |
| 5 | 1785804152 | 775.09 | 70078936 | 1855883088 |
