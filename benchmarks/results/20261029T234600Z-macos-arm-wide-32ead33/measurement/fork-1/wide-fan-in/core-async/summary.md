# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 33627.448 | 68515 | 33395.061 | 33627.448 | 279695.614 | 28 |
| 2 | 35729.436 | 64485 | 35525.409 | 35729.436 | 298664.606 | 26 |
| 3 | 36336.734 | 63407 | 36062.622 | 36336.734 | 302719.390 | 31 |
| 4 | 37183.593 | 61963 | 36942.325 | 37183.593 | 309437.650 | 28 |
| 5 | 37295.370 | 61777 | 37157.461 | 37295.370 | 321321.065 | 35 |
| 6 | 38313.873 | 60135 | 38067.556 | 38313.873 | 317680.845 | 27 |
| 7 | 38941.277 | 59166 | 38767.196 | 38941.277 | 328687.200 | 25 |
| 8 | 39072.543 | 58967 | 38918.916 | 39072.543 | 336190.867 | 25 |
| 9 | 40701.797 | 56607 | 40543.388 | 40701.797 | 346466.543 | 38 |
| 10 | 40871.072 | 56372 | 40719.287 | 40871.072 | 345978.920 | 23 |
| 11 | 41170.753 | 55962 | 40971.737 | 41170.753 | 354990.556 | 23 |
| 12 | 41994.389 | 54864 | 41872.554 | 41994.389 | 353550.688 | 19 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2012640000 | 873.54 | 76403848 | 2089043848 |
| 2 | 2010675104 | 872.69 | 76407016 | 2087082120 |
| 3 | 2012170184 | 873.34 | 76409832 | 2088580016 |
| 4 | 2013697752 | 874.00 | 76407720 | 2090105472 |
| 5 | 2015149888 | 874.63 | 76408680 | 2091558568 |
