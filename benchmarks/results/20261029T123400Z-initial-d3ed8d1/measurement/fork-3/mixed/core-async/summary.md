# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6472.857 | 478923 | 5979.427 | 6472.857 | 25750.000 | 0 |
| 2 | 6305.200 | 491658 | 5761.207 | 6305.200 | 25030.000 | 41 |
| 3 | 6298.979 | 492143 | 5806.146 | 6298.979 | 25040.000 | 40 |
| 4 | 6294.961 | 492457 | 5795.780 | 6294.961 | 25040.000 | 42 |
| 5 | 6305.941 | 491600 | 5812.484 | 6305.941 | 25070.000 | 51 |
| 6 | 6247.843 | 496171 | 5743.030 | 6247.843 | 24860.000 | 0 |
| 7 | 7223.344 | 429164 | 6433.921 | 7223.344 | 25030.000 | 49 |
| 8 | 6510.719 | 476138 | 6018.423 | 6510.719 | 24990.000 | 44 |
| 9 | 6888.408 | 450031 | 6382.911 | 6888.408 | 25010.000 | 43 |
| 10 | 6293.713 | 492555 | 5759.341 | 6293.713 | 24990.000 | 36 |
| 11 | 6281.248 | 493532 | 5794.964 | 6281.248 | 24990.000 | 39 |
| 12 | 6273.686 | 494127 | 5780.744 | 6273.686 | 25000.000 | 39 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1100927664 | 355.14 | 91621040 | 1192548704 |
| 2 | 1100860472 | 355.12 | 91621040 | 1192481512 |
| 3 | 1100885832 | 355.12 | 91621104 | 1192506936 |
| 4 | 1100827616 | 355.11 | 91621072 | 1192448688 |
| 5 | 1100604528 | 355.03 | 91621104 | 1192225632 |
