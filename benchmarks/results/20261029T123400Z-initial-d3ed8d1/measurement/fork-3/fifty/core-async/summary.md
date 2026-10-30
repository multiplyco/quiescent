# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9518.936 | 525269 | 9476.047 | 9518.936 | 37940.000 | 61 |
| 2 | 9493.755 | 526662 | 9446.582 | 9493.755 | 37850.000 | 66 |
| 3 | 9578.191 | 522019 | 9525.889 | 9578.191 | 38180.000 | 137 |
| 4 | 9562.143 | 522895 | 9530.650 | 9562.143 | 38150.000 | 138 |
| 5 | 9545.757 | 523793 | 9493.384 | 9545.744 | 38070.000 | 120 |
| 6 | 9536.274 | 524314 | 9484.480 | 9536.274 | 38000.000 | 110 |
| 7 | 9518.376 | 525300 | 9470.800 | 9518.376 | 37980.000 | 64 |
| 8 | 9477.871 | 527545 | 9433.253 | 9477.871 | 37800.000 | 60 |
| 9 | 9477.755 | 527551 | 9445.676 | 9477.755 | 37810.000 | 55 |
| 10 | 9526.049 | 524877 | 9477.598 | 9526.049 | 37950.000 | 124 |
| 11 | 9547.822 | 523680 | 9499.477 | 9547.822 | 38040.000 | 116 |
| 12 | 9656.261 | 517799 | 9612.950 | 9656.250 | 38490.000 | 123 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1593579280 | 318.72 | 147403896 | 1740983176 |
| 2 | 1593706424 | 318.74 | 147403896 | 1741110320 |
| 3 | 1594809240 | 318.96 | 147403832 | 1742213072 |
| 4 | 1594062496 | 318.81 | 147403960 | 1741466456 |
| 5 | 1593568936 | 318.71 | 147403896 | 1740972832 |
