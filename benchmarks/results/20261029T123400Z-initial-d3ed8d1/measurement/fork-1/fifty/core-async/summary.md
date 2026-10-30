# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9696.778 | 515635 | 9640.781 | 9696.778 | 38520.000 | 68 |
| 2 | 9671.710 | 516972 | 9623.052 | 9671.710 | 38550.000 | 131 |
| 3 | 9640.587 | 518641 | 9608.815 | 9640.587 | 38430.000 | 62 |
| 4 | 9647.564 | 518266 | 9591.926 | 9647.564 | 38450.000 | 61 |
| 5 | 9700.462 | 515439 | 9668.001 | 9700.462 | 38680.000 | 131 |
| 6 | 9704.615 | 515219 | 9654.463 | 9704.615 | 38670.000 | 136 |
| 7 | 9690.419 | 515974 | 9647.739 | 9690.408 | 38620.000 | 106 |
| 8 | 9742.488 | 513216 | 9691.330 | 9742.488 | 38830.000 | 112 |
| 9 | 9668.565 | 517140 | 9613.720 | 9668.565 | 38510.000 | 99 |
| 10 | 9614.090 | 520070 | 9576.161 | 9614.090 | 38340.000 | 55 |
| 11 | 9634.493 | 518969 | 9575.048 | 9634.493 | 38380.000 | 66 |
| 12 | 9634.402 | 518974 | 9594.730 | 9634.402 | 38410.000 | 62 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1593503792 | 318.70 | 147403896 | 1740907688 |
| 2 | 1604565024 | 320.91 | 147403928 | 1751968952 |
| 3 | 1593349000 | 318.67 | 147403928 | 1740752928 |
| 4 | 1593315096 | 318.66 | 147403896 | 1740718992 |
| 5 | 1593422784 | 318.68 | 147403896 | 1740826680 |
