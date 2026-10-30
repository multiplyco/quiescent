# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9372.271 | 533489 | 9334.941 | 9372.260 | 37360.000 | 60 |
| 2 | 9447.786 | 529225 | 9404.556 | 9447.786 | 37650.000 | 136 |
| 3 | 9432.176 | 530100 | 9384.446 | 9432.176 | 37600.000 | 124 |
| 4 | 9379.717 | 533065 | 9333.950 | 9379.717 | 37380.000 | 59 |
| 5 | 9418.685 | 530860 | 9382.297 | 9418.685 | 37550.000 | 66 |
| 6 | 10449.691 | 478483 | 10363.037 | 10449.691 | 37510.000 | 62 |
| 7 | 10083.929 | 495838 | 10009.553 | 10083.929 | 37500.000 | 59 |
| 8 | 9485.701 | 527109 | 9441.842 | 9485.701 | 37810.000 | 127 |
| 9 | 9408.262 | 531448 | 9365.418 | 9408.249 | 37500.000 | 58 |
| 10 | 9445.097 | 529375 | 9394.131 | 9445.097 | 37670.000 | 114 |
| 11 | 9671.225 | 516998 | 9619.890 | 9671.225 | 38540.000 | 114 |
| 12 | 9428.251 | 530321 | 9385.978 | 9428.251 | 37590.000 | 64 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1593488072 | 318.70 | 147403896 | 1740891968 |
| 2 | 1593661760 | 318.73 | 147403896 | 1741065656 |
| 3 | 1593442928 | 318.69 | 147403896 | 1740846824 |
| 4 | 1593658440 | 318.73 | 147403960 | 1741062400 |
| 5 | 1593551776 | 318.71 | 147403896 | 1740955672 |
