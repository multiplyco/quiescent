# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4251.720 | 235199 | 4251.720 | 4251.720 | 8150.000 | 27 |
| 2 | 4276.566 | 233832 | 4276.566 | 4276.566 | 8140.000 | 0 |
| 3 | 4321.650 | 231393 | 4321.650 | 4321.650 | 8270.000 | 29 |
| 4 | 4216.875 | 237142 | 4216.875 | 4216.875 | 8000.000 | 0 |
| 5 | 4289.789 | 233112 | 4289.789 | 4289.789 | 8190.000 | 23 |
| 6 | 4293.967 | 232885 | 4293.967 | 4293.967 | 8140.000 | 0 |
| 7 | 4309.222 | 232060 | 4309.221 | 4309.221 | 8230.000 | 22 |
| 8 | 4271.108 | 234131 | 4271.108 | 4271.108 | 8110.000 | 0 |
| 9 | 4317.268 | 231628 | 4317.268 | 4317.268 | 8250.000 | 24 |
| 10 | 4246.080 | 235511 | 4246.080 | 4246.080 | 8090.000 | 0 |
| 11 | 4338.762 | 230480 | 4338.762 | 4338.762 | 8260.000 | 23 |
| 12 | 4279.553 | 233669 | 4279.553 | 4279.553 | 8150.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 434851224 | 434.85 | 29437320 | 464288544 |
| 2 | 435292008 | 435.29 | 29437320 | 464729328 |
| 3 | 428863600 | 428.86 | 29437320 | 458300920 |
| 4 | 437456360 | 437.46 | 29437320 | 466893680 |
| 5 | 428071008 | 428.07 | 29437320 | 457508328 |
