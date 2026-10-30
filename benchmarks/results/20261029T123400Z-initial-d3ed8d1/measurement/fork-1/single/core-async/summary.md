# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4321.771 | 231387 | 4321.771 | 4321.771 | 8240.000 | 28 |
| 2 | 4258.466 | 234826 | 4258.466 | 4258.466 | 8070.000 | 0 |
| 3 | 4378.131 | 228408 | 4378.131 | 4378.131 | 8380.000 | 31 |
| 4 | 4286.265 | 233303 | 4286.265 | 4286.265 | 8100.000 | 0 |
| 5 | 4270.303 | 234175 | 4270.303 | 4270.303 | 8120.000 | 0 |
| 6 | 4452.084 | 224614 | 4452.084 | 4452.084 | 8440.000 | 0 |
| 7 | 4302.316 | 232433 | 4302.316 | 4302.316 | 8100.000 | 0 |
| 8 | 4307.811 | 232136 | 4307.811 | 4307.811 | 8190.000 | 0 |
| 9 | 4187.050 | 238832 | 4187.049 | 4187.049 | 8040.000 | 0 |
| 10 | 4248.632 | 235370 | 4248.632 | 4248.632 | 8090.000 | 0 |
| 11 | 4259.920 | 234746 | 4259.920 | 4259.920 | 8120.000 | 31 |
| 12 | 4251.656 | 235202 | 4251.656 | 4251.656 | 8030.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 446617656 | 446.62 | 29437368 | 476055024 |
| 2 | 435973584 | 435.97 | 29437368 | 465410952 |
| 3 | 442331568 | 442.33 | 29437368 | 471768936 |
| 4 | 439832120 | 439.83 | 29437368 | 469269488 |
| 5 | 444973048 | 444.97 | 29437368 | 474410416 |
