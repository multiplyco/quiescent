# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9057.961 | 254362 | 8934.717 | 9057.961 | 36110.000 | 42 |
| 2 | 9099.212 | 253209 | 8966.565 | 9099.212 | 36260.000 | 70 |
| 3 | 9156.873 | 251614 | 9094.188 | 9156.873 | 36500.000 | 90 |
| 4 | 9081.561 | 253701 | 9004.674 | 9081.561 | 36200.000 | 44 |
| 5 | 9168.103 | 251306 | 8963.362 | 9168.103 | 36320.000 | 32 |
| 6 | 9100.576 | 253171 | 9040.559 | 9100.576 | 36290.000 | 69 |
| 7 | 9123.732 | 252528 | 9052.409 | 9123.732 | 36390.000 | 54 |
| 8 | 9168.558 | 251294 | 9032.463 | 9168.558 | 36440.000 | 62 |
| 9 | 9151.928 | 251750 | 9042.347 | 9151.928 | 36420.000 | 71 |
| 10 | 9135.319 | 252208 | 8919.776 | 9135.319 | 36390.000 | 69 |
| 11 | 9154.216 | 251687 | 9060.494 | 9154.216 | 36470.000 | 72 |
| 12 | 9137.673 | 252143 | 9033.792 | 9137.665 | 36410.000 | 68 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1638374784 | 711.10 | 70025456 | 1708400240 |
| 2 | 1637867520 | 710.88 | 70026160 | 1707893680 |
| 3 | 1638049152 | 710.96 | 70026000 | 1708075152 |
| 4 | 1639160424 | 711.44 | 70026000 | 1709186424 |
| 5 | 1639086208 | 711.41 | 70026224 | 1709112432 |
