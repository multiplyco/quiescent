# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 33715.069 | 296603 | 33618.937 | 33715.069 | 134370.000 | 281 |
| 2 | 32091.599 | 311608 | 32022.238 | 32091.599 | 127870.000 | 225 |
| 3 | 33860.984 | 295325 | 33778.176 | 33860.974 | 134950.000 | 285 |
| 4 | 33129.653 | 301844 | 33045.117 | 33129.642 | 132040.000 | 240 |
| 5 | 33888.157 | 295088 | 33815.230 | 33888.147 | 135070.000 | 212 |
| 6 | 34061.213 | 293589 | 33980.861 | 34061.203 | 135710.000 | 204 |
| 7 | 34091.372 | 293329 | 33998.600 | 34091.361 | 135910.000 | 249 |
| 8 | 33968.026 | 294394 | 33891.797 | 33968.017 | 135370.000 | 251 |
| 9 | 33215.302 | 301066 | 33148.537 | 33215.293 | 132340.000 | 199 |
| 10 | 33449.630 | 298957 | 33365.657 | 33449.630 | 133330.000 | 199 |
| 11 | 35091.014 | 284973 | 35019.080 | 35091.014 | 132570.000 | 209 |
| 12 | 33270.343 | 300568 | 33208.430 | 33270.343 | 132620.000 | 155 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 6293705744 | 629.37 | 295355944 | 6589061688 |
| 2 | 6294485480 | 629.45 | 295355944 | 6589841424 |
| 3 | 6293539704 | 629.35 | 295355944 | 6588895648 |
| 4 | 6294699152 | 629.47 | 295355976 | 6590055128 |
| 5 | 6294336200 | 629.43 | 295350208 | 6589686408 |
