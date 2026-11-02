# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4307.367 | 232160 | 4307.367 | 4307.367 | 8230.000 | 32 |
| 2 | 4275.115 | 233912 | 4275.115 | 4275.115 | 8100.000 | 0 |
| 3 | 4282.136 | 233528 | 4282.136 | 4282.136 | 8140.000 | 28 |
| 4 | 4238.203 | 235949 | 4238.203 | 4238.203 | 8130.000 | 0 |
| 5 | 4265.116 | 234460 | 4265.116 | 4265.116 | 8180.000 | 25 |
| 6 | 4252.434 | 235159 | 4252.434 | 4252.434 | 8080.000 | 0 |
| 7 | 4286.604 | 233285 | 4286.604 | 4286.604 | 8120.000 | 23 |
| 8 | 4254.614 | 235039 | 4254.614 | 4254.614 | 8080.000 | 0 |
| 9 | 4254.048 | 235070 | 4254.048 | 4254.048 | 8110.000 | 23 |
| 10 | 4251.945 | 235186 | 4251.945 | 4251.945 | 8060.000 | 0 |
| 11 | 4347.749 | 230004 | 4347.749 | 4347.749 | 8210.000 | 22 |
| 12 | 4220.922 | 236915 | 4220.922 | 4220.922 | 8000.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 444915672 | 444.92 | 29437368 | 474353040 |
| 2 | 462813432 | 462.81 | 29437368 | 492250800 |
| 3 | 445895888 | 445.90 | 29437336 | 475333224 |
| 4 | 443034552 | 443.03 | 29437368 | 472471920 |
| 5 | 450926936 | 450.93 | 29437368 | 480364304 |
