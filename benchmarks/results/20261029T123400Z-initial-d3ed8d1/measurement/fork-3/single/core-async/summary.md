# Channel workload trials

Status: `complete`. Case: `single`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4244.360 | 235607 | 4244.360 | 4244.360 | 8090.000 | 0 |
| 2 | 4240.755 | 235807 | 4240.755 | 4240.755 | 8060.000 | 0 |
| 3 | 4228.058 | 236515 | 4228.058 | 4228.058 | 8070.000 | 0 |
| 4 | 4243.505 | 235654 | 4243.504 | 4243.504 | 8020.000 | 0 |
| 5 | 4211.365 | 237453 | 4211.365 | 4211.365 | 8070.000 | 0 |
| 6 | 4221.888 | 236861 | 4221.888 | 4221.888 | 8030.000 | 0 |
| 7 | 4189.859 | 238672 | 4189.859 | 4189.859 | 7970.000 | 0 |
| 8 | 4249.517 | 235321 | 4249.517 | 4249.517 | 8080.000 | 0 |
| 9 | 4258.271 | 234837 | 4258.271 | 4258.271 | 8100.000 | 29 |
| 10 | 4232.729 | 236254 | 4232.729 | 4232.729 | 8110.000 | 0 |
| 11 | 4246.346 | 235497 | 4246.345 | 4246.345 | 8130.000 | 23 |
| 12 | 4209.353 | 237566 | 4209.353 | 4209.353 | 8010.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 431501904 | 431.50 | 29437368 | 460939272 |
| 2 | 430567008 | 430.57 | 29437368 | 460004376 |
| 3 | 427828648 | 427.83 | 29437368 | 457266016 |
| 4 | 415899648 | 415.90 | 29437368 | 445337016 |
| 5 | 429521992 | 429.52 | 29437368 | 458959360 |
