# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 34922.556 | 65975 | 34637.697 | 34922.556 | 292232.717 | 24 |
| 2 | 36983.911 | 62297 | 36720.492 | 36983.911 | 309304.317 | 26 |
| 3 | 38178.146 | 60349 | 37884.798 | 38178.146 | 321113.717 | 30 |
| 4 | 38989.284 | 59093 | 38776.678 | 38989.284 | 329467.866 | 28 |
| 5 | 39920.221 | 57715 | 39725.217 | 39920.221 | 336156.768 | 33 |
| 6 | 40550.026 | 56819 | 40219.251 | 40550.026 | 339051.728 | 36 |
| 7 | 41201.852 | 55920 | 40928.373 | 41201.852 | 346967.469 | 27 |
| 8 | 41269.199 | 55829 | 40924.381 | 41269.199 | 347482.642 | 28 |
| 9 | 42460.998 | 54262 | 42249.585 | 42460.998 | 357555.070 | 25 |
| 10 | 41084.170 | 56080 | 40910.253 | 41084.170 | 345517.100 | 26 |
| 11 | 39926.578 | 57706 | 39661.955 | 39926.578 | 334139.850 | 30 |
| 12 | 40152.434 | 57381 | 39880.668 | 40152.434 | 338145.269 | 13 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2013768904 | 874.03 | 76407848 | 2090176752 |
| 2 | 2014267048 | 874.25 | 76409344 | 2090676392 |
| 3 | 2011528664 | 873.06 | 76409280 | 2087937944 |
| 4 | 2008740096 | 871.85 | 76407200 | 2085147296 |
| 5 | 2012302504 | 873.40 | 76407200 | 2088709704 |
