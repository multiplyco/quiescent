# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 11965.334 | 192556 | 11732.185 | 11965.334 | 47560.000 | 34 |
| 2 | 11970.443 | 192474 | 11768.261 | 11970.443 | 47670.000 | 39 |
| 3 | 11977.845 | 192355 | 11867.912 | 11977.845 | 47700.000 | 73 |
| 4 | 12038.139 | 191392 | 11815.941 | 12038.139 | 47910.000 | 75 |
| 5 | 12032.595 | 191480 | 11772.875 | 12032.595 | 47940.000 | 67 |
| 6 | 12046.799 | 191254 | 11848.574 | 12046.799 | 47910.000 | 62 |
| 7 | 12071.669 | 190860 | 11900.332 | 12071.669 | 48130.000 | 63 |
| 8 | 12102.202 | 190379 | 11883.011 | 12102.202 | 48170.000 | 72 |
| 9 | 12107.413 | 190297 | 11832.096 | 12107.413 | 48220.000 | 65 |
| 10 | 12095.280 | 190488 | 11747.542 | 12095.280 | 48180.000 | 64 |
| 11 | 12139.598 | 189792 | 11914.676 | 12139.598 | 48290.000 | 60 |
| 12 | 12108.654 | 190277 | 11962.547 | 12108.654 | 48230.000 | 70 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2026455992 | 879.54 | 76297248 | 2102753240 |
| 2 | 2024844080 | 878.84 | 76303520 | 2101147600 |
| 3 | 2025887624 | 879.29 | 76302592 | 2102190216 |
| 4 | 2025723600 | 879.22 | 76302976 | 2102026576 |
| 5 | 2024848584 | 878.84 | 76302784 | 2101151368 |
