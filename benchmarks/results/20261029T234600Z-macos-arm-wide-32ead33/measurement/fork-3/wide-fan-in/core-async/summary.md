# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 35922.357 | 64138 | 35781.038 | 35922.357 | 300845.249 | 27 |
| 2 | 38211.672 | 60296 | 37857.440 | 38211.672 | 320192.889 | 33 |
| 3 | 39006.867 | 59067 | 38733.927 | 39006.867 | 325906.782 | 24 |
| 4 | 39898.148 | 57747 | 39628.753 | 39898.148 | 333682.765 | 33 |
| 5 | 40085.954 | 57476 | 39850.956 | 40085.954 | 335912.581 | 25 |
| 6 | 40712.220 | 56592 | 40437.086 | 40712.220 | 341243.215 | 26 |
| 7 | 41160.457 | 55976 | 40924.033 | 41160.457 | 341534.980 | 29 |
| 8 | 42619.981 | 54059 | 42388.612 | 42619.981 | 355191.844 | 27 |
| 9 | 43517.411 | 52944 | 43363.791 | 43517.411 | 363431.567 | 27 |
| 10 | 44073.227 | 52277 | 43770.699 | 44073.227 | 370699.166 | 24 |
| 11 | 43449.505 | 53027 | 43146.501 | 43449.505 | 363215.566 | 15 |
| 12 | 42263.859 | 54515 | 42008.552 | 42263.859 | 352269.304 | 25 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2011168752 | 872.90 | 76308064 | 2087476816 |
| 2 | 2011730584 | 873.15 | 76308000 | 2088038584 |
| 3 | 2007681960 | 871.39 | 76308608 | 2083990568 |
| 4 | 2014510256 | 874.35 | 76305856 | 2090816112 |
| 5 | 2012981560 | 873.69 | 76305664 | 2089287224 |
