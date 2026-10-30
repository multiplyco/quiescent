# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7046.834 | 709539 | 6971.697 | 7046.834 | 28060.000 | 79 |
| 2 | 7109.598 | 703275 | 7066.543 | 7109.598 | 28340.000 | 37 |
| 3 | 7203.059 | 694150 | 7156.110 | 7203.059 | 27970.000 | 97 |
| 4 | 6995.705 | 714724 | 6960.197 | 6995.705 | 27890.000 | 39 |
| 5 | 7036.780 | 710552 | 6993.453 | 7036.780 | 28020.000 | 77 |
| 6 | 6997.207 | 714571 | 6961.869 | 6997.207 | 27900.000 | 35 |
| 7 | 6991.078 | 715197 | 6946.827 | 6991.078 | 27850.000 | 34 |
| 8 | 7009.640 | 713303 | 6978.348 | 7009.640 | 27950.000 | 33 |
| 9 | 7013.054 | 712956 | 6982.798 | 7013.054 | 27940.000 | 32 |
| 10 | 7043.984 | 709826 | 7006.335 | 7043.984 | 28080.000 | 73 |
| 11 | 6995.609 | 714734 | 6966.350 | 6995.609 | 27880.000 | 33 |
| 12 | 7028.450 | 711394 | 7004.750 | 7028.450 | 28020.000 | 35 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1494197216 | 298.84 | 73858736 | 1568055952 |
| 2 | 1493902312 | 298.78 | 73858736 | 1567761048 |
| 3 | 1493698352 | 298.74 | 73858736 | 1567557088 |
| 4 | 1493830728 | 298.77 | 73858768 | 1567689496 |
| 5 | 1493752184 | 298.75 | 73858736 | 1567610920 |
