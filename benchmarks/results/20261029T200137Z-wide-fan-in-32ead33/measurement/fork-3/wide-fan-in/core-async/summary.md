# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12021.823 | 191651 | 11864.417 | 12021.823 | 47880.000 | 36 |
| 2 | 12046.515 | 191259 | 11893.175 | 12046.515 | 47940.000 | 32 |
| 3 | 13364.738 | 172394 | 13130.442 | 13364.738 | 48150.000 | 81 |
| 4 | 12544.758 | 183662 | 12407.108 | 12544.758 | 48290.000 | 66 |
| 5 | 12112.349 | 190219 | 11884.599 | 12112.349 | 48200.000 | 66 |
| 6 | 12126.310 | 190000 | 11907.396 | 12126.310 | 48220.000 | 67 |
| 7 | 12100.011 | 190413 | 11925.238 | 12100.011 | 48110.000 | 68 |
| 8 | 12122.618 | 190058 | 11908.344 | 12122.618 | 48240.000 | 70 |
| 9 | 12088.858 | 190589 | 11888.553 | 12088.858 | 48160.000 | 61 |
| 10 | 12094.782 | 190495 | 11886.092 | 12094.782 | 48080.000 | 73 |
| 11 | 12116.612 | 190152 | 11954.353 | 12116.612 | 48290.000 | 56 |
| 12 | 12127.137 | 189987 | 11944.064 | 12127.137 | 48300.000 | 63 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2025463920 | 879.11 | 76394784 | 2101858704 |
| 2 | 2026594912 | 879.60 | 76401760 | 2102996672 |
| 3 | 2026648368 | 879.62 | 76403264 | 2103051632 |
| 4 | 2026301680 | 879.47 | 76402784 | 2102704464 |
| 5 | 2025983808 | 879.33 | 76402752 | 2102386560 |
