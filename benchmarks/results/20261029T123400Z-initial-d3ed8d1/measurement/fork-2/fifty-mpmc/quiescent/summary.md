# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 217.183 | 23022028 | 120.125 | 217.183 | 860.000 | 0 |
| 2 | 213.622 | 23405861 | 110.207 | 213.622 | 860.000 | 0 |
| 3 | 263.056 | 19007389 | 145.187 | 263.056 | 840.000 | 0 |
| 4 | 209.356 | 23882765 | 111.964 | 209.356 | 840.000 | 0 |
| 5 | 217.596 | 22978381 | 115.731 | 217.596 | 870.000 | 0 |
| 6 | 214.294 | 23332428 | 110.977 | 214.294 | 850.000 | 6 |
| 7 | 226.160 | 22108279 | 126.562 | 226.160 | 830.000 | 0 |
| 8 | 224.968 | 22225401 | 108.951 | 224.968 | 890.000 | 17 |
| 9 | 208.856 | 23939909 | 109.127 | 208.856 | 830.000 | 0 |
| 10 | 253.982 | 19686470 | 151.892 | 253.982 | 1010.000 | 40 |
| 11 | 209.809 | 23831146 | 109.569 | 209.809 | 840.000 | 0 |
| 12 | 216.936 | 23048318 | 110.379 | 216.936 | 850.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 492553264 | 98.51 | 148369712 | 640922976 |
| 2 | 492252576 | 98.45 | 148369680 | 640622256 |
| 3 | 492874184 | 98.57 | 148369744 | 641243928 |
| 4 | 492399232 | 98.48 | 148369680 | 640768912 |
| 5 | 492384376 | 98.48 | 148369712 | 640754088 |
