# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 158.779 | 14510702 | 88.047 | 158.779 | 630.000 | 0 |
| 2 | 160.189 | 14383010 | 94.039 | 160.189 | 640.000 | 0 |
| 3 | 192.386 | 11975922 | 119.715 | 192.386 | 740.000 | 20 |
| 4 | 189.677 | 12146988 | 100.809 | 189.677 | 700.000 | 12 |
| 5 | 174.181 | 13227615 | 98.540 | 174.181 | 660.000 | 0 |
| 6 | 191.668 | 12020801 | 108.909 | 191.668 | 640.000 | 0 |
| 7 | 158.033 | 14579273 | 88.293 | 158.033 | 630.000 | 0 |
| 8 | 160.152 | 14386345 | 92.686 | 160.152 | 630.000 | 0 |
| 9 | 197.417 | 11670712 | 110.398 | 197.417 | 640.000 | 0 |
| 10 | 168.145 | 13702489 | 99.607 | 168.145 | 640.000 | 0 |
| 11 | 158.418 | 14543782 | 88.128 | 158.418 | 630.000 | 0 |
| 12 | 161.490 | 14267176 | 86.808 | 161.490 | 640.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 240487344 | 104.38 | 70108568 | 310595912 |
| 2 | 240339704 | 104.31 | 70108600 | 310448304 |
| 3 | 240916016 | 104.56 | 70108696 | 311024712 |
| 4 | 240476568 | 104.37 | 70108568 | 310585136 |
| 5 | 240460368 | 104.37 | 70108536 | 310568904 |
