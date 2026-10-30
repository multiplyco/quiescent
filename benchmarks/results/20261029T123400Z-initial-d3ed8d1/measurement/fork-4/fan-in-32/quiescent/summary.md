# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 224.998 | 10240102 | 123.318 | 224.998 | 900.000 | 0 |
| 2 | 242.003 | 9520525 | 145.150 | 242.003 | 960.000 | 21 |
| 3 | 238.284 | 9669129 | 127.041 | 238.284 | 940.000 | 13 |
| 4 | 218.849 | 10527793 | 121.340 | 218.849 | 880.000 | 0 |
| 5 | 224.827 | 10247864 | 123.932 | 224.827 | 890.000 | 0 |
| 6 | 222.004 | 10378213 | 126.462 | 222.004 | 880.000 | 0 |
| 7 | 236.988 | 9722000 | 144.671 | 236.988 | 820.000 | 0 |
| 8 | 214.652 | 10733655 | 122.973 | 214.652 | 850.000 | 0 |
| 9 | 218.974 | 10521780 | 121.840 | 218.974 | 870.000 | 0 |
| 10 | 221.804 | 10387530 | 123.568 | 221.804 | 890.000 | 0 |
| 11 | 222.532 | 10353572 | 115.572 | 222.531 | 890.000 | 0 |
| 12 | 217.753 | 10580780 | 117.833 | 217.753 | 870.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 240401728 | 104.34 | 70121240 | 310522968 |
| 2 | 240223592 | 104.26 | 70121272 | 310344864 |
| 3 | 240827520 | 104.53 | 70121336 | 310948856 |
| 4 | 241199096 | 104.69 | 70121208 | 311320304 |
| 5 | 240715824 | 104.48 | 70121272 | 310837096 |
