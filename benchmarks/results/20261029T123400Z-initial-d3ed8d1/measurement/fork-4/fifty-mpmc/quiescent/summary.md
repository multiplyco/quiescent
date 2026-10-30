# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 225.829 | 22140602 | 124.854 | 225.829 | 890.000 | 0 |
| 2 | 219.230 | 22807059 | 116.442 | 219.230 | 870.000 | 0 |
| 3 | 240.001 | 20833266 | 124.909 | 240.001 | 970.000 | 0 |
| 4 | 243.067 | 20570488 | 127.946 | 243.067 | 890.000 | 0 |
| 5 | 225.222 | 22200318 | 122.117 | 225.222 | 890.000 | 0 |
| 6 | 220.167 | 22710071 | 114.828 | 220.167 | 880.000 | 5 |
| 7 | 218.148 | 22920263 | 117.527 | 218.148 | 840.000 | 0 |
| 8 | 236.558 | 21136507 | 116.027 | 236.558 | 940.000 | 16 |
| 9 | 234.962 | 21279999 | 130.160 | 234.962 | 850.000 | 0 |
| 10 | 262.016 | 19082768 | 158.591 | 262.016 | 1040.000 | 41 |
| 11 | 221.809 | 22541928 | 120.486 | 221.809 | 840.000 | 0 |
| 12 | 229.581 | 21778761 | 125.079 | 229.581 | 920.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 492853160 | 98.57 | 148316112 | 641169272 |
| 2 | 493411280 | 98.68 | 148316080 | 641727360 |
| 3 | 493212648 | 98.64 | 148332080 | 641544728 |
| 4 | 492928520 | 98.59 | 148332080 | 641260600 |
| 5 | 492909408 | 98.58 | 148332080 | 641241488 |
