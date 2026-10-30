# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 230.976 | 21647306 | 117.289 | 230.976 | 910.000 | 9 |
| 2 | 228.480 | 21883743 | 122.867 | 228.480 | 910.000 | 0 |
| 3 | 224.861 | 22235974 | 113.793 | 224.861 | 890.000 | 0 |
| 4 | 284.308 | 17586576 | 149.104 | 284.308 | 850.000 | 0 |
| 5 | 220.312 | 22695056 | 117.415 | 220.312 | 880.000 | 0 |
| 6 | 225.865 | 22137151 | 120.722 | 225.865 | 900.000 | 0 |
| 7 | 224.687 | 22253139 | 118.079 | 224.687 | 890.000 | 3 |
| 8 | 216.378 | 23107720 | 116.707 | 216.378 | 860.000 | 0 |
| 9 | 252.304 | 19817390 | 131.547 | 252.304 | 920.000 | 16 |
| 10 | 221.590 | 22564232 | 116.644 | 221.590 | 880.000 | 0 |
| 11 | 290.634 | 17203761 | 166.358 | 290.634 | 1040.000 | 48 |
| 12 | 220.141 | 22712670 | 116.261 | 220.141 | 880.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 492979976 | 98.60 | 148344080 | 641324056 |
| 2 | 493118568 | 98.62 | 148335496 | 641454064 |
| 3 | 492864352 | 98.57 | 148341728 | 641206080 |
| 4 | 493048200 | 98.61 | 148341712 | 641389912 |
| 5 | 493013800 | 98.60 | 148341680 | 641355480 |
