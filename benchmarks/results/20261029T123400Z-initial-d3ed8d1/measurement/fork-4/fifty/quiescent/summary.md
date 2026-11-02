# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 175.606 | 28472842 | 91.993 | 175.606 | 700.000 | 0 |
| 2 | 207.543 | 24091349 | 110.295 | 207.543 | 750.000 | 0 |
| 3 | 176.359 | 28351182 | 91.693 | 176.359 | 710.000 | 0 |
| 4 | 222.253 | 22496901 | 114.995 | 222.253 | 720.000 | 0 |
| 5 | 204.686 | 24427720 | 99.200 | 204.686 | 740.000 | 9 |
| 6 | 174.386 | 28671974 | 91.411 | 174.386 | 700.000 | 0 |
| 7 | 201.954 | 24758053 | 91.542 | 201.954 | 800.000 | 24 |
| 8 | 177.570 | 28157897 | 91.624 | 177.570 | 710.000 | 0 |
| 9 | 192.923 | 25917015 | 101.943 | 192.923 | 770.000 | 0 |
| 10 | 234.576 | 21315084 | 113.203 | 234.576 | 770.000 | 20 |
| 11 | 175.298 | 28522924 | 91.789 | 175.298 | 690.000 | 0 |
| 12 | 205.316 | 24352760 | 119.313 | 205.316 | 690.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 484063968 | 96.81 | 147532632 | 631596600 |
| 2 | 483987088 | 96.80 | 147532632 | 631519720 |
| 3 | 484039104 | 96.81 | 147532632 | 631571736 |
| 4 | 484156744 | 96.83 | 147459832 | 631616576 |
| 5 | 483886440 | 96.78 | 147459832 | 631346272 |
