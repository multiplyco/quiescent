# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 173.622 | 28798194 | 89.965 | 173.622 | 690.000 | 0 |
| 2 | 228.324 | 21898728 | 142.085 | 228.324 | 850.000 | 52 |
| 3 | 172.151 | 29044217 | 90.027 | 172.151 | 690.000 | 0 |
| 4 | 221.367 | 22586911 | 142.664 | 221.367 | 820.000 | 53 |
| 5 | 170.644 | 29300852 | 89.371 | 170.644 | 670.000 | 0 |
| 6 | 197.266 | 25346526 | 111.216 | 197.266 | 780.000 | 0 |
| 7 | 193.758 | 25805374 | 101.691 | 193.758 | 730.000 | 8 |
| 8 | 172.027 | 29065150 | 89.823 | 172.027 | 680.000 | 0 |
| 9 | 206.537 | 24208694 | 89.145 | 206.537 | 740.000 | 24 |
| 10 | 171.804 | 29102929 | 89.257 | 171.804 | 690.000 | 0 |
| 11 | 247.954 | 20164995 | 165.004 | 247.954 | 900.000 | 66 |
| 12 | 192.373 | 25991125 | 89.527 | 192.373 | 760.000 | 17 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 484162760 | 96.83 | 147532632 | 631695392 |
| 2 | 483956368 | 96.79 | 147532632 | 631489000 |
| 3 | 484211504 | 96.84 | 147532632 | 631744136 |
| 4 | 483949128 | 96.79 | 147532632 | 631481760 |
| 5 | 484079280 | 96.82 | 147532632 | 631611912 |
