# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 172.303 | 13371768 | 87.489 | 172.303 | 670.000 | 16 |
| 2 | 156.090 | 14760709 | 88.646 | 156.090 | 620.000 | 0 |
| 3 | 173.930 | 13246687 | 97.512 | 173.930 | 670.000 | 0 |
| 4 | 160.389 | 14365039 | 86.417 | 160.389 | 640.000 | 0 |
| 5 | 155.507 | 14816097 | 88.962 | 155.507 | 620.000 | 0 |
| 6 | 175.257 | 13146423 | 98.248 | 175.257 | 660.000 | 0 |
| 7 | 156.603 | 14712331 | 89.451 | 156.603 | 630.000 | 0 |
| 8 | 155.989 | 14770265 | 87.797 | 155.989 | 620.000 | 0 |
| 9 | 159.679 | 14428985 | 88.702 | 159.679 | 640.000 | 0 |
| 10 | 165.208 | 13946080 | 94.666 | 165.208 | 650.000 | 0 |
| 11 | 159.867 | 14411954 | 89.762 | 159.867 | 640.000 | 0 |
| 12 | 156.052 | 14764316 | 86.069 | 156.052 | 620.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 240882504 | 104.55 | 70095896 | 310978400 |
| 2 | 240805448 | 104.52 | 70095896 | 310901344 |
| 3 | 240549856 | 104.41 | 70095896 | 310645752 |
| 4 | 240673168 | 104.46 | 70095864 | 310769032 |
| 5 | 240821408 | 104.52 | 70095864 | 310917272 |
