# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 239.982 | 9600739 | 134.937 | 239.982 | 880.000 | 22 |
| 2 | 210.263 | 10957718 | 119.126 | 210.263 | 790.000 | 0 |
| 3 | 210.314 | 10955071 | 116.645 | 210.314 | 840.000 | 0 |
| 4 | 198.637 | 11599034 | 112.136 | 198.637 | 790.000 | 0 |
| 5 | 198.565 | 11603253 | 110.326 | 198.565 | 790.000 | 0 |
| 6 | 199.379 | 11555876 | 111.314 | 199.379 | 800.000 | 0 |
| 7 | 193.921 | 11881126 | 109.532 | 193.921 | 770.000 | 0 |
| 8 | 197.225 | 11682061 | 111.670 | 197.225 | 790.000 | 0 |
| 9 | 197.078 | 11690774 | 111.669 | 197.078 | 790.000 | 0 |
| 10 | 208.385 | 11056434 | 116.823 | 208.385 | 830.000 | 0 |
| 11 | 225.139 | 10233656 | 133.432 | 225.139 | 900.000 | 22 |
| 12 | 192.734 | 11954323 | 110.588 | 192.734 | 770.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 251476640 | 109.15 | 70180144 | 321656784 |
| 2 | 251119544 | 108.99 | 70180272 | 321299816 |
| 3 | 251603760 | 109.20 | 70180208 | 321783968 |
| 4 | 250963296 | 108.93 | 70180176 | 321143472 |
| 5 | 251322464 | 109.08 | 70180240 | 321502704 |
