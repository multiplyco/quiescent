# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 140.935 | 14190907 | 82.943 | 140.935 | 560.000 | 0 |
| 2 | 162.414 | 12314194 | 92.783 | 162.414 | 610.000 | 0 |
| 3 | 163.494 | 12232877 | 94.549 | 163.494 | 600.000 | 0 |
| 4 | 140.303 | 14254849 | 83.147 | 140.303 | 560.000 | 0 |
| 5 | 140.864 | 14198141 | 82.875 | 140.864 | 560.000 | 0 |
| 6 | 170.866 | 11705092 | 85.575 | 170.866 | 680.000 | 0 |
| 7 | 150.299 | 13306831 | 88.768 | 150.299 | 600.000 | 0 |
| 8 | 139.983 | 14287479 | 82.677 | 139.983 | 560.000 | 0 |
| 9 | 141.753 | 14109036 | 82.944 | 141.753 | 560.000 | 0 |
| 10 | 156.257 | 12799430 | 89.107 | 156.257 | 620.000 | 0 |
| 11 | 140.842 | 14200284 | 82.840 | 140.842 | 560.000 | 0 |
| 12 | 140.959 | 14188524 | 82.420 | 140.959 | 560.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 374032536 | 187.02 | 59428968 | 433461504 |
| 2 | 374076336 | 187.04 | 59428968 | 433505304 |
| 3 | 374049744 | 187.02 | 59428936 | 433478680 |
| 4 | 374031888 | 187.02 | 59428968 | 433460856 |
| 5 | 374058824 | 187.03 | 59428968 | 433487792 |
