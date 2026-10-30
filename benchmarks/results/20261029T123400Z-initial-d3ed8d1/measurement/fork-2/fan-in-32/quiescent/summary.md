# Channel workload trials

Status: `complete`. Case: `fan-in-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 161.716 | 14247177 | 90.172 | 161.716 | 650.000 | 0 |
| 2 | 187.770 | 12270356 | 114.461 | 187.770 | 720.000 | 28 |
| 3 | 209.029 | 11022416 | 118.314 | 209.029 | 710.000 | 19 |
| 4 | 170.846 | 13485792 | 89.378 | 170.846 | 670.000 | 10 |
| 5 | 161.964 | 14225387 | 91.280 | 161.964 | 640.000 | 0 |
| 6 | 156.856 | 14688631 | 89.177 | 156.856 | 630.000 | 0 |
| 7 | 157.004 | 14674798 | 90.349 | 157.004 | 620.000 | 0 |
| 8 | 160.019 | 14398288 | 88.190 | 160.019 | 640.000 | 0 |
| 9 | 173.956 | 13244751 | 98.219 | 173.956 | 680.000 | 0 |
| 10 | 160.582 | 14347801 | 87.753 | 160.582 | 640.000 | 0 |
| 11 | 159.692 | 14427775 | 89.738 | 159.692 | 640.000 | 0 |
| 12 | 161.095 | 14302123 | 89.399 | 161.095 | 640.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 240740608 | 104.49 | 70146552 | 310887160 |
| 2 | 240547160 | 104.40 | 70146584 | 310693744 |
| 3 | 240283192 | 104.29 | 70146552 | 310429744 |
| 4 | 241493584 | 104.81 | 70146552 | 311640136 |
| 5 | 240748000 | 104.49 | 70146552 | 310894552 |
