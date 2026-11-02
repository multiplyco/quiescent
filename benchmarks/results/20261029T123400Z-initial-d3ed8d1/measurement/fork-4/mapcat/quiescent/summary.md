# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 162.143 | 30837015 | 83.273 | 162.143 | 640.000 | 0 |
| 2 | 181.141 | 27602757 | 104.411 | 181.141 | 710.000 | 21 |
| 3 | 160.241 | 31203097 | 83.464 | 160.241 | 630.000 | 0 |
| 4 | 163.060 | 30663582 | 87.294 | 163.060 | 650.000 | 0 |
| 5 | 171.128 | 29217903 | 87.127 | 171.128 | 660.000 | 4 |
| 6 | 162.678 | 30735513 | 84.735 | 162.678 | 640.000 | 0 |
| 7 | 178.594 | 27996479 | 103.558 | 178.594 | 710.000 | 19 |
| 8 | 205.763 | 24299764 | 108.792 | 205.763 | 610.000 | 0 |
| 9 | 183.322 | 27274424 | 99.108 | 183.322 | 690.000 | 0 |
| 10 | 183.896 | 27189334 | 86.712 | 183.896 | 690.000 | 14 |
| 11 | 156.818 | 31884074 | 82.115 | 156.818 | 620.000 | 0 |
| 12 | 158.756 | 31494924 | 81.973 | 158.756 | 630.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 483786976 | 96.76 | 73911456 | 557698432 |
| 2 | 483818256 | 96.76 | 73911456 | 557729712 |
| 3 | 483827472 | 96.77 | 73911456 | 557738928 |
| 4 | 483834504 | 96.77 | 73910336 | 557744840 |
| 5 | 483825152 | 96.77 | 73923456 | 557748608 |
