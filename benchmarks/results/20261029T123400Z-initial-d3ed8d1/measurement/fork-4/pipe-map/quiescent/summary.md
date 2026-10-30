# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 139.910 | 14294944 | 83.357 | 139.910 | 550.000 | 0 |
| 2 | 142.096 | 14075022 | 82.678 | 142.096 | 570.000 | 0 |
| 3 | 151.082 | 13237827 | 83.247 | 151.082 | 600.000 | 0 |
| 4 | 142.023 | 14082256 | 82.918 | 142.023 | 570.000 | 0 |
| 5 | 143.588 | 13928738 | 85.616 | 143.588 | 580.000 | 0 |
| 6 | 143.253 | 13961318 | 82.789 | 143.253 | 580.000 | 0 |
| 7 | 165.256 | 12102432 | 90.931 | 165.256 | 610.000 | 0 |
| 8 | 139.489 | 14338097 | 83.504 | 139.489 | 560.000 | 0 |
| 9 | 161.165 | 12409650 | 94.163 | 161.165 | 620.000 | 0 |
| 10 | 151.584 | 13194016 | 89.463 | 151.584 | 600.000 | 0 |
| 11 | 141.809 | 14103501 | 83.185 | 141.809 | 560.000 | 0 |
| 12 | 139.729 | 14313426 | 82.724 | 139.729 | 560.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 374073544 | 187.04 | 59428936 | 433502480 |
| 2 | 374199760 | 187.10 | 59428968 | 433628728 |
| 3 | 374055992 | 187.03 | 59428936 | 433484928 |
| 4 | 374135336 | 187.07 | 59428904 | 433564240 |
| 5 | 374081584 | 187.04 | 59428904 | 433510488 |
