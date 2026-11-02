# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5509.062 | 363038 | 5474.257 | 5509.052 | 21940.000 | 44 |
| 2 | 5554.681 | 360057 | 5507.905 | 5554.681 | 22120.000 | 44 |
| 3 | 5540.416 | 360984 | 5506.127 | 5540.416 | 22080.000 | 49 |
| 4 | 5565.041 | 359386 | 5519.189 | 5565.041 | 22150.000 | 44 |
| 5 | 5549.671 | 360382 | 5463.017 | 5549.671 | 22080.000 | 86 |
| 6 | 5532.840 | 361478 | 5486.773 | 5532.840 | 22060.000 | 31 |
| 7 | 5594.276 | 357508 | 5550.268 | 5594.267 | 22200.000 | 38 |
| 8 | 5596.970 | 357336 | 5548.970 | 5596.960 | 22250.000 | 34 |
| 9 | 5532.194 | 361520 | 5496.525 | 5532.194 | 22040.000 | 27 |
| 10 | 5557.864 | 359850 | 5529.772 | 5557.864 | 22150.000 | 34 |
| 11 | 5542.255 | 360864 | 5500.434 | 5542.245 | 22060.000 | 29 |
| 12 | 5531.578 | 361561 | 5506.790 | 5531.578 | 22040.000 | 31 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1212760320 | 606.38 | 59385608 | 1272145928 |
| 2 | 1213259008 | 606.63 | 59385704 | 1272644712 |
| 3 | 1212440768 | 606.22 | 59385640 | 1271826408 |
| 4 | 1212608632 | 606.30 | 59385704 | 1271994336 |
| 5 | 1213366272 | 606.68 | 59385672 | 1272751944 |
