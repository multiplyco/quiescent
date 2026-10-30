# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9890.048 | 505559 | 9833.283 | 9890.048 | 39440.000 | 74 |
| 2 | 9909.055 | 504589 | 9839.004 | 9909.045 | 39520.000 | 82 |
| 3 | 9924.140 | 503822 | 9851.598 | 9924.140 | 39570.000 | 84 |
| 4 | 10014.662 | 499268 | 9838.715 | 10014.662 | 39910.000 | 164 |
| 5 | 10020.094 | 498997 | 9907.648 | 10020.094 | 39910.000 | 158 |
| 6 | 10100.508 | 495025 | 9970.332 | 10100.508 | 39960.000 | 209 |
| 7 | 10221.419 | 489169 | 10027.794 | 10221.419 | 40710.000 | 138 |
| 8 | 9996.865 | 500157 | 9872.555 | 9996.865 | 39840.000 | 125 |
| 9 | 10053.515 | 497338 | 9917.998 | 10053.515 | 40070.000 | 150 |
| 10 | 10029.893 | 498510 | 9747.633 | 10029.893 | 39970.000 | 126 |
| 11 | 10024.209 | 498792 | 9918.543 | 10024.209 | 39970.000 | 91 |
| 12 | 9999.099 | 500045 | 9901.404 | 9999.099 | 39850.000 | 134 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1629915152 | 325.98 | 148253744 | 1778168896 |
| 2 | 1630483152 | 326.10 | 148253968 | 1778737120 |
| 3 | 1631882424 | 326.38 | 148254000 | 1780136424 |
| 4 | 1628280152 | 325.66 | 148253840 | 1776533992 |
| 5 | 1630319816 | 326.06 | 148254128 | 1778573944 |
