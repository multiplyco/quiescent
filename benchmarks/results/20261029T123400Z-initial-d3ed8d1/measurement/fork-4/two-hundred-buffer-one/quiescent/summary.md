# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3217.645 | 3107863 | 1631.265 | 3217.645 | 12580.000 | 295 |
| 2 | 2897.133 | 3451688 | 1459.629 | 2897.133 | 11530.000 | 219 |
| 3 | 3107.670 | 3217844 | 1649.302 | 3107.670 | 12170.000 | 417 |
| 4 | 2793.377 | 3579896 | 1471.728 | 2793.377 | 11090.000 | 58 |
| 5 | 2924.854 | 3418974 | 1517.664 | 2924.854 | 11630.000 | 93 |
| 6 | 3039.874 | 3289610 | 1557.605 | 3039.874 | 11990.000 | 294 |
| 7 | 2936.062 | 3405923 | 1525.813 | 2936.062 | 11660.000 | 109 |
| 8 | 2771.496 | 3608160 | 1442.772 | 2771.496 | 11030.000 | 61 |
| 9 | 2881.748 | 3470116 | 1415.530 | 2881.748 | 11270.000 | 46 |
| 10 | 2971.107 | 3365749 | 1484.906 | 2971.107 | 11210.000 | 48 |
| 11 | 2994.800 | 3339121 | 1473.326 | 2994.800 | 11130.000 | 55 |
| 12 | 3063.608 | 3264125 | 1552.219 | 3063.607 | 11820.000 | 119 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1440324168 | 144.03 | 295485512 | 1735809680 |
| 2 | 1440398672 | 144.04 | 295485544 | 1735884216 |
| 3 | 1440392376 | 144.04 | 295485480 | 1735877856 |
| 4 | 1440381184 | 144.04 | 295418344 | 1735799528 |
| 5 | 1440420088 | 144.04 | 295389480 | 1735809568 |
