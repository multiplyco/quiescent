# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 150.105 | 20652153 | 76.697 | 150.105 | 600.000 | 0 |
| 2 | 147.896 | 20960719 | 76.257 | 147.896 | 580.000 | 0 |
| 3 | 192.659 | 16090624 | 116.864 | 192.659 | 750.000 | 33 |
| 4 | 155.375 | 19951785 | 84.265 | 155.375 | 620.000 | 0 |
| 5 | 154.064 | 20121557 | 82.842 | 154.064 | 580.000 | 0 |
| 6 | 150.050 | 20659734 | 76.475 | 150.050 | 580.000 | 0 |
| 7 | 148.887 | 20821175 | 76.749 | 148.887 | 600.000 | 0 |
| 8 | 164.646 | 18828282 | 85.509 | 164.646 | 620.000 | 17 |
| 9 | 148.884 | 20821646 | 76.716 | 148.884 | 580.000 | 0 |
| 10 | 148.778 | 20836360 | 82.466 | 148.778 | 590.000 | 0 |
| 11 | 148.420 | 20886727 | 81.318 | 148.420 | 580.000 | 0 |
| 12 | 149.766 | 20698904 | 77.078 | 149.766 | 590.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 313693448 | 101.19 | 91671872 | 405365320 |
| 2 | 313670168 | 101.18 | 91671840 | 405342008 |
| 3 | 313674944 | 101.19 | 91671872 | 405346816 |
| 4 | 313680080 | 101.19 | 91671872 | 405351952 |
| 5 | 313664552 | 101.18 | 91671840 | 405336392 |
