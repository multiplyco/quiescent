# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 237.147 | 21083926 | 145.160 | 237.147 | 930.000 | 50 |
| 2 | 178.773 | 27968488 | 94.446 | 178.773 | 710.000 | 0 |
| 3 | 200.162 | 24979814 | 102.715 | 200.162 | 800.000 | 0 |
| 4 | 177.539 | 28162829 | 92.229 | 177.539 | 710.000 | 0 |
| 5 | 175.697 | 28458025 | 94.807 | 175.697 | 700.000 | 0 |
| 6 | 186.517 | 26807159 | 93.564 | 186.517 | 730.000 | 8 |
| 7 | 176.955 | 28255714 | 92.645 | 176.955 | 710.000 | 0 |
| 8 | 205.473 | 24334145 | 92.485 | 205.473 | 810.000 | 28 |
| 9 | 178.560 | 28001817 | 93.084 | 178.560 | 710.000 | 0 |
| 10 | 199.726 | 25034327 | 102.680 | 199.726 | 770.000 | 0 |
| 11 | 203.421 | 24579551 | 93.238 | 203.421 | 770.000 | 24 |
| 12 | 177.103 | 28232220 | 94.438 | 177.103 | 680.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 483935208 | 96.79 | 147541432 | 631476640 |
| 2 | 483968352 | 96.79 | 147541432 | 631509784 |
| 3 | 483834528 | 96.77 | 147541496 | 631376024 |
| 4 | 483881912 | 96.78 | 147534232 | 631416144 |
| 5 | 483875088 | 96.78 | 147461432 | 631336520 |
