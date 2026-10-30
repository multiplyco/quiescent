# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 165.894 | 30139647 | 88.878 | 165.894 | 660.000 | 0 |
| 2 | 209.373 | 23880811 | 123.574 | 209.373 | 800.000 | 17 |
| 3 | 179.364 | 27876350 | 101.716 | 179.364 | 640.000 | 0 |
| 4 | 185.484 | 26956436 | 109.004 | 185.484 | 700.000 | 17 |
| 5 | 166.147 | 30093902 | 85.980 | 166.147 | 660.000 | 0 |
| 6 | 195.513 | 25573705 | 112.997 | 195.513 | 740.000 | 22 |
| 7 | 162.860 | 30701248 | 85.356 | 162.860 | 650.000 | 0 |
| 8 | 164.440 | 30406249 | 89.153 | 164.439 | 660.000 | 0 |
| 9 | 167.832 | 29791727 | 85.652 | 167.832 | 660.000 | 3 |
| 10 | 199.973 | 25003332 | 112.505 | 199.973 | 640.000 | 0 |
| 11 | 191.590 | 26097409 | 112.442 | 191.590 | 740.000 | 22 |
| 12 | 165.581 | 30196744 | 86.577 | 165.581 | 660.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 483800480 | 96.76 | 73916992 | 557717472 |
| 2 | 483791144 | 96.76 | 73916992 | 557708136 |
| 3 | 483798632 | 96.76 | 73916992 | 557715624 |
| 4 | 483793520 | 96.76 | 73916992 | 557710512 |
| 5 | 483771896 | 96.75 | 73916992 | 557688888 |
