# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9109.776 | 252915 | 8912.072 | 9109.776 | 36280.000 | 35 |
| 2 | 9127.732 | 252418 | 9056.902 | 9127.732 | 36390.000 | 79 |
| 3 | 9150.615 | 251786 | 9050.568 | 9150.615 | 36450.000 | 74 |
| 4 | 9107.320 | 252983 | 9036.816 | 9107.320 | 36290.000 | 43 |
| 5 | 9069.170 | 254047 | 8948.626 | 9069.170 | 36150.000 | 41 |
| 6 | 9078.686 | 253781 | 9003.733 | 9078.686 | 36200.000 | 31 |
| 7 | 9137.915 | 252136 | 9032.527 | 9137.915 | 36400.000 | 71 |
| 8 | 9118.615 | 252670 | 9029.829 | 9118.615 | 36340.000 | 67 |
| 9 | 9085.695 | 253585 | 9008.862 | 9085.695 | 36220.000 | 58 |
| 10 | 9090.049 | 253464 | 9033.323 | 9090.041 | 36260.000 | 61 |
| 11 | 9109.681 | 252918 | 8975.480 | 9109.681 | 36220.000 | 65 |
| 12 | 9096.000 | 253298 | 8979.050 | 9096.000 | 36240.000 | 62 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1637981896 | 710.93 | 70025456 | 1708007352 |
| 2 | 1636959232 | 710.49 | 70026416 | 1706985648 |
| 3 | 1638975808 | 711.36 | 70027088 | 1709002896 |
| 4 | 1632077096 | 708.37 | 70026320 | 1702103416 |
| 5 | 1637786392 | 710.84 | 70026544 | 1707812936 |
