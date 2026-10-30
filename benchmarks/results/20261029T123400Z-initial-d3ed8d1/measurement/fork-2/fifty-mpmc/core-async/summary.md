# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9864.829 | 506851 | 9806.170 | 9864.829 | 39330.000 | 74 |
| 2 | 9967.037 | 501654 | 9831.396 | 9967.037 | 39700.000 | 168 |
| 3 | 9963.827 | 501815 | 9839.616 | 9963.827 | 39690.000 | 162 |
| 4 | 9952.405 | 502391 | 9823.687 | 9952.405 | 39680.000 | 150 |
| 5 | 9932.978 | 503374 | 9801.213 | 9932.978 | 39580.000 | 135 |
| 6 | 9999.842 | 500008 | 9770.849 | 9999.842 | 39860.000 | 137 |
| 7 | 9939.863 | 503025 | 9677.584 | 9939.863 | 39610.000 | 147 |
| 8 | 9954.003 | 502310 | 9869.888 | 9954.003 | 39670.000 | 151 |
| 9 | 10871.433 | 459921 | 10631.006 | 10871.433 | 39370.000 | 76 |
| 10 | 10569.191 | 473073 | 10397.690 | 10569.191 | 39400.000 | 117 |
| 11 | 9875.730 | 506292 | 9790.769 | 9875.730 | 39360.000 | 63 |
| 12 | 9898.219 | 505141 | 9805.318 | 9898.219 | 39440.000 | 81 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1630616384 | 326.12 | 148253744 | 1778870128 |
| 2 | 1630081592 | 326.02 | 148254000 | 1778335592 |
| 3 | 1629199208 | 325.84 | 148253904 | 1777453112 |
| 4 | 1629309960 | 325.86 | 148253936 | 1777563896 |
| 5 | 1631897264 | 326.38 | 148254000 | 1780151264 |
