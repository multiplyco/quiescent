# Channel workload trials

Status: `complete`. Case: `fifty-mpmc`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9841.549 | 508050 | 9775.250 | 9841.549 | 39240.000 | 85 |
| 2 | 9867.802 | 506698 | 9791.214 | 9867.793 | 39270.000 | 85 |
| 3 | 9863.631 | 506913 | 9793.983 | 9863.631 | 39330.000 | 95 |
| 4 | 9937.272 | 503156 | 9740.055 | 9937.272 | 39570.000 | 158 |
| 5 | 9888.568 | 505634 | 9725.812 | 9888.568 | 39430.000 | 130 |
| 6 | 9932.016 | 503422 | 9872.348 | 9932.006 | 39580.000 | 153 |
| 7 | 10198.159 | 490285 | 10000.206 | 10198.159 | 40650.000 | 142 |
| 8 | 9907.739 | 504656 | 9718.737 | 9907.739 | 39490.000 | 121 |
| 9 | 9931.844 | 503431 | 9763.091 | 9931.844 | 39580.000 | 149 |
| 10 | 9937.985 | 503120 | 9804.602 | 9937.976 | 39610.000 | 153 |
| 11 | 9909.545 | 504564 | 9672.415 | 9909.536 | 39510.000 | 135 |
| 12 | 9909.245 | 504579 | 9744.857 | 9909.245 | 39510.000 | 127 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1630046696 | 326.01 | 148253744 | 1778300440 |
| 2 | 1631027928 | 326.21 | 148253968 | 1779281896 |
| 3 | 1631979688 | 326.40 | 148253872 | 1780233560 |
| 4 | 1631484032 | 326.30 | 148253872 | 1779737904 |
| 5 | 1629130472 | 325.83 | 148254000 | 1777384472 |
