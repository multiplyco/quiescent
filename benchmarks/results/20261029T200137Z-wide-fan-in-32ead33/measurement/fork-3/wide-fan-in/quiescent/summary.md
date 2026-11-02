# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 83.256 | 27673688 | 47.507 | 83.256 | 340.000 | 0 |
| 2 | 99.484 | 23159550 | 55.397 | 99.483 | 400.000 | 16 |
| 3 | 113.727 | 20258971 | 61.979 | 113.727 | 400.000 | 0 |
| 4 | 86.611 | 26601773 | 48.518 | 86.611 | 340.000 | 0 |
| 5 | 119.957 | 19206906 | 60.396 | 119.957 | 380.000 | 13 |
| 6 | 83.631 | 27549561 | 47.127 | 83.631 | 330.000 | 0 |
| 7 | 83.479 | 27599838 | 50.304 | 83.478 | 330.000 | 0 |
| 8 | 96.865 | 23785772 | 46.387 | 96.865 | 380.000 | 13 |
| 9 | 80.552 | 28602686 | 46.318 | 80.552 | 320.000 | 0 |
| 10 | 101.602 | 22676667 | 53.959 | 101.602 | 410.000 | 0 |
| 11 | 120.555 | 19111598 | 84.509 | 120.555 | 440.000 | 36 |
| 12 | 94.587 | 24358653 | 58.750 | 94.587 | 330.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 227519296 | 98.75 | 76933824 | 304453120 |
| 2 | 227341224 | 98.67 | 76933824 | 304275048 |
| 3 | 226986128 | 98.52 | 76933824 | 303919952 |
| 4 | 227283000 | 98.65 | 76933824 | 304216824 |
| 5 | 227057424 | 98.55 | 76933856 | 303991280 |
