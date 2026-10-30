# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 33924.973 | 294768 | 33830.360 | 33924.961 | 135190.000 | 280 |
| 2 | 33431.124 | 299122 | 33356.005 | 33431.113 | 133290.000 | 256 |
| 3 | 33878.509 | 295172 | 33797.162 | 33878.498 | 135030.000 | 188 |
| 4 | 34613.011 | 288909 | 34503.601 | 34613.000 | 130690.000 | 195 |
| 5 | 33253.243 | 300723 | 33188.590 | 33253.233 | 132540.000 | 116 |
| 6 | 33005.977 | 302975 | 32928.114 | 33005.977 | 130580.000 | 194 |
| 7 | 33278.698 | 300493 | 33182.665 | 33278.688 | 132640.000 | 189 |
| 8 | 33746.146 | 296330 | 33677.028 | 33746.137 | 134510.000 | 216 |
| 9 | 33877.829 | 295178 | 33795.038 | 33877.819 | 135060.000 | 192 |
| 10 | 33514.631 | 298377 | 33435.177 | 33514.631 | 133540.000 | 197 |
| 11 | 33865.712 | 295284 | 33808.636 | 33865.702 | 134990.000 | 199 |
| 12 | 33112.077 | 302005 | 33039.772 | 33112.077 | 131950.000 | 241 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 6289657648 | 628.97 | 295355944 | 6585013592 |
| 2 | 6291608664 | 629.16 | 295355976 | 6586964640 |
| 3 | 6293786864 | 629.38 | 295355976 | 6589142840 |
| 4 | 6293131008 | 629.31 | 295355976 | 6588486984 |
| 5 | 6291212728 | 629.12 | 295355968 | 6586568696 |
