# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 34865.492 | 286817 | 34780.320 | 34865.478 | 138940.000 | 286 |
| 2 | 34255.046 | 291928 | 34170.782 | 34255.035 | 136540.000 | 307 |
| 3 | 33783.517 | 296002 | 33719.989 | 33783.507 | 134540.000 | 280 |
| 4 | 33300.150 | 300299 | 33231.613 | 33300.141 | 132670.000 | 205 |
| 5 | 33001.293 | 303018 | 32929.174 | 33001.293 | 131420.000 | 205 |
| 6 | 32864.478 | 304280 | 32784.457 | 32864.478 | 130900.000 | 188 |
| 7 | 33167.554 | 301499 | 33095.926 | 33167.554 | 132140.000 | 203 |
| 8 | 33032.091 | 302736 | 32943.751 | 33032.082 | 131580.000 | 192 |
| 9 | 32904.108 | 303913 | 32810.579 | 32904.098 | 131090.000 | 193 |
| 10 | 33573.723 | 297852 | 33503.630 | 33573.713 | 133770.000 | 196 |
| 11 | 33081.915 | 302280 | 33001.136 | 33081.904 | 131700.000 | 193 |
| 12 | 33584.590 | 297756 | 33498.756 | 33584.580 | 133820.000 | 202 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 6298108512 | 629.81 | 295355944 | 6593464456 |
| 2 | 6296014248 | 629.60 | 295355944 | 6591370192 |
| 3 | 6295779656 | 629.58 | 295355912 | 6591135568 |
| 4 | 6296098472 | 629.61 | 295355944 | 6591454416 |
| 5 | 6294056248 | 629.41 | 295355968 | 6589412216 |
