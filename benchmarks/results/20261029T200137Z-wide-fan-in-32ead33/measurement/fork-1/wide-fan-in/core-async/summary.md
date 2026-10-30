# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 11943.258 | 192912 | 11852.775 | 11943.258 | 47590.000 | 34 |
| 2 | 12011.100 | 191823 | 11894.276 | 12011.100 | 47850.000 | 34 |
| 3 | 11977.462 | 192361 | 11832.271 | 11977.462 | 47740.000 | 32 |
| 4 | 12005.648 | 191910 | 11835.795 | 12005.648 | 47780.000 | 60 |
| 5 | 12093.322 | 190518 | 11919.917 | 12093.322 | 48120.000 | 67 |
| 6 | 12080.114 | 190727 | 11892.042 | 12080.114 | 48050.000 | 58 |
| 7 | 12092.369 | 190533 | 11944.762 | 12092.369 | 48160.000 | 61 |
| 8 | 12089.234 | 190583 | 11912.294 | 12089.234 | 48160.000 | 61 |
| 9 | 13209.152 | 174425 | 13050.433 | 13209.152 | 48440.000 | 72 |
| 10 | 12924.289 | 178269 | 12646.057 | 12924.289 | 48320.000 | 63 |
| 11 | 12128.679 | 189963 | 11864.209 | 12128.679 | 48230.000 | 60 |
| 12 | 12136.320 | 189843 | 11803.203 | 12136.320 | 48360.000 | 67 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 2025445536 | 879.10 | 76590016 | 2102035552 |
| 2 | 2024896480 | 878.86 | 76597408 | 2101493888 |
| 3 | 2025270176 | 879.02 | 76594816 | 2101864992 |
| 4 | 2025493128 | 879.12 | 76303680 | 2101796808 |
| 5 | 2026045816 | 879.36 | 76302720 | 2102348536 |
