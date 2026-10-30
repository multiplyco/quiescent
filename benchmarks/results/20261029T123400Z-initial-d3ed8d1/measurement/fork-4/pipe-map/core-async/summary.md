# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5577.543 | 358581 | 5509.843 | 5577.543 | 22180.000 | 40 |
| 2 | 5572.182 | 358926 | 5552.128 | 5572.182 | 22200.000 | 38 |
| 3 | 5561.568 | 359611 | 5537.577 | 5561.568 | 22170.000 | 39 |
| 4 | 5576.918 | 358621 | 5532.736 | 5576.918 | 22210.000 | 41 |
| 5 | 5589.243 | 357830 | 5511.652 | 5589.243 | 22250.000 | 76 |
| 6 | 5574.394 | 358783 | 5520.030 | 5574.379 | 22190.000 | 42 |
| 7 | 5561.848 | 359593 | 5517.470 | 5561.848 | 22120.000 | 35 |
| 8 | 5549.416 | 360398 | 5504.014 | 5549.406 | 22090.000 | 29 |
| 9 | 5544.832 | 360696 | 5523.499 | 5544.832 | 22080.000 | 31 |
| 10 | 5537.312 | 361186 | 5487.957 | 5537.312 | 22070.000 | 33 |
| 11 | 5540.550 | 360975 | 5484.791 | 5540.541 | 22050.000 | 26 |
| 12 | 5527.813 | 361807 | 5502.367 | 5527.808 | 22030.000 | 29 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1212215928 | 606.11 | 59385648 | 1271601576 |
| 2 | 1213356480 | 606.68 | 59385712 | 1272742192 |
| 3 | 1212374320 | 606.19 | 59385648 | 1271759968 |
| 4 | 1213184552 | 606.59 | 59385680 | 1272570232 |
| 5 | 1213072352 | 606.54 | 59385744 | 1272458096 |
