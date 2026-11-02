# Channel workload trials

Status: `complete`. Case: `pipe-map`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5623.561 | 355647 | 5588.963 | 5623.561 | 22390.000 | 38 |
| 2 | 5595.446 | 357434 | 5550.399 | 5595.436 | 22310.000 | 45 |
| 3 | 5609.939 | 356510 | 5585.455 | 5609.929 | 22350.000 | 40 |
| 4 | 5589.891 | 357789 | 5556.352 | 5589.891 | 22280.000 | 45 |
| 5 | 5624.646 | 355578 | 5554.674 | 5624.646 | 22380.000 | 78 |
| 6 | 5617.019 | 356061 | 5587.465 | 5617.019 | 22380.000 | 36 |
| 7 | 5635.670 | 354882 | 5583.498 | 5635.670 | 22430.000 | 31 |
| 8 | 5643.738 | 354375 | 5602.192 | 5643.729 | 22480.000 | 35 |
| 9 | 5625.410 | 355530 | 5596.437 | 5625.400 | 22420.000 | 34 |
| 10 | 5627.433 | 355402 | 5595.601 | 5627.433 | 22440.000 | 30 |
| 11 | 5631.639 | 355136 | 5602.826 | 5631.630 | 22450.000 | 27 |
| 12 | 5640.186 | 354598 | 5580.789 | 5640.176 | 22450.000 | 26 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1212924208 | 606.46 | 59385608 | 1272309816 |
| 2 | 1214341312 | 607.17 | 59385704 | 1273727016 |
| 3 | 1213047600 | 606.52 | 59385864 | 1272433464 |
| 4 | 1213695504 | 606.85 | 59385736 | 1273081240 |
| 5 | 1212948224 | 606.47 | 59385672 | 1272333896 |
