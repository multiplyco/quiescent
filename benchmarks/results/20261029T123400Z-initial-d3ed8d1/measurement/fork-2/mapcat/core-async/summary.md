# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7052.395 | 708979 | 7019.157 | 7052.395 | 28130.000 | 42 |
| 2 | 7084.648 | 705751 | 7052.426 | 7084.648 | 28230.000 | 45 |
| 3 | 7043.766 | 709848 | 7015.064 | 7043.766 | 28100.000 | 34 |
| 4 | 7061.866 | 708028 | 7035.496 | 7061.866 | 28150.000 | 40 |
| 5 | 7074.452 | 706769 | 7041.427 | 7074.452 | 28210.000 | 70 |
| 6 | 7032.699 | 710965 | 7006.670 | 7032.699 | 28060.000 | 32 |
| 7 | 7075.812 | 706633 | 7036.038 | 7075.812 | 28200.000 | 71 |
| 8 | 7032.291 | 711006 | 6990.020 | 7032.291 | 28030.000 | 31 |
| 9 | 7048.107 | 709410 | 7008.644 | 7048.107 | 28100.000 | 32 |
| 10 | 7081.387 | 706076 | 7047.681 | 7081.387 | 28200.000 | 76 |
| 11 | 7043.857 | 709838 | 7008.785 | 7043.857 | 28060.000 | 31 |
| 12 | 7059.715 | 708244 | 7006.612 | 7059.715 | 28130.000 | 29 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1493829680 | 298.77 | 73858656 | 1567688336 |
| 2 | 1493840600 | 298.77 | 73858688 | 1567699288 |
| 3 | 1493883960 | 298.78 | 73858656 | 1567742616 |
| 4 | 1493862024 | 298.77 | 73858656 | 1567720680 |
| 5 | 1493881024 | 298.78 | 73858656 | 1567739680 |
