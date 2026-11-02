# Channel workload trials

Status: `complete`. Case: `mapcat`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7176.000 | 696767 | 7145.243 | 7176.000 | 28600.000 | 67 |
| 2 | 7139.287 | 700350 | 7106.601 | 7139.287 | 28450.000 | 35 |
| 3 | 7158.201 | 698500 | 7121.259 | 7158.201 | 28530.000 | 67 |
| 4 | 7124.270 | 701826 | 7081.541 | 7124.270 | 28390.000 | 34 |
| 5 | 7160.640 | 698262 | 7122.710 | 7160.640 | 28540.000 | 65 |
| 6 | 7121.576 | 702092 | 7095.404 | 7121.576 | 28400.000 | 30 |
| 7 | 7124.332 | 701820 | 7092.276 | 7124.332 | 28400.000 | 33 |
| 8 | 7140.945 | 700187 | 7107.851 | 7140.945 | 28440.000 | 30 |
| 9 | 7127.644 | 701494 | 7090.654 | 7127.644 | 28410.000 | 33 |
| 10 | 7194.617 | 694964 | 7162.034 | 7194.617 | 28660.000 | 63 |
| 11 | 7151.434 | 699160 | 7119.085 | 7151.434 | 28520.000 | 29 |
| 12 | 7166.946 | 697647 | 7128.513 | 7166.946 | 28570.000 | 29 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1494016192 | 298.80 | 73858656 | 1567874848 |
| 2 | 1493942272 | 298.79 | 73858656 | 1567800928 |
| 3 | 1493819736 | 298.76 | 73858656 | 1567678392 |
| 4 | 1493860432 | 298.77 | 73858592 | 1567719024 |
| 5 | 1493696464 | 298.74 | 73858656 | 1567555120 |
