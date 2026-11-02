# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9027.613 | 255217 | 8960.235 | 9027.613 | 36010.000 | 44 |
| 2 | 9078.301 | 253792 | 8964.513 | 9078.301 | 36170.000 | 81 |
| 3 | 9063.039 | 254219 | 8894.996 | 9063.039 | 36110.000 | 69 |
| 4 | 9054.086 | 254471 | 8936.258 | 9054.078 | 36110.000 | 46 |
| 5 | 9027.819 | 255211 | 8913.875 | 9027.819 | 35990.000 | 42 |
| 6 | 9080.083 | 253742 | 9028.762 | 9080.083 | 36220.000 | 75 |
| 7 | 9050.310 | 254577 | 8966.229 | 9050.310 | 36100.000 | 61 |
| 8 | 9049.860 | 254590 | 8914.095 | 9049.860 | 36070.000 | 66 |
| 9 | 9049.810 | 254591 | 8960.694 | 9049.810 | 36090.000 | 61 |
| 10 | 9062.797 | 254226 | 8961.452 | 9062.797 | 36130.000 | 33 |
| 11 | 9040.622 | 254850 | 8935.696 | 9040.622 | 36060.000 | 33 |
| 12 | 9062.494 | 254235 | 8949.896 | 9062.494 | 36100.000 | 34 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1637282728 | 710.63 | 70025456 | 1707308184 |
| 2 | 1636178816 | 710.15 | 70026056 | 1706204872 |
| 3 | 1638237768 | 711.04 | 70026320 | 1708264088 |
| 4 | 1638739384 | 711.26 | 70026512 | 1708765896 |
| 5 | 1637503184 | 710.72 | 70026128 | 1707529312 |
