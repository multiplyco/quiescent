# Channel workload trials

Status: `complete`. Case: `two-hundred-buffer-one`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 35092.294 | 284963 | 34993.670 | 35092.283 | 139850.000 | 284 |
| 2 | 35089.883 | 284982 | 35004.821 | 35089.872 | 132450.000 | 262 |
| 3 | 33750.767 | 296290 | 33671.070 | 33750.756 | 134520.000 | 219 |
| 4 | 33688.009 | 296842 | 33622.034 | 33687.999 | 134210.000 | 225 |
| 5 | 33532.831 | 298215 | 33461.982 | 33532.822 | 133660.000 | 195 |
| 6 | 33870.072 | 295246 | 33800.975 | 33870.063 | 134970.000 | 253 |
| 7 | 33948.940 | 294560 | 33853.859 | 33948.940 | 135300.000 | 253 |
| 8 | 33435.670 | 299082 | 33333.141 | 33435.661 | 133240.000 | 275 |
| 9 | 33144.481 | 301709 | 33067.688 | 33144.472 | 132020.000 | 149 |
| 10 | 33797.837 | 295877 | 33704.786 | 33797.827 | 134700.000 | 192 |
| 11 | 33229.209 | 300940 | 33161.534 | 33229.209 | 132410.000 | 199 |
| 12 | 33582.609 | 297773 | 33525.303 | 33582.609 | 133760.000 | 201 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 6299347448 | 629.93 | 295355864 | 6594703312 |
| 2 | 6299727848 | 629.97 | 295356056 | 6595083904 |
| 3 | 6299189872 | 629.92 | 295356024 | 6594545896 |
| 4 | 6301173536 | 630.12 | 295356056 | 6596529592 |
| 5 | 6301204976 | 630.12 | 295354784 | 6596559760 |
