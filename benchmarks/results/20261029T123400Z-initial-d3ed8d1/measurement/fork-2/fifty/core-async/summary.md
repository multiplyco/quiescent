# Channel workload trials

Status: `complete`. Case: `fifty`. Implementation: `core-async`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9430.362 | 530202 | 9378.622 | 9430.362 | 37590.000 | 66 |
| 2 | 9506.824 | 525938 | 9463.445 | 9506.824 | 37880.000 | 141 |
| 3 | 9429.215 | 530267 | 9380.174 | 9429.205 | 37550.000 | 70 |
| 4 | 9416.276 | 530995 | 9374.481 | 9416.276 | 37540.000 | 57 |
| 5 | 9489.750 | 526884 | 9435.807 | 9489.750 | 37790.000 | 136 |
| 6 | 9462.420 | 528406 | 9421.711 | 9462.420 | 37730.000 | 119 |
| 7 | 9457.329 | 528691 | 9399.396 | 9457.329 | 37700.000 | 114 |
| 8 | 9459.411 | 528574 | 9390.598 | 9459.411 | 37720.000 | 88 |
| 9 | 9445.659 | 529344 | 9404.670 | 9445.659 | 37670.000 | 107 |
| 10 | 9452.028 | 528987 | 9415.751 | 9452.028 | 37710.000 | 120 |
| 11 | 9400.543 | 531884 | 9357.781 | 9400.533 | 37470.000 | 59 |
| 12 | 9408.770 | 531419 | 9340.662 | 9408.770 | 37500.000 | 63 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 1593574016 | 318.71 | 147403896 | 1740977912 |
| 2 | 1603834496 | 320.77 | 147404016 | 1751238512 |
| 3 | 1593737056 | 318.75 | 147403928 | 1741140984 |
| 4 | 1593697136 | 318.74 | 147403928 | 1741101064 |
| 5 | 1593433784 | 318.69 | 147403928 | 1740837712 |
