# Channel workload trials

Status: `complete`. Case: `mixed`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 148.622 | 20858327 | 74.921 | 148.622 | 600.000 | 0 |
| 2 | 185.182 | 16740283 | 112.566 | 185.182 | 720.000 | 36 |
| 3 | 157.798 | 19645406 | 87.885 | 157.798 | 610.000 | 0 |
| 4 | 145.468 | 21310525 | 76.442 | 145.468 | 580.000 | 0 |
| 5 | 148.289 | 20905120 | 75.251 | 148.289 | 590.000 | 0 |
| 6 | 148.835 | 20828446 | 75.412 | 148.835 | 580.000 | 0 |
| 7 | 169.470 | 18292329 | 97.224 | 169.470 | 650.000 | 22 |
| 8 | 150.544 | 20591964 | 75.402 | 150.544 | 580.000 | 0 |
| 9 | 152.896 | 20275177 | 80.057 | 152.896 | 610.000 | 0 |
| 10 | 150.003 | 20666187 | 75.053 | 150.003 | 590.000 | 0 |
| 11 | 146.117 | 21215903 | 77.491 | 146.117 | 570.000 | 0 |
| 12 | 149.345 | 20757337 | 75.819 | 149.345 | 590.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 313709312 | 101.20 | 91689680 | 405398992 |
| 2 | 313751592 | 101.21 | 91689712 | 405441304 |
| 3 | 313671920 | 101.18 | 91689680 | 405361600 |
| 4 | 313676552 | 101.19 | 91689680 | 405366232 |
| 5 | 313737720 | 101.21 | 91689808 | 405427528 |
