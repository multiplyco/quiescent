# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 66.244 | 34780771 | 19.890 | 66.244 | 582.731 | 0 |
| 2 | 75.885 | 30361882 | 24.901 | 75.885 | 637.469 | 0 |
| 3 | 73.275 | 31443319 | 19.233 | 73.275 | 637.415 | 5 |
| 4 | 65.600 | 35121996 | 21.491 | 65.599 | 564.751 | 0 |
| 5 | 36.325 | 63427901 | 18.280 | 36.325 | 316.996 | 0 |
| 6 | 72.686 | 31697843 | 21.017 | 72.686 | 624.093 | 0 |
| 7 | 55.351 | 41625546 | 24.555 | 55.351 | 427.562 | 0 |
| 8 | 66.647 | 34570263 | 27.976 | 66.647 | 566.724 | 0 |
| 9 | 78.012 | 29533823 | 35.364 | 78.012 | 599.622 | 8 |
| 10 | 63.306 | 36394798 | 19.150 | 63.306 | 511.688 | 4 |
| 11 | 46.660 | 49378879 | 21.629 | 46.660 | 373.253 | 0 |
| 12 | 52.285 | 44066176 | 21.391 | 52.285 | 458.516 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 229529744 | 99.62 | 77174904 | 306704648 |
| 2 | 230927920 | 100.23 | 77174776 | 308102696 |
| 3 | 230721232 | 100.14 | 77174808 | 307896040 |
| 4 | 232786520 | 101.04 | 77174936 | 309961456 |
| 5 | 229877232 | 99.77 | 77174840 | 307052072 |
