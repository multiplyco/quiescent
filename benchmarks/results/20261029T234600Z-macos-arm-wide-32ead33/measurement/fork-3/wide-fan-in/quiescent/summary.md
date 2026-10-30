# Channel workload trials

Status: `complete`. Case: `wide-fan-in`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 62.178 | 37054708 | 22.492 | 62.178 | 514.789 | 0 |
| 2 | 56.711 | 40627039 | 20.665 | 56.711 | 467.273 | 0 |
| 3 | 71.035 | 32434563 | 21.674 | 71.035 | 617.934 | 0 |
| 4 | 84.220 | 27356841 | 30.542 | 84.220 | 640.100 | 6 |
| 5 | 67.906 | 33929255 | 19.832 | 67.902 | 598.399 | 0 |
| 6 | 77.203 | 29843303 | 20.624 | 77.203 | 666.145 | 0 |
| 7 | 68.811 | 33483242 | 24.171 | 68.811 | 589.877 | 0 |
| 8 | 83.852 | 27476983 | 27.640 | 83.852 | 737.949 | 6 |
| 9 | 62.343 | 36956761 | 20.194 | 62.343 | 459.698 | 0 |
| 10 | 74.959 | 30736939 | 22.606 | 74.959 | 622.156 | 0 |
| 11 | 57.557 | 40029883 | 21.296 | 57.557 | 462.417 | 0 |
| 12 | 71.398 | 32269623 | 30.258 | 71.398 | 601.252 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 229584128 | 99.65 | 77373112 | 306957240 |
| 2 | 230154600 | 99.89 | 77373048 | 307527648 |
| 3 | 229271440 | 99.51 | 77373112 | 306644552 |
| 4 | 229696240 | 99.69 | 77373048 | 307069288 |
| 5 | 230825816 | 100.18 | 77372952 | 308198768 |
