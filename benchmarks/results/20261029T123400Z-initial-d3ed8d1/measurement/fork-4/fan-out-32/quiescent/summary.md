# Channel workload trials

Status: `complete`. Case: `fan-out-32`. Implementation: `quiescent`.

Completion median/max describes whole workloads finishing from the common start, not operation latency. Mixed topologies have different amounts of work, so their completion spread is not a fairness score. CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.

| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 211.806 | 10877888 | 116.928 | 211.806 | 830.000 | 0 |
| 2 | 206.288 | 11168827 | 116.278 | 206.288 | 820.000 | 0 |
| 3 | 236.090 | 9759009 | 148.210 | 236.090 | 840.000 | 0 |
| 4 | 224.621 | 10257275 | 127.013 | 224.621 | 880.000 | 18 |
| 5 | 204.274 | 11278996 | 117.259 | 204.274 | 810.000 | 0 |
| 6 | 208.363 | 11057606 | 117.348 | 208.363 | 830.000 | 0 |
| 7 | 226.374 | 10177829 | 130.930 | 226.374 | 850.000 | 0 |
| 8 | 226.316 | 10180446 | 127.786 | 226.316 | 860.000 | 0 |
| 9 | 208.884 | 11030049 | 117.040 | 208.884 | 830.000 | 0 |
| 10 | 240.183 | 9592668 | 142.584 | 240.183 | 820.000 | 0 |
| 11 | 207.518 | 11102638 | 114.291 | 207.518 | 830.000 | 0 |
| 12 | 265.335 | 8683367 | 143.462 | 265.335 | 790.000 | 0 |

## Separate allocation passes

These use the JVM-wide allocated-byte counter; transfer spans gate release through worker teardown, before result validation. The scope includes harness/runtime/background allocations, not just channel objects.

| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |
|---:|---:|---:|---:|---:|
| 1 | 251531840 | 109.17 | 70180216 | 321712056 |
| 2 | 251394512 | 109.11 | 70180216 | 321574728 |
| 3 | 251482080 | 109.15 | 70180184 | 321662264 |
| 4 | 251466968 | 109.14 | 70180184 | 321647152 |
| 5 | 251271480 | 109.06 | 70180248 | 321451728 |
