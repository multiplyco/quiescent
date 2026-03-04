## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C|1024|Quiescent|110,705 ms|9,605 ms|100,728 ms|121,393 ms|15,7%|2,0x|
|1P1C|1024|core.async|224,364 ms|58,975 ms|183,769 ms|318,913 ms|65,0%|1,0x|
