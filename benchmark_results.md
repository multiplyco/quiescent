## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C|1024|Quiescent|123,608 ms|6,188 ms|116,572 ms|132,505 ms|13,9%|1,8x|
|1P1C|1024|core.async|217,660 ms|39,540 ms|178,918 ms|276,919 ms|48,0%|1,0x|
|1P4C|1024|Quiescent|1106,213 ms|41,603 ms|1063,846 ms|1171,664 ms|13,9%|1,0x|
|1P4C|1024|core.async|345,054 ms|4,073 ms|339,551 ms|349,380 ms|13,9%|3,2x|
