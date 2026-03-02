## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|XF map 1P1C|1024|BoundedChannel|110,825 ms|5,024 ms|103,879 ms|116,392 ms|13,9%||
|XF map 1P1C|1024|Adaptive|164,982 ms|6,559 ms|156,417 ms|171,787 ms|13,9%|1,5x|
|XF map 1P1C|1024|Locked|166,990 ms|38,927 ms|113,195 ms|206,482 ms|64,5%|1,5x|
|XF map 4P4C|1024|BoundedChannel|335,803 ms|23,195 ms|306,164 ms|363,935 ms|15,2%|1,4x|
|XF map 4P4C|1024|Adaptive|246,775 ms|14,373 ms|227,351 ms|263,022 ms|14,5%||
|XF map 4P4C|1024|Locked|261,954 ms|22,865 ms|241,250 ms|287,214 ms|15,7%|1,1x|
|XF filter 1P1C|1024|BoundedChannel|48,466 ms|4,967 ms|43,956 ms|54,240 ms|30,6%||
|XF filter 1P1C|1024|Adaptive|75,605 ms|3,259 ms|72,640 ms|80,950 ms|13,9%|1,6x|
|XF filter 1P1C|1024|Locked|139,753 ms|44,481 ms|84,670 ms|184,423 ms|81,5%|2,9x|
|XF mapcat 1P1C|1024|BoundedChannel|78,232 ms|6,374 ms|72,059 ms|85,249 ms|15,6%||
|XF mapcat 1P1C|1024|Adaptive|99,096 ms|17,273 ms|80,837 ms|120,490 ms|47,9%|1,3x|
|XF mapcat 1P1C|1024|Locked|161,778 ms|29,355 ms|134,712 ms|198,786 ms|48,0%|2,1x|
