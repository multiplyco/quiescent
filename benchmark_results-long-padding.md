## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C|1024|BoundedChannel|30,408 ms|2,043 ms|26,729 ms|33,686 ms|50,1%||
|1P1C|1024|core.async|209,307 ms|15,659 ms|186,837 ms|243,246 ms|56,8%|2,3x|
|1P4C|1024|BoundedChannel|73,338 ms|4,227 ms|71,278 ms|80,937 ms|43,4%||
|1P4C|1024|core.async|311,362 ms|3,630 ms|305,989 ms|322,265 ms|1,6%|4,2x|
|4P1C|1024|BoundedChannel|94,972 ms|3,494 ms|88,240 ms|101,401 ms|23,8%||
|4P1C|1024|core.async|327,548 ms|2,946 ms|323,919 ms|333,017 ms|1,6%|3,4x|
|4P4C|1024|BoundedChannel|130,087 ms|2,250 ms|126,006 ms|134,652 ms|6,3%||
|4P4C|1024|core.async|234,241 ms|7,162 ms|229,933 ms|247,640 ms|17,4%|1,3x|
|Ping-pong|1|BoundedChannel|19,015 ms|0,637 ms|17,875 ms|19,584 ms|20,6%||
|Ping-pong|1|core.async|528,366 ms|5,219 ms|521,579 ms|538,544 ms|1,6%|27,8x|
|1P1C|1|BoundedChannel|199,157 ms|16,489 ms|162,941 ms|229,040 ms|60,2%||
|1P1C|1|core.async|1929,275 ms|15,473 ms|1911,704 ms|1965,665 ms|1,6%|21,6x|
|1P1C|16|BoundedChannel|89,434 ms|8,992 ms|74,478 ms|106,580 ms|70,3%||
|1P1C|16|core.async|476,701 ms|8,306 ms|459,347 ms|491,425 ms|6,3%|5,3x|
|4P4C|1|BoundedChannel|7213,537 ms|68,625 ms|7042,439 ms|7306,559 ms|1,6%||
|4P4C|1|core.async|3229,932 ms|81,390 ms|3152,184 ms|3392,475 ms|12,6%|17,7x|
|4P4C|16|BoundedChannel|182,592 ms|7,574 ms|161,972 ms|194,150 ms|27,1%||
|4P4C|16|core.async|1207,319 ms|20,210 ms|1193,267 ms|1259,281 ms|6,3%|6,6x|
|50×1P1C||BoundedChannel|32,341 ms|1,064 ms|30,906 ms|34,956 ms|19,0%||
|50×1P1C||core.async|574,343 ms|8,618 ms|551,678 ms|588,321 ms|1,6%|17,8x|
|50×4P4C||BoundedChannel|59,843 ms|2,241 ms|57,419 ms|67,609 ms|23,8%||
|50×4P4C||core.async|1890,819 ms|41,600 ms|1815,349 ms|1973,155 ms|9,5%|31,6x|
|Mixed (40 ch)||BoundedChannel|42,039 ms|1,138 ms|39,845 ms|44,281 ms|14,2%||
|Mixed (40 ch)||core.async|1150,462 ms|42,439 ms|1105,414 ms|1232,650 ms|23,8%|27,4x|
|200×1P1C||BoundedChannel|68,343 ms|1,764 ms|65,565 ms|72,474 ms|12,6%||
|200×1P1C||core.async|2003,685 ms|20,437 ms|1962,050 ms|2037,558 ms|1,6%|29,3x|
|XF map 1P1C|1024|BoundedChannel|98,645 ms|2,308 ms|92,074 ms|101,750 ms|11,0%||
|XF map 1P1C|1024|core.async|422,247 ms|22,801 ms|383,717 ms|451,002 ms|40,1%|4,3x|
|XF map 4P4C|1024|BoundedChannel|284,342 ms|13,366 ms|251,226 ms|314,016 ms|33,6%||
|XF map 4P4C|1024|core.async|382,521 ms|9,084 ms|373,114 ms|403,338 ms|11,0%|1,3x|
|XF filter 1P1C|1024|BoundedChannel|43,444 ms|0,855 ms|42,696 ms|45,021 ms|7,9%||
|XF filter 1P1C|1024|core.async|221,434 ms|5,810 ms|212,508 ms|232,545 ms|14,1%|5,1x|
|XF mapcat 1P1C|1024|BoundedChannel|60,034 ms|1,533 ms|57,988 ms|64,752 ms|12,6%||
|XF mapcat 1P1C|1024|core.async|243,195 ms|4,154 ms|234,545 ms|249,751 ms|6,3%|4,1x|
