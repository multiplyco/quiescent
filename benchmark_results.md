## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C|1024|BoundedChannel|31,776 ms|2,666 ms|25,567 ms|35,717 ms|61,8%||
|1P1C|1024|core.async|211,124 ms|20,864 ms|178,016 ms|251,973 ms|68,7%|6,6x|
|1P4C|1024|BoundedChannel|67,964 ms|1,547 ms|65,322 ms|71,345 ms|11,0%||
|1P4C|1024|core.async|316,825 ms|2,893 ms|312,265 ms|323,059 ms|1,6%|4,7x|
|4P1C|1024|BoundedChannel|95,790 ms|4,759 ms|86,163 ms|103,500 ms|35,2%||
|4P1C|1024|core.async|323,820 ms|2,687 ms|318,983 ms|329,847 ms|1,6%|3,4x|
|4P4C|1024|BoundedChannel|131,116 ms|6,554 ms|122,985 ms|148,272 ms|35,3%||
|4P4C|1024|core.async|239,172 ms|5,396 ms|234,686 ms|250,442 ms|11,0%|1,8x|
|Ping-pong|1|BoundedChannel|19,077 ms|0,209 ms|18,762 ms|19,464 ms|1,6%||
|Ping-pong|1|core.async|539,178 ms|35,658 ms|524,167 ms|654,098 ms|50,1%|28,3x|
|1P1C|1|BoundedChannel|168,587 ms|10,070 ms|150,321 ms|185,513 ms|45,1%||
|1P1C|1|core.async|1902,256 ms|81,097 ms|1866,977 ms|2166,035 ms|28,7%|11,3x|
|1P1C|16|BoundedChannel|79,646 ms|7,871 ms|65,412 ms|95,810 ms|68,7%||
|1P1C|16|core.async|460,010 ms|16,056 ms|424,276 ms|482,749 ms|22,2%|5,8x|
|4P4C|1|BoundedChannel|7368,109 ms|68,047 ms|7264,233 ms|7536,844 ms|1,6%||
|4P4C|1|core.async|3161,803 ms|70,054 ms|3120,947 ms|3383,380 ms|10,9%|0,4x|
|4P4C|16|BoundedChannel|157,021 ms|9,136 ms|150,946 ms|166,188 ms|43,4%||
|4P4C|16|core.async|1185,108 ms|17,133 ms|1171,942 ms|1197,401 ms|1,6%|7,5x|
|50×1P1C||BoundedChannel|33,988 ms|1,013 ms|32,013 ms|36,008 ms|17,3%||
|50×1P1C||core.async|583,800 ms|32,162 ms|559,888 ms|665,721 ms|40,2%|17,2x|
|50×4P4C||BoundedChannel|56,987 ms|1,019 ms|54,899 ms|58,862 ms|7,8%||
|50×4P4C||core.async|1928,705 ms|36,202 ms|1854,510 ms|2007,064 ms|7,8%|33,8x|
|Mixed (40 ch)||BoundedChannel|38,625 ms|3,141 ms|36,025 ms|45,690 ms|60,2%||
|Mixed (40 ch)||core.async|1159,747 ms|17,136 ms|1122,215 ms|1189,483 ms|1,6%|30,0x|
|200×1P1C||BoundedChannel|65,634 ms|1,431 ms|63,147 ms|68,336 ms|9,5%||
|200×1P1C||core.async|1991,656 ms|28,635 ms|1948,223 ms|2031,289 ms|1,6%|30,3x|
|XF map 1P1C|1024|BoundedChannel|102,568 ms|2,230 ms|96,473 ms|107,239 ms|9,4%||
|XF map 1P1C|1024|core.async|307,617 ms|31,423 ms|270,442 ms|386,166 ms|70,4%|3,0x|
|XF map 4P4C|1024|BoundedChannel|324,664 ms|10,720 ms|303,007 ms|342,838 ms|19,0%||
|XF map 4P4C|1024|core.async|420,297 ms|11,017 ms|412,095 ms|455,893 ms|14,1%|1,3x|
|XF filter 1P1C|1024|BoundedChannel|43,545 ms|4,057 ms|41,491 ms|54,068 ms|66,9%||
|XF filter 1P1C|1024|core.async|225,975 ms|5,150 ms|215,480 ms|236,730 ms|11,0%|5,2x|
|XF mapcat 1P1C|1024|BoundedChannel|58,044 ms|1,394 ms|56,044 ms|60,386 ms|11,1%||
|XF mapcat 1P1C|1024|core.async|243,688 ms|12,694 ms|232,392 ms|274,005 ms|38,5%|4,2x|
