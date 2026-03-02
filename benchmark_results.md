## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C|1024|BoundedChannel|40,784 ms|3,509 ms|36,288 ms|48,121 ms|63,5%||
|1P1C|1024|Locked|116,463 ms|10,575 ms|99,748 ms|136,876 ms|65,3%|2,9x|
|1P1C|1024|core.async|217,137 ms|17,688 ms|190,842 ms|258,719 ms|60,2%|5,3x|
|1P4C|1024|BoundedChannel|71,475 ms|1,767 ms|67,511 ms|74,606 ms|12,6%||
|1P4C|1024|Locked|136,091 ms|3,505 ms|129,256 ms|141,633 ms|12,6%|1,9x|
|1P4C|1024|core.async|377,704 ms|5,615 ms|370,868 ms|387,732 ms|1,6%|5,3x|
|4P1C|1024|BoundedChannel|97,014 ms|4,791 ms|86,092 ms|104,775 ms|35,2%||
|4P1C|1024|Locked|144,848 ms|9,652 ms|136,054 ms|168,373 ms|50,1%|1,5x|
|4P1C|1024|core.async|441,246 ms|8,165 ms|431,580 ms|458,458 ms|7,8%|4,5x|
|4P4C|1024|BoundedChannel|129,761 ms|3,410 ms|123,565 ms|135,994 ms|14,2%||
|4P4C|1024|Locked|162,856 ms|5,388 ms|154,972 ms|172,155 ms|20,5%|1,3x|
|4P4C|1024|core.async|237,572 ms|2,243 ms|234,497 ms|240,984 ms|1,6%|1,8x|
|Ping-pong|1|BoundedChannel|19,080 ms|0,192 ms|18,813 ms|19,524 ms|1,6%||
|Ping-pong|1|Locked|144,544 ms|4,803 ms|139,455 ms|156,033 ms|20,6%|7,6x|
|Ping-pong|1|core.async|545,257 ms|4,339 ms|540,956 ms|555,871 ms|1,6%|28,6x|
|1P1C|1|BoundedChannel|164,325 ms|10,307 ms|147,029 ms|185,581 ms|46,8%||
|1P1C|1|Locked|1274,140 ms|26,705 ms|1233,947 ms|1326,784 ms|9,4%|7,8x|
|1P1C|1|core.async|1895,968 ms|122,762 ms|1840,785 ms|2079,922 ms|48,4%|11,5x|
|1P1C|16|BoundedChannel|85,857 ms|12,223 ms|61,377 ms|108,287 ms|82,4%||
|1P1C|16|Locked|178,723 ms|17,628 ms|148,111 ms|212,669 ms|68,7%|2,1x|
|1P1C|16|core.async|448,397 ms|16,877 ms|434,172 ms|506,420 ms|23,9%|5,2x|
|4P4C|1|BoundedChannel|7435,601 ms|151,234 ms|7303,734 ms|7848,832 ms|9,4%|4,0x|
|4P4C|1|Locked|1881,444 ms|48,244 ms|1815,541 ms|2002,926 ms|12,6%||
|4P4C|1|core.async|3046,766 ms|36,804 ms|3016,522 ms|3100,638 ms|1,6%|1,6x|
|4P4C|16|BoundedChannel|159,021 ms|17,904 ms|150,113 ms|191,479 ms|73,8%||
|4P4C|16|Locked|459,384 ms|18,949 ms|436,586 ms|510,931 ms|27,1%|2,9x|
|4P4C|16|core.async|1167,710 ms|13,226 ms|1152,981 ms|1193,455 ms|1,6%|7,3x|
|50×1P1C||BoundedChannel|39,637 ms|1,419 ms|36,926 ms|41,716 ms|22,2%||
|50×1P1C||Locked|45,504 ms|1,321 ms|43,142 ms|48,217 ms|15,8%|1,1x|
|50×1P1C||core.async|601,319 ms|9,745 ms|585,398 ms|620,529 ms|2,5%|15,2x|
|50×4P4C||BoundedChannel|62,879 ms|2,901 ms|60,362 ms|68,207 ms|32,0%||
|50×4P4C||Locked|59,909 ms|1,402 ms|57,537 ms|62,797 ms|11,0%||
|50×4P4C||core.async|1975,919 ms|34,493 ms|1914,844 ms|2042,718 ms|6,3%|33,0x|
|Mixed (40 ch)||BoundedChannel|44,242 ms|3,480 ms|41,257 ms|53,591 ms|58,5%||
|Mixed (40 ch)||Locked|53,403 ms|1,364 ms|50,536 ms|55,555 ms|12,6%|1,2x|
|Mixed (40 ch)||core.async|1176,693 ms|38,637 ms|1084,597 ms|1235,641 ms|19,0%|26,6x|
|200×1P1C||BoundedChannel|73,914 ms|5,861 ms|66,837 ms|87,518 ms|58,5%||
|200×1P1C||Locked|77,862 ms|1,378 ms|75,705 ms|80,887 ms|6,3%|1,1x|
|200×1P1C||core.async|1979,660 ms|33,882 ms|1907,727 ms|2032,470 ms|6,3%|26,8x|
