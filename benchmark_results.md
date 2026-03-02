## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C|1024|BoundedChannel|39,363 ms|2,018 ms|35,483 ms|43,233 ms|36,9%||
|1P1C|1024|Adaptive|98,193 ms|13,428 ms|88,361 ms|136,095 ms|80,7%|2,5x|
|1P1C|1024|Locked|119,962 ms|13,164 ms|98,646 ms|143,077 ms|73,8%|3,0x|
|1P1C|1024|core.async|210,863 ms|17,736 ms|181,888 ms|249,482 ms|61,9%|5,4x|
|1P4C|1024|BoundedChannel|66,016 ms|1,766 ms|62,514 ms|69,002 ms|14,2%||
|1P4C|1024|Adaptive|108,799 ms|1,027 ms|106,868 ms|110,787 ms|1,6%|1,6x|
|1P4C|1024|Locked|137,826 ms|4,432 ms|130,650 ms|142,997 ms|19,0%|2,1x|
|1P4C|1024|core.async|371,959 ms|126,269 ms|324,952 ms|602,151 ms|96,5%|5,6x|
|4P1C|1024|BoundedChannel|95,226 ms|5,134 ms|86,529 ms|105,603 ms|40,1%||
|4P1C|1024|Adaptive|127,697 ms|2,393 ms|121,861 ms|131,416 ms|7,8%|1,3x|
|4P1C|1024|Locked|149,829 ms|3,333 ms|143,929 ms|155,586 ms|11,0%|1,6x|
|4P1C|1024|core.async|353,656 ms|11,103 ms|347,796 ms|367,115 ms|18,9%|3,7x|
|4P4C|1024|BoundedChannel|134,175 ms|7,298 ms|124,544 ms|156,372 ms|40,1%||
|4P4C|1024|Adaptive|150,910 ms|4,127 ms|142,267 ms|160,346 ms|14,2%|1,1x|
|4P4C|1024|Locked|167,242 ms|4,453 ms|160,136 ms|176,674 ms|14,2%|1,2x|
|4P4C|1024|core.async|238,172 ms|7,174 ms|233,157 ms|255,672 ms|17,4%|1,8x|
|Ping-pong|1|BoundedChannel|19,299 ms|0,574 ms|18,863 ms|20,877 ms|17,3%||
|Ping-pong|1|Adaptive|145,634 ms|2,591 ms|140,688 ms|151,171 ms|7,8%|7,5x|
|Ping-pong|1|Locked|143,362 ms|9,459 ms|136,301 ms|166,824 ms|50,1%|7,4x|
|Ping-pong|1|core.async|550,410 ms|24,369 ms|532,623 ms|614,632 ms|30,3%|28,5x|
|1P1C|1|BoundedChannel|176,067 ms|12,175 ms|149,700 ms|197,417 ms|51,8%||
|1P1C|1|Adaptive|1341,579 ms|26,749 ms|1304,150 ms|1385,486 ms|7,9%|7,6x|
|1P1C|1|Locked|1286,940 ms|28,356 ms|1246,014 ms|1346,573 ms|9,5%|7,3x|
|1P1C|1|core.async|1927,253 ms|74,803 ms|1891,543 ms|2139,178 ms|25,4%|10,9x|
|1P1C|16|BoundedChannel|76,316 ms|11,199 ms|62,482 ms|97,364 ms|84,1%||
|1P1C|16|Adaptive|112,631 ms|3,606 ms|107,314 ms|119,574 ms|19,0%|1,5x|
|1P1C|16|Locked|192,567 ms|18,453 ms|159,147 ms|226,822 ms|68,6%|2,5x|
|1P1C|16|core.async|465,569 ms|11,584 ms|442,739 ms|483,295 ms|12,6%|6,1x|
|4P4C|1|BoundedChannel|7400,939 ms|111,047 ms|7282,263 ms|7658,330 ms|1,6%|4,1x|
|4P4C|1|Adaptive|1851,405 ms|73,168 ms|1781,915 ms|2101,412 ms|25,5%||
|4P4C|1|Locked|1818,172 ms|39,541 ms|1748,904 ms|1902,755 ms|9,4%||
|4P4C|1|core.async|3175,575 ms|30,806 ms|3151,275 ms|3218,908 ms|1,6%|1,7x|
|4P4C|16|BoundedChannel|162,020 ms|25,987 ms|149,291 ms|217,603 ms|85,9%||
|4P4C|16|Adaptive|464,372 ms|18,362 ms|445,737 ms|513,499 ms|25,5%|2,9x|
|4P4C|16|Locked|457,527 ms|14,280 ms|436,587 ms|484,180 ms|17,4%|2,8x|
|4P4C|16|core.async|1190,818 ms|26,516 ms|1170,870 ms|1253,210 ms|11,0%|7,3x|
|50×1P1C||BoundedChannel|38,068 ms|1,138 ms|36,106 ms|40,208 ms|17,4%|1,2x|
|50×1P1C||Adaptive|32,288 ms|1,521 ms|30,373 ms|35,836 ms|33,6%||
|50×1P1C||Locked|45,080 ms|1,562 ms|42,665 ms|47,914 ms|20,6%|1,4x|
|50×1P1C||core.async|589,525 ms|15,613 ms|566,795 ms|626,406 ms|14,2%|18,3x|
|50×4P4C||BoundedChannel|60,468 ms|3,911 ms|57,889 ms|74,369 ms|48,4%|1,1x|
|50×4P4C||Adaptive|54,846 ms|1,688 ms|52,999 ms|58,103 ms|17,4%||
|50×4P4C||Locked|58,813 ms|1,244 ms|56,139 ms|61,033 ms|9,4%|1,1x|
|50×4P4C||core.async|1946,293 ms|38,057 ms|1880,781 ms|2017,766 ms|7,9%|35,5x|
|Mixed (40 ch)||BoundedChannel|40,013 ms|0,867 ms|37,975 ms|41,309 ms|9,4%||
|Mixed (40 ch)||Adaptive|48,645 ms|1,340 ms|46,442 ms|51,127 ms|14,2%|1,2x|
|Mixed (40 ch)||Locked|51,529 ms|1,501 ms|48,630 ms|54,902 ms|15,8%|1,3x|
|Mixed (40 ch)||core.async|1185,456 ms|35,997 ms|1109,127 ms|1255,333 ms|17,4%|29,6x|
|200×1P1C||BoundedChannel|72,532 ms|1,778 ms|67,996 ms|74,973 ms|12,6%|1,4x|
|200×1P1C||Adaptive|51,550 ms|1,465 ms|49,040 ms|55,147 ms|15,8%||
|200×1P1C||Locked|75,518 ms|5,936 ms|70,460 ms|92,865 ms|58,5%|1,5x|
|200×1P1C||core.async|1941,002 ms|70,117 ms|1817,485 ms|2072,329 ms|22,2%|37,7x|
