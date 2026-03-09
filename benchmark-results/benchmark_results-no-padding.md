## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C buf=1024|1024|BoundedChannel|70,617 ms|3,289 ms|63,981 ms|76,894 ms|32,0%||
|1P1C buf=1024|1024|core.async|245,719 ms|75,142 ms|187,040 ms|416,704 ms|96,4%|3,5x|
|1P4C buf=1024|1024|BoundedChannel|129,656 ms|8,495 ms|119,479 ms|142,547 ms|48,5%||
|1P4C buf=1024|1024|core.async|325,235 ms|3,601 ms|320,929 ms|334,591 ms|1,6%|2,5x|
|4P1C buf=1024|1024|BoundedChannel|122,706 ms|1,804 ms|119,048 ms|125,764 ms|1,6%||
|4P1C buf=1024|1024|core.async|303,004 ms|29,187 ms|287,758 ms|371,318 ms|68,6%|2,5x|
|4P4C buf=1024|1024|BoundedChannel|163,838 ms|4,676 ms|154,277 ms|171,603 ms|15,8%||
|4P4C buf=1024|1024|core.async|241,535 ms|12,062 ms|234,758 ms|278,736 ms|35,3%|1,5x|
|Ping-pong|1|BoundedChannel|24,101 ms|0,309 ms|23,499 ms|24,596 ms|1,6%||
|Ping-pong|1|core.async|531,434 ms|5,586 ms|524,356 ms|544,777 ms|1,6%|22,1x|
|1P1C buf=1|1|BoundedChannel|199,617 ms|17,215 ms|169,446 ms|238,952 ms|63,5%||
|1P1C buf=1|1|core.async|1883,856 ms|9,208 ms|1871,715 ms|1903,019 ms|1,6%|9,4x|
|1P1C buf=16|16|BoundedChannel|136,687 ms|10,407 ms|113,004 ms|154,858 ms|56,8%||
|1P1C buf=16|16|core.async|407,228 ms|14,705 ms|379,347 ms|432,138 ms|22,2%|3,0x|
|4P4C buf=1|1|BoundedChannel|7259,131 ms|84,064 ms|7149,343 ms|7490,361 ms|1,6%||
|4P4C buf=1|1|core.async|3199,249 ms|69,757 ms|3162,847 ms|3313,607 ms|9,5%|0,4x|
|4P4C buf=16|16|BoundedChannel|180,934 ms|5,195 ms|172,221 ms|192,100 ms|15,8%||
|4P4C buf=16|16|core.async|1137,224 ms|14,595 ms|1123,305 ms|1169,211 ms|1,6%|6,3x|
|50×1P1C||BoundedChannel|41,510 ms|3,290 ms|38,285 ms|49,625 ms|58,5%||
|50×1P1C||core.async|588,102 ms|11,901 ms|569,701 ms|608,948 ms|9,4%|14,2x|
|50×4P4C||BoundedChannel|55,516 ms|0,862 ms|53,353 ms|56,998 ms|1,6%||
|50×4P4C||core.async|1869,738 ms|52,379 ms|1752,870 ms|1965,898 ms|15,7%|33,7x|
|Mixed (40 ch)||BoundedChannel|38,407 ms|1,059 ms|36,179 ms|40,120 ms|14,2%||
|Mixed (40 ch)||core.async|1063,102 ms|23,191 ms|1031,140 ms|1100,218 ms|9,5%|27,7x|
|200×1P1C||BoundedChannel|75,155 ms|2,156 ms|71,023 ms|78,678 ms|15,8%||
|200×1P1C||core.async|1909,893 ms|28,426 ms|1861,144 ms|1956,784 ms|1,6%|25,4x|
|XF map 1P1C|1024|BoundedChannel|102,559 ms|9,371 ms|94,608 ms|124,197 ms|65,3%||
|XF map 1P1C|1024|core.async|388,858 ms|15,736 ms|346,682 ms|416,629 ms|27,1%|3,8x|
|XF map 4P4C|1024|BoundedChannel|396,439 ms|10,752 ms|370,269 ms|417,951 ms|14,2%||
|XF map 4P4C|1024|core.async|433,149 ms|12,792 ms|425,955 ms|462,797 ms|15,8%|1,1x|
|XF filter 1P1C|1024|BoundedChannel|51,451 ms|1,418 ms|49,363 ms|55,189 ms|14,2%||
|XF filter 1P1C|1024|core.async|216,534 ms|8,041 ms|203,262 ms|230,568 ms|23,8%|4,2x|
|XF mapcat 1P1C|1024|BoundedChannel|67,198 ms|5,604 ms|58,526 ms|80,256 ms|61,8%||
|XF mapcat 1P1C|1024|core.async|219,728 ms|4,659 ms|210,233 ms|227,354 ms|9,4%|3,3x|
