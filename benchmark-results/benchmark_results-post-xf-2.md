## Benchmark Results

|label|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|
|1P1C buf=1024|BoundedChannel|21,249 ms|0,902 ms|19,750 ms|22,834 ms|28,7%||
|1P1C buf=1024|core.async|207,713 ms|15,337 ms|181,829 ms|233,885 ms|55,1%|9,8x|
|1P4C buf=1024|BoundedChannel|57,170 ms|7,171 ms|52,359 ms|66,430 ms|78,9%||
|1P4C buf=1024|core.async|319,627 ms|4,174 ms|314,269 ms|331,157 ms|1,6%|5,6x|
|4P1C buf=1024|BoundedChannel|64,402 ms|2,029 ms|60,944 ms|68,278 ms|19,0%||
|4P1C buf=1024|core.async|359,835 ms|16,194 ms|350,065 ms|390,298 ms|31,9%|5,6x|
|4P4C buf=1024|BoundedChannel|101,459 ms|3,992 ms|93,358 ms|108,686 ms|25,5%||
|4P4C buf=1024|core.async|232,745 ms|2,153 ms|229,607 ms|237,177 ms|1,6%|2,3x|
|Ping-pong|BoundedChannel|33,273 ms|6,752 ms|19,957 ms|41,177 ms|91,1%||
|Ping-pong|core.async|550,993 ms|5,686 ms|546,653 ms|566,855 ms|1,6%|16,6x|
|1P1C buf=1|BoundedChannel|259,683 ms|28,471 ms|196,838 ms|300,596 ms|73,8%||
|1P1C buf=1|core.async|1944,095 ms|14,259 ms|1928,877 ms|1981,520 ms|1,6%|7,5x|
|1P1C buf=16|BoundedChannel|128,100 ms|14,851 ms|70,098 ms|146,163 ms|75,5%||
|1P1C buf=16|core.async|466,740 ms|17,370 ms|437,685 ms|518,513 ms|23,8%|3,6x|
|4P4C buf=1|BoundedChannel|7304,629 ms|552,694 ms|7088,160 ms|8771,943 ms|56,8%||
|4P4C buf=1|core.async|3154,054 ms|47,048 ms|3120,965 ms|3282,363 ms|1,6%|0,4x|
|4P4C buf=16|BoundedChannel|114,754 ms|3,976 ms|106,887 ms|124,745 ms|20,6%||
|4P4C buf=16|core.async|1138,387 ms|5,708 ms|1131,648 ms|1148,797 ms|1,6%|9,9x|
|50×1P1C|BoundedChannel|39,565 ms|1,569 ms|37,159 ms|44,285 ms|25,5%||
|50×1P1C|core.async|596,313 ms|8,702 ms|579,977 ms|615,089 ms|1,6%|15,1x|
|50×4P4C|BoundedChannel|62,834 ms|1,034 ms|60,771 ms|64,448 ms|5,4%||
|50×4P4C|core.async|1972,294 ms|35,827 ms|1898,109 ms|2031,544 ms|7,8%|31,4x|
|Mixed (40 ch)|BoundedChannel|48,831 ms|1,951 ms|46,177 ms|52,060 ms|27,0%||
|Mixed (40 ch)|core.async|1181,633 ms|16,461 ms|1137,817 ms|1212,625 ms|1,6%|24,2x|
|200×1P1C|BoundedChannel|75,636 ms|8,435 ms|70,459 ms|91,836 ms|73,8%||
|200×1P1C|core.async|2006,713 ms|23,770 ms|1956,464 ms|2047,125 ms|1,6%|26,5x|
|XF map 1P1C|BoundedChannel|56,840 ms|3,174 ms|51,263 ms|62,322 ms|41,8%||
|XF map 1P1C|core.async|325,565 ms|9,972 ms|306,165 ms|341,565 ms|17,4%|5,7x|
|XF map 4P4C|BoundedChannel|234,480 ms|35,949 ms|170,611 ms|310,234 ms|84,2%||
|XF map 4P4C|core.async|421,890 ms|6,026 ms|415,593 ms|435,604 ms|1,6%|1,8x|
|XF filter 1P1C|BoundedChannel|34,860 ms|0,624 ms|34,017 ms|36,401 ms|7,8%||
|XF filter 1P1C|core.async|217,120 ms|6,405 ms|206,239 ms|227,896 ms|15,8%|6,2x|
|XF mapcat 1P1C|BoundedChannel|42,004 ms|1,942 ms|38,906 ms|46,287 ms|32,0%||
|XF mapcat 1P1C|core.async|220,898 ms|4,296 ms|214,736 ms|229,031 ms|7,9%|5,3x|
