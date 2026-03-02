## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|XF 16P1C|64|BoundedChannel|240,266 ms|5,297 ms|230,923 ms|248,010 ms|9,5%||
|XF 16P1C|64|core.async|3019,446 ms|136,982 ms|2885,659 ms|3433,426 ms|31,9%|12,6x|
|XF 32P1C|64|BoundedChannel|255,870 ms|4,501 ms|248,605 ms|263,171 ms|6,3%||
|XF 32P1C|64|core.async|4792,402 ms|240,919 ms|4011,634 ms|5044,958 ms|36,8%|18,7x|
|XF 64P1C|64|BoundedChannel|258,530 ms|5,143 ms|249,510 ms|267,518 ms|7,9%||
|XF 64P1C|64|core.async|5948,690 ms|73,698 ms|5840,509 ms|6085,499 ms|1,6%|23,0x|
|XF 128P1C|64|BoundedChannel|262,929 ms|9,028 ms|249,349 ms|278,130 ms|20,6%||
|XF 128P1C|64|core.async|7368,055 ms|449,665 ms|5685,792 ms|7625,305 ms|45,1%|28,0x|
|XF 16P1C|1024|BoundedChannel|179,238 ms|4,491 ms|170,260 ms|189,855 ms|12,6%||
|XF 16P1C|1024|core.async|2048,423 ms|74,276 ms|1906,116 ms|2218,674 ms|22,3%|11,4x|
|XF 32P1C|1024|BoundedChannel|174,451 ms|13,133 ms|127,294 ms|183,640 ms|56,8%||
|XF 32P1C|1024|core.async|4702,042 ms|315,867 ms|3757,484 ms|5023,443 ms|50,1%|27,0x|
|XF 64P1C|1024|BoundedChannel|180,464 ms|3,129 ms|174,893 ms|186,771 ms|6,3%||
|XF 64P1C|1024|core.async|5726,852 ms|362,113 ms|4311,059 ms|6066,833 ms|46,8%|31,7x|
|XF 128P1C|1024|BoundedChannel|179,592 ms|3,955 ms|170,665 ms|187,521 ms|9,5%||
|XF 128P1C|1024|core.async|7284,324 ms|519,210 ms|5889,897 ms|7640,206 ms|53,4%|40,6x|
