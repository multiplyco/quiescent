## Benchmark Results

| label            | buffer | channel    | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|------------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | Quiescent  | 116,758 ms  | 16,568 ms  | 98,288 ms   | 134,528 ms  | 31,9%       | 1,9x    |
| 1P1C             | 1024   | core.async | 223,638 ms  | 32,292 ms  | 197,683 ms  | 267,048 ms  | 31,9%       | 1,0x    |
| 1P4C             | 1024   | Quiescent  | 106,674 ms  | 4,755 ms   | 101,565 ms  | 112,049 ms  | 13,9%       | 3,2x    |
| 1P4C             | 1024   | core.async | 338,503 ms  | 3,659 ms   | 334,367 ms  | 344,115 ms  | 13,9%       | 1,0x    |
| 4P1C             | 1024   | Quiescent  | 121,699 ms  | 7,007 ms   | 114,496 ms  | 130,185 ms  | 14,5%       | 2,5x    |
| 4P1C             | 1024   | core.async | 305,897 ms  | 3,349 ms   | 301,268 ms  | 309,440 ms  | 13,9%       | 1,0x    |
| 4P4C             | 1024   | Quiescent  | 155,063 ms  | 7,347 ms   | 147,645 ms  | 164,319 ms  | 13,9%       | 1,6x    |
| 4P4C             | 1024   | core.async | 248,144 ms  | 4,695 ms   | 239,937 ms  | 252,778 ms  | 13,9%       | 1,0x    |
| Ping-pong        | 1      | Quiescent  | 137,150 ms  | 12,997 ms  | 122,173 ms  | 152,269 ms  | 30,1%       | 3,9x    |
| Ping-pong        | 1      | core.async | 536,342 ms  | 2,804 ms   | 533,723 ms  | 540,324 ms  | 13,9%       | 1,0x    |
| 1P1C             | 1      | Quiescent  | 1678,164 ms | 95,202 ms  | 1588,707 ms | 1828,693 ms | 14,4%       | 1,2x    |
| 1P1C             | 1      | core.async | 1954,987 ms | 113,401 ms | 1869,393 ms | 2092,405 ms | 14,5%       | 1,0x    |
| 1P1C             | 16     | Quiescent  | 171,938 ms  | 33,250 ms  | 117,075 ms  | 201,931 ms  | 48,3%       | 2,6x    |
| 1P1C             | 16     | core.async | 452,775 ms  | 8,760 ms   | 442,602 ms  | 461,704 ms  | 13,9%       | 1,0x    |
| 4P4C             | 1      | Quiescent  | 4614,999 ms | 15,793 ms  | 4595,670 ms | 4635,229 ms | 13,9%       | 1,0x    |
| 4P4C             | 1      | core.async | 3065,611 ms | 21,458 ms  | 3033,763 ms | 3086,807 ms | 13,9%       | 1,5x    |
| 4P4C             | 16     | Quiescent  | 412,731 ms  | 12,084 ms  | 402,576 ms  | 432,222 ms  | 13,9%       | 2,7x    |
| 4P4C             | 16     | core.async | 1098,046 ms | 19,416 ms  | 1075,252 ms | 1119,295 ms | 13,9%       | 1,0x    |
| 50×1P1C          |        | Quiescent  | 39,765 ms   | 3,147 ms   | 35,945 ms   | 42,837 ms   | 15,5%       | 13,7x   |
| 50×1P1C          |        | core.async | 544,815 ms  | 18,250 ms  | 527,921 ms  | 571,960 ms  | 13,9%       | 1,0x    |
| 50×4P4C          |        | Quiescent  | 47,093 ms   | 2,441 ms   | 44,664 ms   | 49,901 ms   | 14,0%       | 40,1x   |
| 50×4P4C          |        | core.async | 1887,193 ms | 45,682 ms  | 1840,344 ms | 1946,775 ms | 13,9%       | 1,0x    |
| Mixed (40 ch)    |        | Quiescent  | 45,548 ms   | 2,577 ms   | 42,758 ms   | 49,086 ms   | 14,4%       | 47,7x   |
| Mixed (40 ch)    |        | core.async | 2173,690 ms | 30,749 ms  | 2135,318 ms | 2215,445 ms | 13,9%       | 1,0x    |
| 200×1P1C         |        | Quiescent  | 72,728 ms   | 2,640 ms   | 69,764 ms   | 75,511 ms   | 13,9%       | 26,3x   |
| 200×1P1C         |        | core.async | 1913,571 ms | 24,994 ms  | 1885,669 ms | 1941,634 ms | 13,9%       | 1,0x    |
| XF map 1P1C      | 1024   | Quiescent  | 114,288 ms  | 23,863 ms  | 87,319 ms   | 140,020 ms  | 64,0%       | 2,1x    |
| XF map 1P1C      | 1024   | core.async | 235,611 ms  | 44,057 ms  | 192,669 ms  | 301,473 ms  | 48,1%       | 1,0x    |
| XF map 4P4C      | 1024   | Quiescent  | 185,248 ms  | 2,279 ms   | 183,078 ms  | 188,512 ms  | 13,9%       | 2,2x    |
| XF map 4P4C      | 1024   | core.async | 401,327 ms  | 29,440 ms  | 362,291 ms  | 432,962 ms  | 15,3%       | 1,0x    |
| XF filter 1P1C   | 1024   | Quiescent  | 70,741 ms   | 16,556 ms  | 54,295 ms   | 94,002 ms   | 64,6%       | 2,4x    |
| XF filter 1P1C   | 1024   | core.async | 167,943 ms  | 14,218 ms  | 150,423 ms  | 185,417 ms  | 15,7%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | Quiescent  | 98,410 ms   | 7,355 ms   | 91,014 ms   | 108,885 ms  | 15,4%       | 2,9x    |
| XF mapcat 1P1C   | 1024   | core.async | 285,382 ms  | 18,248 ms  | 265,013 ms  | 304,935 ms  | 14,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | Quiescent  | 539,453 ms  | 24,128 ms  | 512,391 ms  | 571,665 ms  | 13,9%       | 8,1x    |
| Pipe 4P→1P→4C    | 16     | core.async | 4371,452 ms | 80,898 ms  | 4284,283 ms | 4458,133 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | Quiescent  | 568,707 ms  | 14,524 ms  | 552,224 ms  | 584,121 ms  | 13,9%       | 7,7x    |
| Pipe XF 4P→1P→4C | 16     | core.async | 4399,403 ms | 24,114 ms  | 4374,417 ms | 4425,164 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | Quiescent  | 278,979 ms  | 24,501 ms  | 258,888 ms  | 319,800 ms  | 15,7%       | 10,4x   |
| Pipe 4P→1P→4C    | 64     | core.async | 2907,342 ms | 103,821 ms | 2823,850 ms | 3071,549 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | Quiescent  | 306,511 ms  | 23,830 ms  | 278,996 ms  | 336,188 ms  | 15,5%       | 9,4x    |
| Pipe XF 4P→1P→4C | 64     | core.async | 2895,689 ms | 42,786 ms  | 2853,153 ms | 2954,451 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | Quiescent  | 211,130 ms  | 23,518 ms  | 182,675 ms  | 234,994 ms  | 31,0%       | 8,9x    |
| Pipe 4P→1P→4C    | 1024   | core.async | 1885,360 ms | 38,981 ms  | 1826,909 ms | 1927,997 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | Quiescent  | 226,582 ms  | 12,620 ms  | 210,342 ms  | 239,451 ms  | 14,3%       | 8,8x    |
| Pipe XF 4P→1P→4C | 1024   | core.async | 1997,786 ms | 27,225 ms  | 1966,200 ms | 2031,772 ms | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent  | 42,351 ms   | 3,812 ms   | 36,946 ms   | 45,303 ms   | 15,8%       | 40,5x   |
| 20×Pipe 4P→1P→4C |        | core.async | 1715,607 ms | 80,052 ms  | 1631,090 ms | 1821,566 ms | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent  | 37,945 ms   | 3,032 ms   | 35,172 ms   | 41,246 ms   | 15,5%       | 25,4x   |
| 20×Pipe 4P→1P→4C |        | core.async | 964,503 ms  | 650,743 ms | 263,643 ms  | 1861,212 ms | 82,9%       | 1,0x    |
| 16P1C            | 64     | Quiescent  | 190,699 ms  | 9,254 ms   | 178,864 ms  | 201,776 ms  | 13,9%       | 16,1x   |
| 16P1C            | 64     | core.async | 3063,488 ms | 27,972 ms  | 3030,017 ms | 3093,674 ms | 13,9%       | 1,0x    |
| 32P1C            | 64     | Quiescent  | 165,633 ms  | 4,929 ms   | 157,474 ms  | 169,974 ms  | 13,9%       | 31,8x   |
| 32P1C            | 64     | core.async | 5273,419 ms | 22,621 ms  | 5242,557 ms | 5298,520 ms | 13,9%       | 1,0x    |
| 64P1C            | 64     | Quiescent  | 169,617 ms  | 11,803 ms  | 155,202 ms  | 184,812 ms  | 15,2%       | 38,4x   |
| 64P1C            | 64     | core.async | 6520,425 ms | 103,850 ms | 6420,252 ms | 6656,648 ms | 13,9%       | 1,0x    |
| 128P1C           | 64     | Quiescent  | 176,124 ms  | 11,446 ms  | 167,003 ms  | 189,504 ms  | 15,0%       | 46,3x   |
| 128P1C           | 64     | core.async | 8160,727 ms | 112,612 ms | 8063,301 ms | 8342,217 ms | 13,9%       | 1,0x    |
| 16P1C            | 1024   | Quiescent  | 114,342 ms  | 6,936 ms   | 101,517 ms  | 120,654 ms  | 14,7%       | 24,1x   |
| 16P1C            | 1024   | core.async | 2758,008 ms | 82,483 ms  | 2652,316 ms | 2863,169 ms | 13,9%       | 1,0x    |
| 32P1C            | 1024   | Quiescent  | 113,688 ms  | 7,164 ms   | 104,860 ms  | 121,017 ms  | 14,8%       | 46,1x   |
| 32P1C            | 1024   | core.async | 5245,980 ms | 25,810 ms  | 5213,488 ms | 5271,032 ms | 13,9%       | 1,0x    |
| 64P1C            | 1024   | Quiescent  | 113,732 ms  | 9,085 ms   | 102,389 ms  | 122,496 ms  | 15,5%       | 56,5x   |
| 64P1C            | 1024   | core.async | 6422,839 ms | 52,812 ms  | 6358,347 ms | 6507,023 ms | 13,9%       | 1,0x    |
| 128P1C           | 1024   | Quiescent  | 110,987 ms  | 7,549 ms   | 102,615 ms  | 122,519 ms  | 15,1%       | 73,7x   |
| 128P1C           | 1024   | core.async | 8184,568 ms | 73,825 ms  | 8101,165 ms | 8271,203 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 64     | Quiescent  | 310,095 ms  | 11,779 ms  | 300,313 ms  | 329,874 ms  | 13,9%       | 11,4x   |
| XF 16P1C         | 64     | core.async | 3523,012 ms | 38,158 ms  | 3472,202 ms | 3556,878 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 64     | Quiescent  | 273,713 ms  | 9,035 ms   | 263,582 ms  | 283,605 ms  | 13,9%       | 21,4x   |
| XF 32P1C         | 64     | core.async | 5849,146 ms | 946,245 ms | 5313,211 ms | 7442,869 ms | 47,5%       | 1,0x    |
| XF 64P1C         | 64     | Quiescent  | 271,674 ms  | 8,955 ms   | 258,891 ms  | 281,229 ms  | 13,9%       | 24,1x   |
| XF 64P1C         | 64     | core.async | 6543,158 ms | 99,295 ms  | 6419,198 ms | 6634,467 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 64     | Quiescent  | 285,089 ms  | 11,076 ms  | 273,454 ms  | 300,775 ms  | 13,9%       | 29,7x   |
| XF 128P1C        | 64     | core.async | 8477,651 ms | 73,647 ms  | 8372,605 ms | 8540,156 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 1024   | Quiescent  | 223,683 ms  | 10,700 ms  | 215,365 ms  | 236,383 ms  | 13,9%       | 13,2x   |
| XF 16P1C         | 1024   | core.async | 2942,310 ms | 136,118 ms | 2715,630 ms | 3063,493 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 1024   | Quiescent  | 210,432 ms  | 6,924 ms   | 202,033 ms  | 221,448 ms  | 13,9%       | 24,5x   |
| XF 32P1C         | 1024   | core.async | 5152,126 ms | 137,636 ms | 4910,536 ms | 5286,691 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 1024   | Quiescent  | 204,222 ms  | 2,805 ms   | 201,787 ms  | 208,362 ms  | 13,9%       | 32,2x   |
| XF 64P1C         | 1024   | core.async | 6573,770 ms | 33,432 ms  | 6515,314 ms | 6602,025 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 1024   | Quiescent  | 211,002 ms  | 3,626 ms   | 206,827 ms  | 215,885 ms  | 13,9%       | 39,7x   |
| XF 128P1C        | 1024   | core.async | 8366,335 ms | 77,329 ms  | 8285,471 ms | 8452,034 ms | 13,9%       | 1,0x    |
