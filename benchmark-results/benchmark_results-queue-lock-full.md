## Benchmark Results

| label            | buffer | channel    | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
  |------------------|--------|------------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | Quiescent  | 76,978 ms   | 5,274 ms   | 70,244 ms   | 91,047 ms   | 51,8%       | 2,7x    |
| 1P1C             | 1024   | core.async | 207,928 ms  | 16,455 ms  | 185,141 ms  | 249,012 ms  | 58,5%       | 1,0x    |
| 1P4C             | 1024   | Quiescent  | 93,483 ms   | 0,946 ms   | 91,539 ms   | 95,248 ms   | 1,6%        | 3,5x    |
| 1P4C             | 1024   | core.async | 326,755 ms  | 10,123 ms  | 318,477 ms  | 351,413 ms  | 17,4%       | 1,0x    |
| 4P1C             | 1024   | Quiescent  | 117,057 ms  | 7,458 ms   | 112,879 ms  | 142,415 ms  | 48,4%       | 3,0x    |
| 4P1C             | 1024   | core.async | 347,977 ms  | 4,995 ms   | 342,149 ms  | 359,128 ms  | 1,6%        | 1,0x    |
| 4P4C             | 1024   | Quiescent  | 135,132 ms  | 3,964 ms   | 127,876 ms  | 142,741 ms  | 15,8%       | 1,9x    |
| 4P4C             | 1024   | core.async | 253,068 ms  | 2,432 ms   | 250,670 ms  | 260,193 ms  | 1,6%        | 1,0x    |
| Ping-pong        | 1      | Quiescent  | 193,656 ms  | 4,894 ms   | 175,773 ms  | 201,277 ms  | 12,6%       | 2,9x    |
| Ping-pong        | 1      | core.async | 557,847 ms  | 43,289 ms  | 547,341 ms  | 561,770 ms  | 58,4%       | 1,0x    |
| 1P1C             | 1      | Quiescent  | 2204,131 ms | 85,755 ms  | 2095,937 ms | 2326,053 ms | 25,4%       | 1,0x    |
| 1P1C             | 1      | core.async | 1945,677 ms | 81,770 ms  | 1920,745 ms | 1956,526 ms | 28,7%       | 1,1x    |
| 1P1C             | 16     | Quiescent  | 232,271 ms  | 23,169 ms  | 202,796 ms  | 294,489 ms  | 70,3%       | 2,1x    |
| 1P1C             | 16     | core.async | 486,422 ms  | 24,139 ms  | 468,914 ms  | 492,683 ms  | 35,2%       | 1,0x    |
| 4P4C             | 1      | Quiescent  | 1829,719 ms | 57,751 ms  | 1720,737 ms | 1924,816 ms | 19,0%       | 1,7x    |
| 4P4C             | 1      | core.async | 3128,774 ms | 91,052 ms  | 3073,402 ms | 3337,884 ms | 15,8%       | 1,0x    |
| 4P4C             | 16     | Quiescent  | 432,996 ms  | 6,545 ms   | 421,629 ms  | 448,248 ms  | 1,6%        | 2,7x    |
| 4P4C             | 16     | core.async | 1161,569 ms | 63,892 ms  | 1128,736 ms | 1251,820 ms | 40,2%       | 1,0x    |
| 50×1P1C          |        | Quiescent  | 37,365 ms   | 1,103 ms   | 35,400 ms   | 39,274 ms   | 15,8%       | 15,3x   |
| 50×1P1C          |        | core.async | 570,157 ms  | 7,381 ms   | 556,073 ms  | 585,206 ms  | 1,6%        | 1,0x    |
| 50×4P4C          |        | Quiescent  | 54,847 ms   | 1,215 ms   | 52,568 ms   | 57,150 ms   | 10,9%       | 36,5x   |
| 50×4P4C          |        | core.async | 2001,649 ms | 29,991 ms  | 1932,810 ms | 2049,687 ms | 1,6%        | 1,0x    |
| Mixed (40 ch)    |        | Quiescent  | 46,999 ms   | 1,353 ms   | 44,410 ms   | 49,511 ms   | 15,8%       | 25,4x   |
| Mixed (40 ch)    |        | core.async | 1193,867 ms | 48,553 ms  | 1053,345 ms | 1266,502 ms | 27,1%       | 1,0x    |
| 200×1P1C         |        | Quiescent  | 67,681 ms   | 8,951 ms   | 62,566 ms   | 104,903 ms  | 80,7%       | 29,5x   |
| 200×1P1C         |        | core.async | 1996,111 ms | 30,147 ms  | 1928,625 ms | 2043,120 ms | 1,6%        | 1,0x    |
| XF map 1P1C      | 1024   | Quiescent  | 102,014 ms  | 19,118 ms  | 82,774 ms   | 161,204 ms  | 89,4%       | 2,6x    |
| XF map 1P1C      | 1024   | core.async | 264,280 ms  | 11,131 ms  | 247,717 ms  | 285,270 ms  | 28,7%       | 1,0x    |
| XF map 4P4C      | 1024   | Quiescent  | 207,252 ms  | 4,308 ms   | 197,154 ms  | 215,572 ms  | 9,4%        | 1,5x    |
| XF map 4P4C      | 1024   | core.async | 320,512 ms  | 0,901 ms   | 319,067 ms  | 322,123 ms  | 1,6%        | 1,0x    |
| XF filter 1P1C   | 1024   | Quiescent  | 55,007 ms   | 2,658 ms   | 51,742 ms   | 60,231 ms   | 35,2%       | 3,9x    |
| XF filter 1P1C   | 1024   | core.async | 214,512 ms  | 11,981 ms  | 191,658 ms  | 241,152 ms  | 41,8%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | Quiescent  | 75,287 ms   | 2,097 ms   | 71,750 ms   | 80,174 ms   | 15,7%       | 2,8x    |
| XF mapcat 1P1C   | 1024   | core.async | 212,587 ms  | 19,025 ms  | 199,954 ms  | 224,871 ms  | 65,2%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | Quiescent  | 603,849 ms  | 15,623 ms  | 583,314 ms  | 636,512 ms  | 12,6%       | 7,6x    |
| Pipe 4P→1P→4C    | 16     | core.async | 4567,624 ms | 120,186 ms | 4430,294 ms | 4936,063 ms | 14,2%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | Quiescent  | 672,442 ms  | 69,015 ms  | 487,309 ms  | 728,507 ms  | 70,4%       | 7,0x    |
| Pipe XF 4P→1P→4C | 16     | core.async | 4737,567 ms | 329,220 ms | 4532,235 ms | 5546,766 ms | 51,8%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | Quiescent  | 271,890 ms  | 9,135 ms   | 259,160 ms  | 287,884 ms  | 20,6%       | 11,3x   |
| Pipe 4P→1P→4C    | 64     | core.async | 3063,300 ms | 62,329 ms  | 3005,957 ms | 3244,880 ms | 9,4%        | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | Quiescent  | 303,110 ms  | 9,539 ms   | 283,393 ms  | 318,466 ms  | 18,9%       | 10,7x   |
| Pipe XF 4P→1P→4C | 64     | core.async | 3236,721 ms | 31,925 ms  | 3176,027 ms | 3303,679 ms | 1,6%        | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | Quiescent  | 171,131 ms  | 13,252 ms  | 160,337 ms  | 218,104 ms  | 58,4%       | 15,2x   |
| Pipe 4P→1P→4C    | 1024   | core.async | 2595,993 ms | 87,426 ms  | 2521,787 ms | 2691,978 ms | 20,6%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | Quiescent  | 208,091 ms  | 13,496 ms  | 188,675 ms  | 226,472 ms  | 48,5%       | 12,6x   |
| Pipe XF 4P→1P→4C | 1024   | core.async | 2618,993 ms | 63,346 ms  | 2511,151 ms | 2774,434 ms | 12,5%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent  | 39,883 ms   | 3,466 ms   | 37,470 ms   | 40,960 ms   | 63,5%       | 50,7x   |
| 20×Pipe 4P→1P→4C |        | core.async | 2020,500 ms | 89,581 ms  | 1845,419 ms | 2192,559 ms | 30,3%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent  | 33,295 ms   | 0,741 ms   | 32,048 ms   | 34,601 ms   | 11,0%       | 25,2x   |
| 20×Pipe 4P→1P→4C |        | core.async | 837,753 ms  | 203,386 ms | 409,133 ms  | 1158,196 ms | 94,6%       | 1,0x    |
| 16P1C            | 64     | Quiescent  | 171,902 ms  | 11,660 ms  | 158,657 ms  | 189,967 ms  | 51,7%       | 18,6x   |
| 16P1C            | 64     | core.async | 3197,648 ms | 43,145 ms  | 3140,138 ms | 3271,099 ms | 1,6%        | 1,0x    |
| 32P1C            | 64     | Quiescent  | 158,671 ms  | 2,689 ms   | 153,876 ms  | 164,602 ms  | 6,3%        | 33,7x   |
| 32P1C            | 64     | core.async | 5343,403 ms | 44,359 ms  | 5265,665 ms | 5430,261 ms | 1,6%        | 1,0x    |
| 64P1C            | 64     | Quiescent  | 159,227 ms  | 3,809 ms   | 153,972 ms  | 170,519 ms  | 11,0%       | 41,9x   |
| 64P1C            | 64     | core.async | 6664,012 ms | 169,178 ms | 6188,190 ms | 6866,030 ms | 12,6%       | 1,0x    |
| 128P1C           | 64     | Quiescent  | 166,495 ms  | 4,427 ms   | 158,447 ms  | 174,611 ms  | 14,2%       | 49,6x   |
| 128P1C           | 64     | core.async | 8255,984 ms | 107,436 ms | 8010,947 ms | 8459,478 ms | 1,6%        | 1,0x    |
| 16P1C            | 1024   | Quiescent  | 110,429 ms  | 1,482 ms   | 107,618 ms  | 113,025 ms  | 1,6%        | 21,3x   |
| 16P1C            | 1024   | core.async | 2355,212 ms | 111,381 ms | 1997,459 ms | 2484,849 ms | 33,6%       | 1,0x    |
| 32P1C            | 1024   | Quiescent  | 107,987 ms  | 1,223 ms   | 105,828 ms  | 110,167 ms  | 1,6%        | 49,5x   |
| 32P1C            | 1024   | core.async | 5349,828 ms | 197,189 ms | 5275,019 ms | 5446,422 ms | 23,8%       | 1,0x    |
| 64P1C            | 1024   | Quiescent  | 109,756 ms  | 1,465 ms   | 107,042 ms  | 112,544 ms  | 1,6%        | 59,7x   |
| 64P1C            | 1024   | core.async | 6554,218 ms | 50,135 ms  | 6482,637 ms | 6649,275 ms | 1,6%        | 1,0x    |
| 128P1C           | 1024   | Quiescent  | 103,834 ms  | 1,444 ms   | 101,659 ms  | 107,026 ms  | 1,6%        | 80,0x   |
| 128P1C           | 1024   | core.async | 8304,462 ms | 76,389 ms  | 8124,474 ms | 8454,906 ms | 1,6%        | 1,0x    |
| XF 16P1C         | 64     | Quiescent  | 285,415 ms  | 7,623 ms   | 273,974 ms  | 301,669 ms  | 14,2%       | 11,2x   |
| XF 16P1C         | 64     | core.async | 3192,549 ms | 36,644 ms  | 3136,240 ms | 3271,382 ms | 1,6%        | 1,0x    |
| XF 32P1C         | 64     | Quiescent  | 283,125 ms  | 6,153 ms   | 273,711 ms  | 294,815 ms  | 9,4%        | 18,8x   |
| XF 32P1C         | 64     | core.async | 5325,337 ms | 86,171 ms  | 5221,775 ms | 5532,445 ms | 2,2%        | 1,0x    |
| XF 64P1C         | 64     | Quiescent  | 288,939 ms  | 5,046 ms   | 281,928 ms  | 299,494 ms  | 6,3%        | 22,2x   |
| XF 64P1C         | 64     | core.async | 6421,052 ms | 142,820 ms | 6148,699 ms | 6641,213 ms | 11,0%       | 1,0x    |
| XF 128P1C        | 64     | Quiescent  | 284,801 ms  | 6,722 ms   | 273,885 ms  | 298,769 ms  | 11,0%       | 28,6x   |
| XF 128P1C        | 64     | core.async | 8136,740 ms | 89,378 ms  | 8035,064 ms | 8325,776 ms | 1,6%        | 1,0x    |
| XF 16P1C         | 1024   | Quiescent  | 228,309 ms  | 11,329 ms  | 217,639 ms  | 253,138 ms  | 35,2%       | 10,6x   |
| XF 16P1C         | 1024   | core.async | 2414,177 ms | 97,751 ms  | 2292,453 ms | 2573,075 ms | 27,1%       | 1,0x    |
| XF 32P1C         | 1024   | Quiescent  | 223,797 ms  | 4,477 ms   | 213,668 ms  | 231,524 ms  | 7,9%        | 23,6x   |
| XF 32P1C         | 1024   | core.async | 5285,565 ms | 33,587 ms  | 5216,250 ms | 5347,247 ms | 1,6%        | 1,0x    |
| XF 64P1C         | 1024   | Quiescent  | 227,863 ms  | 3,364 ms   | 221,835 ms  | 233,843 ms  | 1,6%        | 28,1x   |
| XF 64P1C         | 1024   | core.async | 6401,327 ms | 62,468 ms  | 6314,768 ms | 6505,555 ms | 1,6%        | 1,0x    |
| XF 128P1C        | 1024   | Quiescent  | 241,780 ms  | 6,825 ms   | 230,835 ms  | 255,198 ms  | 15,8%       | 33,3x   |
| XF 128P1C        | 1024   | core.async | 8042,089 ms | 124,448 ms | 7848,525 ms | 8275,449 ms | 1,6%        | 1,0x    |
|50×1P1C||Quiescent|45,108 ms|5,312 ms|38,113 ms|51,479 ms|31,2%|13,9x|
|50×1P1C||core.async|625,337 ms|28,357 ms|590,639 ms|655,380 ms|13,9%|1,0x|
