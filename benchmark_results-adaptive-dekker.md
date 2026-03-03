## Benchmark Results

| label            | buffer | channel        | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|----------------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | BoundedChannel | 36,381 ms   | 9,942 ms   | 18,188 ms   | 45,176 ms   | 65,1%       |         |
| 1P1C             | 1024   | Adaptive       | 99,955 ms   | 20,989 ms  | 82,736 ms   | 133,236 ms  | 64,0%       | 2,7x    |
| 1P1C             | 1024   | Locked         | 119,149 ms  | 44,112 ms  | 83,133 ms   | 174,393 ms  | 82,0%       | 3,3x    |
| 1P1C             | 1024   | core.async     | 214,975 ms  | 25,307 ms  | 190,804 ms  | 255,316 ms  | 31,2%       | 5,9x    |
| 1P4C             | 1024   | BoundedChannel | 71,351 ms   | 1,972 ms   | 67,822 ms   | 73,003 ms   | 13,9%       |         |
| 1P4C             | 1024   | Adaptive       | 96,567 ms   | 3,417 ms   | 93,496 ms   | 100,367 ms  | 13,9%       | 1,4x    |
| 1P4C             | 1024   | Locked         | 138,552 ms  | 11,308 ms  | 122,461 ms  | 150,205 ms  | 15,6%       | 1,9x    |
| 1P4C             | 1024   | core.async     | 320,736 ms  | 6,067 ms   | 315,269 ms  | 330,399 ms  | 13,9%       | 4,5x    |
| 4P1C             | 1024   | BoundedChannel | 99,121 ms   | 11,674 ms  | 85,191 ms   | 112,133 ms  | 31,2%       |         |
| 4P1C             | 1024   | Adaptive       | 124,111 ms  | 9,248 ms   | 112,933 ms  | 136,959 ms  | 15,4%       | 1,3x    |
| 4P1C             | 1024   | Locked         | 146,684 ms  | 5,969 ms   | 140,741 ms  | 155,761 ms  | 13,9%       | 1,5x    |
| 4P1C             | 1024   | core.async     | 321,633 ms  | 1,522 ms   | 320,011 ms  | 323,829 ms  | 13,9%       | 3,2x    |
| 4P4C             | 1024   | BoundedChannel | 124,236 ms  | 10,586 ms  | 113,399 ms  | 136,239 ms  | 15,7%       |         |
| 4P4C             | 1024   | Adaptive       | 146,962 ms  | 21,925 ms  | 117,724 ms  | 167,221 ms  | 47,1%       | 1,2x    |
| 4P4C             | 1024   | Locked         | 175,731 ms  | 12,286 ms  | 162,054 ms  | 194,860 ms  | 15,2%       | 1,4x    |
| 4P4C             | 1024   | core.async     | 252,334 ms  | 7,720 ms   | 244,546 ms  | 264,587 ms  | 13,9%       | 2,0x    |
| Ping-pong        | 1      | BoundedChannel | 19,209 ms   | 0,424 ms   | 18,797 ms   | 19,670 ms   | 13,9%       |         |
| Ping-pong        | 1      | Adaptive       | 146,073 ms  | 2,746 ms   | 141,452 ms  | 149,147 ms  | 13,9%       | 7,6x    |
| Ping-pong        | 1      | Locked         | 149,986 ms  | 2,620 ms   | 146,832 ms  | 152,516 ms  | 13,9%       | 7,8x    |
| Ping-pong        | 1      | core.async     | 571,489 ms  | 4,622 ms   | 566,229 ms  | 577,702 ms  | 13,9%       | 29,8x   |
| 1P1C             | 1      | BoundedChannel | 168,882 ms  | 22,419 ms  | 134,570 ms  | 187,246 ms  | 31,7%       |         |
| 1P1C             | 1      | Adaptive       | 1357,000 ms | 27,816 ms  | 1321,611 ms | 1390,410 ms | 13,9%       | 8,0x    |
| 1P1C             | 1      | Locked         | 1362,739 ms | 18,336 ms  | 1338,602 ms | 1384,088 ms | 13,9%       | 8,1x    |
| 1P1C             | 1      | core.async     | 1961,963 ms | 11,839 ms  | 1950,275 ms | 1978,250 ms | 13,9%       | 11,6x   |
| 1P1C             | 16     | BoundedChannel | 88,679 ms   | 12,766 ms  | 70,931 ms   | 102,490 ms  | 31,9%       |         |
| 1P1C             | 16     | Adaptive       | 115,072 ms  | 8,260 ms   | 105,821 ms  | 126,438 ms  | 15,3%       | 1,3x    |
| 1P1C             | 16     | Locked         | 199,236 ms  | 38,140 ms  | 159,622 ms  | 236,298 ms  | 48,2%       | 2,2x    |
| 1P1C             | 16     | core.async     | 476,105 ms  | 3,512 ms   | 472,617 ms  | 480,060 ms  | 13,9%       | 5,4x    |
| 4P4C             | 1      | BoundedChannel | 7398,177 ms | 38,307 ms  | 7356,265 ms | 7445,352 ms | 13,9%       | 3,8x    |
| 4P4C             | 1      | Adaptive       | 1954,525 ms | 63,277 ms  | 1878,736 ms | 2018,648 ms | 13,9%       |         |
| 4P4C             | 1      | Locked         | 1972,963 ms | 23,662 ms  | 1947,753 ms | 2004,166 ms | 13,9%       |         |
| 4P4C             | 1      | core.async     | 3198,584 ms | 41,915 ms  | 3161,047 ms | 3251,635 ms | 13,9%       | 1,6x    |
| 4P4C             | 16     | BoundedChannel | 165,777 ms  | 8,268 ms   | 158,248 ms  | 174,900 ms  | 13,9%       |         |
| 4P4C             | 16     | Adaptive       | 447,672 ms  | 29,805 ms  | 411,591 ms  | 495,484 ms  | 15,0%       | 2,7x    |
| 4P4C             | 16     | Locked         | 460,277 ms  | 27,150 ms  | 420,882 ms  | 483,312 ms  | 14,6%       | 2,8x    |
| 4P4C             | 16     | core.async     | 1133,880 ms | 13,535 ms  | 1118,755 ms | 1152,842 ms | 13,9%       | 6,8x    |
| 50×1P1C          |        | BoundedChannel | 36,577 ms   | 2,774 ms   | 33,251 ms   | 39,424 ms   | 15,4%       | 1,2x    |
| 50×1P1C          |        | Adaptive       | 31,261 ms   | 1,592 ms   | 29,660 ms   | 33,245 ms   | 13,9%       |         |
| 50×1P1C          |        | Locked         | 41,739 ms   | 2,985 ms   | 39,196 ms   | 45,319 ms   | 15,3%       | 1,3x    |
| 50×1P1C          |        | core.async     | 533,600 ms  | 8,777 ms   | 524,071 ms  | 544,735 ms  | 13,9%       | 17,1x   |
| 50×4P4C          |        | BoundedChannel | 60,294 ms   | 4,385 ms   | 55,580 ms   | 65,760 ms   | 15,3%       |         |
| 50×4P4C          |        | Adaptive       | 67,207 ms   | 6,761 ms   | 59,605 ms   | 77,810 ms   | 30,5%       | 1,1x    |
| 50×4P4C          |        | Locked         | 60,641 ms   | 5,116 ms   | 53,600 ms   | 66,183 ms   | 15,7%       |         |
| 50×4P4C          |        | core.async     | 1845,246 ms | 43,442 ms  | 1786,793 ms | 1894,260 ms | 13,9%       | 30,6x   |
| Mixed (40 ch)    |        | BoundedChannel | 40,989 ms   | 1,208 ms   | 39,989 ms   | 42,495 ms   | 13,9%       |         |
| Mixed (40 ch)    |        | Adaptive       | 56,748 ms   | 12,070 ms  | 43,563 ms   | 69,011 ms   | 64,1%       | 1,4x    |
| Mixed (40 ch)    |        | Locked         | 54,777 ms   | 8,397 ms   | 45,202 ms   | 63,212 ms   | 47,2%       | 1,3x    |
| Mixed (40 ch)    |        | core.async     | 2055,984 ms | 54,487 ms  | 2000,175 ms | 2132,399 ms | 13,9%       | 50,2x   |
| 200×1P1C         |        | BoundedChannel | 72,173 ms   | 6,799 ms   | 65,085 ms   | 80,156 ms   | 30,1%       | 1,4x    |
| 200×1P1C         |        | Adaptive       | 52,952 ms   | 2,517 ms   | 50,300 ms   | 56,387 ms   | 13,9%       |         |
| 200×1P1C         |        | Locked         | 73,122 ms   | 3,915 ms   | 68,282 ms   | 78,119 ms   | 14,1%       | 1,4x    |
| 200×1P1C         |        | core.async     | 1896,108 ms | 29,997 ms  | 1870,474 ms | 1941,366 ms | 13,9%       | 35,8x   |
| XF map 1P1C      | 1024   | BoundedChannel | 91,884 ms   | 7,417 ms   | 82,338 ms   | 97,258 ms   | 15,6%       |         |
| XF map 1P1C      | 1024   | Adaptive       | 199,172 ms  | 10,981 ms  | 187,989 ms  | 210,759 ms  | 14,3%       | 2,2x    |
| XF map 1P1C      | 1024   | Locked         | 254,769 ms  | 100,462 ms | 178,285 ms  | 376,730 ms  | 82,2%       | 2,8x    |
| XF map 1P1C      | 1024   | core.async     | 300,999 ms  | 61,999 ms  | 231,471 ms  | 372,619 ms  | 48,5%       | 3,3x    |
| XF map 4P4C      | 1024   | BoundedChannel | 614,586 ms  | 27,129 ms  | 581,542 ms  | 639,947 ms  | 13,9%       | 2,1x    |
| XF map 4P4C      | 1024   | Adaptive       | 292,735 ms  | 13,544 ms  | 272,189 ms  | 306,469 ms  | 13,9%       |         |
| XF map 4P4C      | 1024   | Locked         | 300,502 ms  | 17,852 ms  | 281,139 ms  | 323,735 ms  | 14,6%       |         |
| XF map 4P4C      | 1024   | core.async     | 306,982 ms  | 7,853 ms   | 299,220 ms  | 318,597 ms  | 13,9%       |         |
| XF filter 1P1C   | 1024   | BoundedChannel | 52,052 ms   | 5,120 ms   | 46,711 ms   | 56,987 ms   | 30,3%       |         |
| XF filter 1P1C   | 1024   | Adaptive       | 104,922 ms  | 4,179 ms   | 98,469 ms   | 109,111 ms  | 13,9%       | 2,0x    |
| XF filter 1P1C   | 1024   | Locked         | 155,621 ms  | 36,356 ms  | 128,163 ms  | 215,393 ms  | 64,5%       | 3,0x    |
| XF filter 1P1C   | 1024   | core.async     | 191,196 ms  | 9,232 ms   | 183,723 ms  | 202,317 ms  | 13,9%       | 3,7x    |
| XF mapcat 1P1C   | 1024   | BoundedChannel | 65,612 ms   | 3,204 ms   | 60,781 ms   | 69,004 ms   | 13,9%       |         |
| XF mapcat 1P1C   | 1024   | Adaptive       | 177,989 ms  | 20,422 ms  | 154,876 ms  | 202,775 ms  | 31,1%       | 2,7x    |
| XF mapcat 1P1C   | 1024   | Locked         | 262,431 ms  | 95,962 ms  | 133,496 ms  | 370,888 ms  | 82,0%       | 4,0x    |
| XF mapcat 1P1C   | 1024   | core.async     | 262,415 ms  | 2,365 ms   | 258,065 ms  | 264,684 ms  | 13,9%       | 4,0x    |
| Pipe 4P→1P→4C    | 16     | BoundedChannel | 1437,060 ms | 32,691 ms  | 1378,260 ms | 1469,130 ms | 13,9%       | 1,9x    |
| Pipe 4P→1P→4C    | 16     | Adaptive       | 743,724 ms  | 29,566 ms  | 712,205 ms  | 777,153 ms  | 13,9%       |         |
| Pipe 4P→1P→4C    | 16     | Locked         | 870,373 ms  | 42,999 ms  | 820,343 ms  | 936,270 ms  | 13,9%       | 1,2x    |
| Pipe 4P→1P→4C    | 16     | core.async     | 4491,886 ms | 31,339 ms  | 4462,477 ms | 4534,790 ms | 13,9%       | 6,0x    |
| Pipe XF 4P→1P→4C | 16     | BoundedChannel | 1120,973 ms | 36,022 ms  | 1069,979 ms | 1161,290 ms | 13,9%       | 1,5x    |
| Pipe XF 4P→1P→4C | 16     | Adaptive       | 764,592 ms  | 8,048 ms   | 754,739 ms  | 774,126 ms  | 13,9%       |         |
| Pipe XF 4P→1P→4C | 16     | Locked         | 848,504 ms  | 22,738 ms  | 825,605 ms  | 877,913 ms  | 13,9%       | 1,1x    |
| Pipe XF 4P→1P→4C | 16     | core.async     | 4553,607 ms | 31,550 ms  | 4508,672 ms | 4588,369 ms | 13,9%       | 6,0x    |
| Pipe 4P→1P→4C    | 64     | BoundedChannel | 743,014 ms  | 25,100 ms  | 713,099 ms  | 777,654 ms  | 13,9%       | 2,1x    |
| Pipe 4P→1P→4C    | 64     | Adaptive       | 346,992 ms  | 29,991 ms  | 306,858 ms  | 376,829 ms  | 15,7%       |         |
| Pipe 4P→1P→4C    | 64     | Locked         | 395,674 ms  | 28,505 ms  | 362,466 ms  | 433,554 ms  | 15,3%       | 1,1x    |
| Pipe 4P→1P→4C    | 64     | core.async     | 2870,558 ms | 30,878 ms  | 2811,928 ms | 2894,114 ms | 13,9%       | 8,3x    |
| Pipe XF 4P→1P→4C | 64     | BoundedChannel | 679,021 ms  | 17,976 ms  | 654,202 ms  | 694,742 ms  | 13,9%       | 1,9x    |
| Pipe XF 4P→1P→4C | 64     | Adaptive       | 349,519 ms  | 19,085 ms  | 331,001 ms  | 380,955 ms  | 14,2%       |         |
| Pipe XF 4P→1P→4C | 64     | Locked         | 446,971 ms  | 33,011 ms  | 403,158 ms  | 485,424 ms  | 15,3%       | 1,3x    |
| Pipe XF 4P→1P→4C | 64     | core.async     | 3170,394 ms | 388,854 ms | 2923,978 ms | 3715,871 ms | 31,4%       | 9,1x    |
| Pipe 4P→1P→4C    | 1024   | BoundedChannel | 256,922 ms  | 27,370 ms  | 221,487 ms  | 286,714 ms  | 30,8%       | 1,7x    |
| Pipe 4P→1P→4C    | 1024   | Adaptive       | 153,586 ms  | 12,518 ms  | 139,785 ms  | 166,038 ms  | 15,6%       |         |
| Pipe 4P→1P→4C    | 1024   | Locked         | 295,902 ms  | 52,743 ms  | 252,584 ms  | 363,463 ms  | 48,0%       | 1,9x    |
| Pipe 4P→1P→4C    | 1024   | core.async     | 1977,704 ms | 43,457 ms  | 1924,413 ms | 2031,211 ms | 13,9%       | 12,9x   |
| Pipe XF 4P→1P→4C | 1024   | BoundedChannel | 274,013 ms  | 42,922 ms  | 194,409 ms  | 307,512 ms  | 47,3%       | 1,3x    |
| Pipe XF 4P→1P→4C | 1024   | Adaptive       | 218,534 ms  | 9,215 ms   | 205,173 ms  | 228,824 ms  | 13,9%       |         |
| Pipe XF 4P→1P→4C | 1024   | Locked         | 299,340 ms  | 21,422 ms  | 269,481 ms  | 324,079 ms  | 15,3%       | 1,4x    |
| Pipe XF 4P→1P→4C | 1024   | core.async     | 2085,976 ms | 27,164 ms  | 2064,700 ms | 2131,010 ms | 13,9%       | 9,5x    |
| 20×Pipe 4P→1P→4C |        | BoundedChannel | 58,097 ms   | 6,051 ms   | 51,085 ms   | 64,575 ms   | 30,7%       | 2,3x    |
| 20×Pipe 4P→1P→4C |        | Adaptive       | 40,198 ms   | 2,147 ms   | 38,109 ms   | 42,606 ms   | 14,1%       | 1,6x    |
| 20×Pipe 4P→1P→4C |        | Locked         | 53,017 ms   | 5,221 ms   | 47,126 ms   | 59,835 ms   | 30,3%       | 2,1x    |
| 20×Pipe 4P→1P→4C |        | core.async     | 1687,400 ms | 108,370 ms | 1590,196 ms | 1861,144 ms | 14,9%       | 68,0x   |
| 20×Pipe 4P→1P→4C |        | BoundedChannel | 24,810 ms   | 1,919 ms   | 22,666 ms   | 27,468 ms   | 15,5%       |         |
| 20×Pipe 4P→1P→4C |        | Adaptive       | 31,199 ms   | 1,954 ms   | 28,487 ms   | 33,340 ms   | 14,8%       | 1,3x    |
| 20×Pipe 4P→1P→4C |        | Locked         | 40,198 ms   | 2,496 ms   | 35,712 ms   | 42,980 ms   | 14,8%       | 1,6x    |
| 20×Pipe 4P→1P→4C |        | core.async     | 717,333 ms  | 271,040 ms | 269,870 ms  | 1001,231 ms | 82,1%       | 28,9x   |
| 16P1C            | 64     | BoundedChannel | 745,424 ms  | 10,136 ms  | 735,664 ms  | 760,288 ms  | 13,9%       | 3,7x    |
| 16P1C            | 64     | Adaptive       | 204,012 ms  | 9,630 ms   | 193,338 ms  | 213,666 ms  | 13,9%       |         |
| 16P1C            | 64     | Locked         | 224,263 ms  | 10,630 ms  | 216,237 ms  | 241,547 ms  | 13,9%       | 1,1x    |
| 16P1C            | 64     | core.async     | 3282,890 ms | 322,199 ms | 3063,960 ms | 3759,459 ms | 30,3%       | 16,1x   |
| 32P1C            | 64     | BoundedChannel | 786,513 ms  | 21,850 ms  | 757,945 ms  | 806,370 ms  | 13,9%       | 4,1x    |
| 32P1C            | 64     | Adaptive       | 193,889 ms  | 6,970 ms   | 184,870 ms  | 202,420 ms  | 13,9%       |         |
| 32P1C            | 64     | Locked         | 210,368 ms  | 7,134 ms   | 203,464 ms  | 218,893 ms  | 13,9%       | 1,1x    |
| 32P1C            | 64     | core.async     | 5546,636 ms | 32,603 ms  | 5513,961 ms | 5593,946 ms | 13,9%       | 28,6x   |
| 64P1C            | 64     | BoundedChannel | 1137,977 ms | 11,740 ms  | 1126,604 ms | 1150,335 ms | 13,9%       | 4,9x    |
| 64P1C            | 64     | Adaptive       | 234,230 ms  | 7,702 ms   | 224,088 ms  | 239,534 ms  | 13,9%       |         |
| 64P1C            | 64     | Locked         | 249,875 ms  | 14,065 ms  | 232,545 ms  | 266,319 ms  | 14,4%       | 1,1x    |
| 64P1C            | 64     | core.async     | 6671,309 ms | 481,745 ms | 5714,731 ms | 6947,657 ms | 15,3%       | 28,5x   |
| 128P1C           | 64     | BoundedChannel | 5942,191 ms | 424,139 ms | 5533,238 ms | 6628,656 ms | 15,2%       | 21,2x   |
| 128P1C           | 64     | Adaptive       | 279,896 ms  | 9,900 ms   | 269,394 ms  | 291,146 ms  | 13,9%       |         |
| 128P1C           | 64     | Locked         | 307,414 ms  | 10,350 ms  | 296,415 ms  | 324,424 ms  | 13,9%       | 1,1x    |
| 128P1C           | 64     | core.async     | 8951,909 ms | 44,861 ms  | 8909,402 ms | 9005,592 ms | 13,9%       | 32,0x   |
| 16P1C            | 1024   | BoundedChannel | 143,701 ms  | 3,570 ms   | 139,461 ms  | 148,687 ms  | 13,9%       | 1,2x    |
| 16P1C            | 1024   | Adaptive       | 120,371 ms  | 5,911 ms   | 115,165 ms  | 127,035 ms  | 13,9%       |         |
| 16P1C            | 1024   | Locked         | 130,541 ms  | 3,453 ms   | 125,124 ms  | 133,212 ms  | 13,9%       | 1,1x    |
| 16P1C            | 1024   | core.async     | 2726,293 ms | 171,069 ms | 2525,827 ms | 2906,675 ms | 14,8%       | 22,6x   |
| 32P1C            | 1024   | BoundedChannel | 170,151 ms  | 6,555 ms   | 162,880 ms  | 177,021 ms  | 13,9%       | 1,4x    |
| 32P1C            | 1024   | Adaptive       | 120,326 ms  | 8,361 ms   | 106,140 ms  | 127,011 ms  | 15,2%       |         |
| 32P1C            | 1024   | Locked         | 135,304 ms  | 5,320 ms   | 130,107 ms  | 143,110 ms  | 13,9%       | 1,1x    |
| 32P1C            | 1024   | core.async     | 5532,933 ms | 49,564 ms  | 5449,899 ms | 5578,485 ms | 13,9%       | 46,0x   |
| 64P1C            | 1024   | BoundedChannel | 276,076 ms  | 24,412 ms  | 243,008 ms  | 295,254 ms  | 15,7%       | 2,3x    |
| 64P1C            | 1024   | Adaptive       | 120,706 ms  | 6,748 ms   | 110,417 ms  | 126,086 ms  | 14,4%       |         |
| 64P1C            | 1024   | Locked         | 139,236 ms  | 8,306 ms   | 129,283 ms  | 151,773 ms  | 14,6%       | 1,2x    |
| 64P1C            | 1024   | core.async     | 6812,714 ms | 76,918 ms  | 6693,619 ms | 6890,003 ms | 13,9%       | 56,4x   |
| 128P1C           | 1024   | BoundedChannel | 293,840 ms  | 9,230 ms   | 278,815 ms  | 303,117 ms  | 13,9%       | 2,5x    |
| 128P1C           | 1024   | Adaptive       | 118,035 ms  | 6,511 ms   | 109,292 ms  | 125,683 ms  | 14,3%       |         |
| 128P1C           | 1024   | Locked         | 138,816 ms  | 7,517 ms   | 128,205 ms  | 146,543 ms  | 14,2%       | 1,2x    |
| 128P1C           | 1024   | core.async     | 8848,971 ms | 29,725 ms  | 8813,920 ms | 8887,586 ms | 13,9%       | 75,0x   |
| XF 16P1C         | 64     | BoundedChannel | 266,528 ms  | 6,966 ms   | 257,923 ms  | 277,570 ms  | 13,9%       |         |
| XF 16P1C         | 64     | Adaptive       | 499,694 ms  | 21,924 ms  | 461,288 ms  | 516,914 ms  | 13,9%       | 1,9x    |
| XF 16P1C         | 64     | Locked         | 422,150 ms  | 7,454 ms   | 415,700 ms  | 430,748 ms  | 13,9%       | 1,6x    |
| XF 16P1C         | 64     | core.async     | 3193,457 ms | 43,768 ms  | 3124,891 ms | 3236,663 ms | 13,9%       | 12,0x   |
| XF 32P1C         | 64     | BoundedChannel | 273,381 ms  | 24,460 ms  | 230,864 ms  | 292,684 ms  | 15,8%       |         |
| XF 32P1C         | 64     | Adaptive       | 408,217 ms  | 34,070 ms  | 376,110 ms  | 446,785 ms  | 15,6%       | 1,5x    |
| XF 32P1C         | 64     | Locked         | 380,037 ms  | 9,843 ms   | 365,621 ms  | 388,066 ms  | 13,9%       | 1,4x    |
| XF 32P1C         | 64     | core.async     | 5548,005 ms | 60,891 ms  | 5481,338 ms | 5632,048 ms | 13,9%       | 20,3x   |
| XF 64P1C         | 64     | BoundedChannel | 248,705 ms  | 7,083 ms   | 241,356 ms  | 258,759 ms  | 13,9%       |         |
| XF 64P1C         | 64     | Adaptive       | 374,130 ms  | 15,893 ms  | 353,984 ms  | 390,133 ms  | 13,9%       | 1,5x    |
| XF 64P1C         | 64     | Locked         | 391,653 ms  | 14,771 ms  | 371,109 ms  | 406,788 ms  | 13,9%       | 1,6x    |
| XF 64P1C         | 64     | core.async     | 6917,716 ms | 38,014 ms  | 6863,739 ms | 6955,232 ms | 13,9%       | 27,8x   |
| XF 128P1C        | 64     | BoundedChannel | 259,188 ms  | 5,676 ms   | 250,046 ms  | 264,162 ms  | 13,9%       |         |
| XF 128P1C        | 64     | Adaptive       | 394,495 ms  | 7,400 ms   | 385,169 ms  | 402,549 ms  | 13,9%       | 1,5x    |
| XF 128P1C        | 64     | Locked         | 396,559 ms  | 18,815 ms  | 378,957 ms  | 417,290 ms  | 13,9%       | 1,5x    |
| XF 128P1C        | 64     | core.async     | 9084,062 ms | 126,150 ms | 8975,167 ms | 9278,356 ms | 13,9%       | 35,0x   |
| XF 16P1C         | 1024   | BoundedChannel | 193,443 ms  | 2,974 ms   | 190,947 ms  | 197,158 ms  | 13,9%       |         |
| XF 16P1C         | 1024   | Adaptive       | 320,001 ms  | 11,266 ms  | 307,835 ms  | 334,117 ms  | 13,9%       | 1,7x    |
| XF 16P1C         | 1024   | Locked         | 348,124 ms  | 11,833 ms  | 326,575 ms  | 359,578 ms  | 13,9%       | 1,8x    |
| XF 16P1C         | 1024   | core.async     | 2681,136 ms | 292,154 ms | 2114,948 ms | 2881,765 ms | 30,9%       | 13,9x   |
| XF 32P1C         | 1024   | BoundedChannel | 172,348 ms  | 9,105 ms   | 155,006 ms  | 180,473 ms  | 14,1%       |         |
| XF 32P1C         | 1024   | Adaptive       | 288,778 ms  | 9,129 ms   | 278,599 ms  | 298,040 ms  | 13,9%       | 1,7x    |
| XF 32P1C         | 1024   | Locked         | 315,308 ms  | 16,183 ms  | 295,561 ms  | 332,650 ms  | 13,9%       | 1,8x    |
| XF 32P1C         | 1024   | core.async     | 5498,458 ms | 59,976 ms  | 5414,688 ms | 5543,101 ms | 13,9%       | 31,9x   |
| XF 64P1C         | 1024   | BoundedChannel | 178,196 ms  | 2,299 ms   | 174,867 ms  | 180,291 ms  | 13,9%       |         |
| XF 64P1C         | 1024   | Adaptive       | 285,465 ms  | 7,892 ms   | 275,227 ms  | 293,600 ms  | 13,9%       | 1,6x    |
| XF 64P1C         | 1024   | Locked         | 315,476 ms  | 8,655 ms   | 302,483 ms  | 324,313 ms  | 13,9%       | 1,8x    |
| XF 64P1C         | 1024   | core.async     | 6538,557 ms | 795,182 ms | 4921,850 ms | 6916,648 ms | 31,4%       | 36,7x   |
| XF 128P1C        | 1024   | BoundedChannel | 200,772 ms  | 9,570 ms   | 189,072 ms  | 209,360 ms  | 13,9%       |         |
| XF 128P1C        | 1024   | Adaptive       | 309,175 ms  | 11,804 ms  | 290,786 ms  | 321,772 ms  | 13,9%       | 1,5x    |
| XF 128P1C        | 1024   | Locked         | 327,859 ms  | 18,711 ms  | 309,673 ms  | 351,183 ms  | 14,4%       | 1,6x    |
| XF 128P1C        | 1024   | core.async     | 8812,390 ms | 54,596 ms  | 8755,529 ms | 8873,832 ms | 13,9%       | 43,9x   |
