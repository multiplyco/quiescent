## Benchmark Results

| label            | buffer | channel        | mean        | std-dev     | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|----------------|-------------|-------------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | BoundedChannel | 74,726 ms   | 9,327 ms    | 68,319 ms   | 88,721 ms   | 31,5%       | 3,1x    |
| 1P1C             | 1024   | Adaptive       | 125,816 ms  | 38,849 ms   | 93,449 ms   | 188,795 ms  | 81,4%       | 1,8x    |
| 1P1C             | 1024   | Locked         | 115,222 ms  | 18,019 ms   | 90,194 ms   | 134,287 ms  | 47,3%       | 2,0x    |
| 1P1C             | 1024   | core.async     | 228,272 ms  | 38,751 ms   | 186,890 ms  | 268,939 ms  | 47,7%       | 1,0x    |
| 1P4C             | 1024   | BoundedChannel | 67,423 ms   | 2,650 ms    | 62,396 ms   | 69,416 ms   | 13,9%       | 4,6x    |
| 1P4C             | 1024   | Adaptive       | 113,768 ms  | 5,531 ms    | 106,874 ms  | 119,988 ms  | 13,9%       | 2,7x    |
| 1P4C             | 1024   | Locked         | 130,720 ms  | 8,828 ms    | 125,325 ms  | 145,709 ms  | 15,1%       | 2,4x    |
| 1P4C             | 1024   | core.async     | 311,543 ms  | 11,960 ms   | 300,807 ms  | 330,314 ms  | 13,9%       | 1,0x    |
| 4P1C             | 1024   | BoundedChannel | 96,793 ms   | 3,197 ms    | 92,098 ms   | 100,166 ms  | 13,9%       | 3,0x    |
| 4P1C             | 1024   | Adaptive       | 115,771 ms  | 5,911 ms    | 109,470 ms  | 122,831 ms  | 13,9%       | 2,5x    |
| 4P1C             | 1024   | Locked         | 143,861 ms  | 7,200 ms    | 136,184 ms  | 154,667 ms  | 13,9%       | 2,0x    |
| 4P1C             | 1024   | core.async     | 291,752 ms  | 12,574 ms   | 282,233 ms  | 312,842 ms  | 13,9%       | 1,0x    |
| 4P4C             | 1024   | BoundedChannel | 129,591 ms  | 9,219 ms    | 114,738 ms  | 137,902 ms  | 15,2%       | 1,8x    |
| 4P4C             | 1024   | Adaptive       | 146,582 ms  | 14,906 ms   | 128,583 ms  | 162,143 ms  | 30,5%       | 1,6x    |
| 4P4C             | 1024   | Locked         | 164,387 ms  | 8,471 ms    | 154,695 ms  | 175,329 ms  | 13,9%       | 1,4x    |
| 4P4C             | 1024   | core.async     | 230,895 ms  | 2,248 ms    | 227,454 ms  | 233,254 ms  | 13,9%       | 1,0x    |
| Ping-pong        | 1      | BoundedChannel | 23,943 ms   | 0,849 ms    | 22,776 ms   | 24,683 ms   | 13,9%       | 25,5x   |
| Ping-pong        | 1      | Adaptive       | 138,459 ms  | 4,591 ms    | 133,085 ms  | 144,230 ms  | 13,9%       | 4,4x    |
| Ping-pong        | 1      | Locked         | 133,462 ms  | 1,670 ms    | 130,999 ms  | 135,233 ms  | 13,9%       | 4,6x    |
| Ping-pong        | 1      | core.async     | 610,769 ms  | 69,850 ms   | 572,107 ms  | 730,810 ms  | 31,1%       | 1,0x    |
| 1P1C             | 1      | BoundedChannel | 174,660 ms  | 20,011 ms   | 149,099 ms  | 195,714 ms  | 31,1%       | 10,9x   |
| 1P1C             | 1      | Adaptive       | 1303,074 ms | 17,806 ms   | 1278,467 ms | 1321,084 ms | 13,9%       | 1,5x    |
| 1P1C             | 1      | Locked         | 1327,574 ms | 77,731 ms   | 1271,047 ms | 1457,219 ms | 14,6%       | 1,4x    |
| 1P1C             | 1      | core.async     | 1910,313 ms | 24,841 ms   | 1890,139 ms | 1945,090 ms | 13,9%       | 1,0x    |
| 1P1C             | 16     | BoundedChannel | 146,051 ms  | 11,065 ms   | 132,892 ms  | 160,637 ms  | 15,4%       | 2,9x    |
| 1P1C             | 16     | Adaptive       | 136,687 ms  | 25,863 ms   | 98,840 ms   | 158,752 ms  | 48,2%       | 3,1x    |
| 1P1C             | 16     | Locked         | 247,065 ms  | 10,337 ms   | 235,832 ms  | 261,134 ms  | 13,9%       | 1,7x    |
| 1P1C             | 16     | core.async     | 425,153 ms  | 30,379 ms   | 388,809 ms  | 455,392 ms  | 15,2%       | 1,0x    |
| 4P4C             | 1      | BoundedChannel | 7211,665 ms | 70,991 ms   | 7138,281 ms | 7281,282 ms | 13,9%       | 1,0x    |
| 4P4C             | 1      | Adaptive       | 1744,921 ms | 30,306 ms   | 1716,755 ms | 1776,153 ms | 13,9%       | 4,1x    |
| 4P4C             | 1      | Locked         | 1783,713 ms | 33,960 ms   | 1740,694 ms | 1823,940 ms | 13,9%       | 4,0x    |
| 4P4C             | 1      | core.async     | 3098,745 ms | 24,880 ms   | 3066,363 ms | 3123,234 ms | 13,9%       | 2,3x    |
| 4P4C             | 16     | BoundedChannel | 156,757 ms  | 4,820 ms    | 147,784 ms  | 160,843 ms  | 13,9%       | 7,0x    |
| 4P4C             | 16     | Adaptive       | 447,549 ms  | 22,871 ms   | 424,517 ms  | 483,871 ms  | 13,9%       | 2,4x    |
| 4P4C             | 16     | Locked         | 448,549 ms  | 26,989 ms   | 413,593 ms  | 478,668 ms  | 14,7%       | 2,4x    |
| 4P4C             | 16     | core.async     | 1096,402 ms | 8,431 ms    | 1086,620 ms | 1105,264 ms | 13,9%       | 1,0x    |
| 50×1P1C          |        | BoundedChannel | 44,136 ms   | 3,255 ms    | 40,120 ms   | 48,260 ms   | 15,3%       | 11,2x   |
| 50×1P1C          |        | Adaptive       | 30,163 ms   | 2,231 ms    | 27,449 ms   | 32,892 ms   | 15,3%       | 16,4x   |
| 50×1P1C          |        | Locked         | 47,512 ms   | 2,791 ms    | 44,445 ms   | 50,596 ms   | 14,6%       | 10,4x   |
| 50×1P1C          |        | core.async     | 496,146 ms  | 8,899 ms    | 485,707 ms  | 505,325 ms  | 13,9%       | 1,0x    |
| 50×4P4C          |        | BoundedChannel | 72,881 ms   | 1,910 ms    | 71,355 ms   | 75,298 ms   | 13,9%       | 25,8x   |
| 50×4P4C          |        | Adaptive       | 52,190 ms   | 1,632 ms    | 50,090 ms   | 54,173 ms   | 13,9%       | 36,0x   |
| 50×4P4C          |        | Locked         | 60,326 ms   | 6,273 ms    | 56,986 ms   | 71,150 ms   | 30,7%       | 31,2x   |
| 50×4P4C          |        | core.async     | 1880,137 ms | 43,058 ms   | 1822,308 ms | 1928,265 ms | 13,9%       | 1,0x    |
| Mixed (40 ch)    |        | BoundedChannel | 55,543 ms   | 1,601 ms    | 54,219 ms   | 57,739 ms   | 13,9%       | 38,2x   |
| Mixed (40 ch)    |        | Adaptive       | 49,008 ms   | 1,194 ms    | 47,768 ms   | 50,228 ms   | 13,9%       | 43,3x   |
| Mixed (40 ch)    |        | Locked         | 53,672 ms   | 5,933 ms    | 43,894 ms   | 59,715 ms   | 31,0%       | 39,5x   |
| Mixed (40 ch)    |        | core.async     | 2122,477 ms | 44,066 ms   | 2063,724 ms | 2169,114 ms | 13,9%       | 1,0x    |
| 200×1P1C         |        | BoundedChannel | 76,797 ms   | 4,837 ms    | 68,879 ms   | 81,370 ms   | 14,8%       | 25,9x   |
| 200×1P1C         |        | Adaptive       | 51,674 ms   | 2,640 ms    | 49,136 ms   | 54,782 ms   | 13,9%       | 38,4x   |
| 200×1P1C         |        | Locked         | 70,388 ms   | 2,687 ms    | 67,398 ms   | 73,614 ms   | 13,9%       | 28,2x   |
| 200×1P1C         |        | core.async     | 1986,614 ms | 83,590 ms   | 1900,488 ms | 2088,046 ms | 13,9%       | 1,0x    |
| XF map 1P1C      | 1024   | BoundedChannel | 94,837 ms   | 4,896 ms    | 90,597 ms   | 100,791 ms  | 14,0%       | 3,0x    |
| XF map 1P1C      | 1024   | Adaptive       | 165,227 ms  | 32,525 ms   | 125,965 ms  | 207,180 ms  | 48,3%       | 1,7x    |
| XF map 1P1C      | 1024   | Locked         | 237,487 ms  | 114,907 ms  | 147,311 ms  | 425,173 ms  | 82,6%       | 1,2x    |
| XF map 1P1C      | 1024   | core.async     | 280,519 ms  | 53,304 ms   | 218,581 ms  | 351,508 ms  | 48,2%       | 1,0x    |
| XF map 4P4C      | 1024   | BoundedChannel | 558,797 ms  | 131,461 ms  | 328,703 ms  | 673,555 ms  | 64,6%       | 1,0x    |
| XF map 4P4C      | 1024   | Adaptive       | 181,097 ms  | 7,135 ms    | 170,975 ms  | 189,084 ms  | 13,9%       | 3,1x    |
| XF map 4P4C      | 1024   | Locked         | 289,559 ms  | 5,933 ms    | 279,009 ms  | 293,917 ms  | 13,9%       | 1,9x    |
| XF map 4P4C      | 1024   | core.async     | 255,355 ms  | 2,812 ms    | 252,984 ms  | 259,712 ms  | 13,9%       | 2,2x    |
| XF filter 1P1C   | 1024   | BoundedChannel | 48,904 ms   | 0,670 ms    | 47,744 ms   | 49,647 ms   | 13,9%       | 3,6x    |
| XF filter 1P1C   | 1024   | Adaptive       | 129,080 ms  | 26,840 ms   | 96,200 ms   | 155,284 ms  | 64,0%       | 1,4x    |
| XF filter 1P1C   | 1024   | Locked         | 163,720 ms  | 38,826 ms   | 122,153 ms  | 212,968 ms  | 64,6%       | 1,1x    |
| XF filter 1P1C   | 1024   | core.async     | 176,725 ms  | 9,439 ms    | 160,782 ms  | 187,268 ms  | 14,1%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | BoundedChannel | 72,248 ms   | 3,603 ms    | 69,441 ms   | 77,347 ms   | 13,9%       | 3,2x    |
| XF mapcat 1P1C   | 1024   | Adaptive       | 120,067 ms  | 16,512 ms   | 100,849 ms  | 137,902 ms  | 31,8%       | 1,9x    |
| XF mapcat 1P1C   | 1024   | Locked         | 174,877 ms  | 47,198 ms   | 126,494 ms  | 226,913 ms  | 65,1%       | 1,3x    |
| XF mapcat 1P1C   | 1024   | core.async     | 228,264 ms  | 8,182 ms    | 219,694 ms  | 237,374 ms  | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | BoundedChannel | 1747,683 ms | 410,922 ms  | 1471,109 ms | 2311,359 ms | 64,6%       | 2,5x    |
| Pipe 4P→1P→4C    | 16     | Adaptive       | 806,584 ms  | 19,955 ms   | 782,641 ms  | 832,838 ms  | 13,9%       | 5,4x    |
| Pipe 4P→1P→4C    | 16     | Locked         | 886,333 ms  | 49,713 ms   | 838,909 ms  | 962,338 ms  | 14,4%       | 4,9x    |
| Pipe 4P→1P→4C    | 16     | core.async     | 4355,446 ms | 45,208 ms   | 4317,024 ms | 4406,500 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | BoundedChannel | 1179,160 ms | 35,944 ms   | 1142,626 ms | 1216,095 ms | 13,9%       | 3,8x    |
| Pipe XF 4P→1P→4C | 16     | Adaptive       | 838,891 ms  | 15,584 ms   | 820,988 ms  | 860,252 ms  | 13,9%       | 5,3x    |
| Pipe XF 4P→1P→4C | 16     | Locked         | 844,789 ms  | 11,860 ms   | 823,370 ms  | 854,895 ms  | 13,9%       | 5,3x    |
| Pipe XF 4P→1P→4C | 16     | core.async     | 4476,317 ms | 84,557 ms   | 4395,458 ms | 4612,620 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | BoundedChannel | 765,475 ms  | 6,986 ms    | 757,373 ms  | 773,009 ms  | 13,9%       | 3,7x    |
| Pipe 4P→1P→4C    | 64     | Adaptive       | 355,485 ms  | 14,019 ms   | 340,325 ms  | 368,667 ms  | 13,9%       | 7,9x    |
| Pipe 4P→1P→4C    | 64     | Locked         | 416,128 ms  | 32,862 ms   | 362,836 ms  | 445,516 ms  | 15,5%       | 6,7x    |
| Pipe 4P→1P→4C    | 64     | core.async     | 2803,844 ms | 27,621 ms   | 2759,067 ms | 2833,025 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | BoundedChannel | 698,694 ms  | 17,544 ms   | 679,755 ms  | 721,136 ms  | 13,9%       | 4,1x    |
| Pipe XF 4P→1P→4C | 64     | Adaptive       | 375,951 ms  | 11,843 ms   | 361,404 ms  | 389,991 ms  | 13,9%       | 7,7x    |
| Pipe XF 4P→1P→4C | 64     | Locked         | 438,640 ms  | 22,745 ms   | 411,198 ms  | 461,555 ms  | 14,0%       | 6,6x    |
| Pipe XF 4P→1P→4C | 64     | core.async     | 2893,699 ms | 44,902 ms   | 2845,499 ms | 2943,274 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | BoundedChannel | 246,659 ms  | 54,891 ms   | 156,888 ms  | 292,195 ms  | 64,3%       | 15,6x   |
| Pipe 4P→1P→4C    | 1024   | Adaptive       | 188,993 ms  | 24,278 ms   | 161,659 ms  | 222,190 ms  | 31,6%       | 20,4x   |
| Pipe 4P→1P→4C    | 1024   | Locked         | 314,072 ms  | 17,358 ms   | 294,030 ms  | 337,137 ms  | 14,3%       | 12,3x   |
| Pipe 4P→1P→4C    | 1024   | core.async     | 3859,945 ms | 394,893 ms  | 3255,658 ms | 4231,496 ms | 30,6%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | BoundedChannel | 271,840 ms  | 47,531 ms   | 207,229 ms  | 318,462 ms  | 47,9%       | 10,3x   |
| Pipe XF 4P→1P→4C | 1024   | Adaptive       | 215,213 ms  | 16,433 ms   | 195,243 ms  | 236,722 ms  | 15,4%       | 13,0x   |
| Pipe XF 4P→1P→4C | 1024   | Locked         | 324,170 ms  | 21,108 ms   | 301,188 ms  | 350,090 ms  | 15,0%       | 8,6x    |
| Pipe XF 4P→1P→4C | 1024   | core.async     | 2789,164 ms | 1390,869 ms | 1867,294 ms | 5103,932 ms | 82,6%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | BoundedChannel | 59,424 ms   | 9,209 ms    | 46,613 ms   | 68,366 ms   | 47,3%       | 29,5x   |
| 20×Pipe 4P→1P→4C |        | Adaptive       | 45,335 ms   | 5,317 ms    | 36,542 ms   | 50,248 ms   | 31,2%       | 38,7x   |
| 20×Pipe 4P→1P→4C |        | Locked         | 49,999 ms   | 7,102 ms    | 41,138 ms   | 58,516 ms   | 31,9%       | 35,1x   |
| 20×Pipe 4P→1P→4C |        | core.async     | 1753,735 ms | 148,705 ms  | 1556,469 ms | 1910,410 ms | 15,7%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | BoundedChannel | 30,291 ms   | 2,754 ms    | 27,046 ms   | 33,283 ms   | 15,8%       | 21,1x   |
| 20×Pipe 4P→1P→4C |        | Adaptive       | 31,175 ms   | 2,612 ms    | 27,939 ms   | 34,521 ms   | 15,6%       | 20,5x   |
| 20×Pipe 4P→1P→4C |        | Locked         | 42,521 ms   | 4,614 ms    | 37,139 ms   | 48,008 ms   | 30,9%       | 15,0x   |
| 20×Pipe 4P→1P→4C |        | core.async     | 637,709 ms  | 373,595 ms  | 227,027 ms  | 1236,322 ms | 82,8%       | 1,0x    |
| 16P1C            | 64     | BoundedChannel | 740,673 ms  | 10,205 ms   | 729,953 ms  | 754,045 ms  | 13,9%       | 4,1x    |
| 16P1C            | 64     | Adaptive       | 201,051 ms  | 3,750 ms    | 196,488 ms  | 205,129 ms  | 13,9%       | 15,1x   |
| 16P1C            | 64     | Locked         | 233,844 ms  | 15,255 ms   | 215,664 ms  | 249,717 ms  | 15,0%       | 12,9x   |
| 16P1C            | 64     | core.async     | 3026,714 ms | 37,559 ms   | 2968,835 ms | 3070,310 ms | 13,9%       | 1,0x    |
| 32P1C            | 64     | BoundedChannel | 765,214 ms  | 45,391 ms   | 701,335 ms  | 801,686 ms  | 14,6%       | 6,9x    |
| 32P1C            | 64     | Adaptive       | 197,885 ms  | 8,041 ms    | 191,091 ms  | 211,037 ms  | 13,9%       | 26,8x   |
| 32P1C            | 64     | Locked         | 220,414 ms  | 17,137 ms   | 201,888 ms  | 244,686 ms  | 15,5%       | 24,0x   |
| 32P1C            | 64     | core.async     | 5296,632 ms | 78,271 ms   | 5220,536 ms | 5416,636 ms | 13,9%       | 1,0x    |
| 64P1C            | 64     | BoundedChannel | 1155,645 ms | 41,686 ms   | 1107,658 ms | 1207,961 ms | 13,9%       | 5,6x    |
| 64P1C            | 64     | Adaptive       | 263,648 ms  | 22,720 ms   | 240,500 ms  | 295,812 ms  | 15,7%       | 24,7x   |
| 64P1C            | 64     | Locked         | 264,497 ms  | 18,052 ms   | 245,365 ms  | 282,135 ms  | 15,1%       | 24,6x   |
| 64P1C            | 64     | core.async     | 6517,491 ms | 50,734 ms   | 6452,070 ms | 6574,189 ms | 13,9%       | 1,0x    |
| 128P1C           | 64     | BoundedChannel | 5559,219 ms | 157,495 ms  | 5365,642 ms | 5761,890 ms | 13,9%       | 1,5x    |
| 128P1C           | 64     | Adaptive       | 307,203 ms  | 15,692 ms   | 289,515 ms  | 323,009 ms  | 13,9%       | 26,4x   |
| 128P1C           | 64     | Locked         | 323,580 ms  | 19,717 ms   | 307,058 ms  | 357,362 ms  | 14,7%       | 25,1x   |
| 128P1C           | 64     | core.async     | 8115,847 ms | 537,337 ms  | 7075,533 ms | 8515,349 ms | 15,0%       | 1,0x    |
| 16P1C            | 1024   | BoundedChannel | 187,426 ms  | 33,854 ms   | 165,312 ms  | 243,391 ms  | 48,0%       | 15,2x   |
| 16P1C            | 1024   | Adaptive       | 111,156 ms  | 6,000 ms    | 106,592 ms  | 120,699 ms  | 14,2%       | 25,6x   |
| 16P1C            | 1024   | Locked         | 138,953 ms  | 8,247 ms    | 129,486 ms  | 149,707 ms  | 14,6%       | 20,5x   |
| 16P1C            | 1024   | core.async     | 2846,861 ms | 243,926 ms  | 2374,290 ms | 3023,062 ms | 15,7%       | 1,0x    |
| 32P1C            | 1024   | BoundedChannel | 184,436 ms  | 13,956 ms   | 169,056 ms  | 202,944 ms  | 15,4%       | 28,4x   |
| 32P1C            | 1024   | Adaptive       | 108,766 ms  | 4,376 ms    | 104,122 ms  | 113,563 ms  | 13,9%       | 48,1x   |
| 32P1C            | 1024   | Locked         | 141,836 ms  | 9,780 ms    | 131,379 ms  | 151,739 ms  | 15,1%       | 36,9x   |
| 32P1C            | 1024   | core.async     | 5234,121 ms | 74,177 ms   | 5111,214 ms | 5299,298 ms | 13,9%       | 1,0x    |
| 64P1C            | 1024   | BoundedChannel | 289,429 ms  | 15,483 ms   | 271,278 ms  | 305,327 ms  | 14,1%       | 21,6x   |
| 64P1C            | 1024   | Adaptive       | 112,600 ms  | 4,850 ms    | 108,433 ms  | 118,727 ms  | 13,9%       | 55,6x   |
| 64P1C            | 1024   | Locked         | 141,826 ms  | 10,035 ms   | 130,290 ms  | 151,573 ms  | 15,2%       | 44,2x   |
| 64P1C            | 1024   | core.async     | 6265,226 ms | 448,225 ms  | 5356,286 ms | 6485,580 ms | 15,3%       | 1,0x    |
| 128P1C           | 1024   | BoundedChannel | 295,752 ms  | 4,931 ms    | 289,410 ms  | 300,435 ms  | 13,9%       | 27,7x   |
| 128P1C           | 1024   | Adaptive       | 114,802 ms  | 6,247 ms    | 108,597 ms  | 122,204 ms  | 14,2%       | 71,3x   |
| 128P1C           | 1024   | Locked         | 140,104 ms  | 11,636 ms   | 128,611 ms  | 153,274 ms  | 15,6%       | 58,5x   |
| 128P1C           | 1024   | core.async     | 8189,584 ms | 298,887 ms  | 7587,712 ms | 8359,015 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 64     | BoundedChannel | 284,177 ms  | 6,034 ms    | 278,125 ms  | 290,694 ms  | 13,9%       | 10,9x   |
| XF 16P1C         | 64     | Adaptive       | 347,836 ms  | 7,326 ms    | 337,725 ms  | 356,022 ms  | 13,9%       | 8,9x    |
| XF 16P1C         | 64     | Locked         | 420,584 ms  | 20,632 ms   | 397,288 ms  | 447,413 ms  | 13,9%       | 7,4x    |
| XF 16P1C         | 64     | core.async     | 3092,691 ms | 47,121 ms   | 3043,187 ms | 3160,887 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 64     | BoundedChannel | 251,421 ms  | 11,137 ms   | 238,837 ms  | 264,372 ms  | 13,9%       | 21,1x   |
| XF 32P1C         | 64     | Adaptive       | 323,540 ms  | 5,572 ms    | 317,873 ms  | 329,941 ms  | 13,9%       | 16,4x   |
| XF 32P1C         | 64     | Locked         | 385,448 ms  | 11,551 ms   | 372,932 ms  | 400,055 ms  | 13,9%       | 13,8x   |
| XF 32P1C         | 64     | core.async     | 5306,376 ms | 11,587 ms   | 5291,956 ms | 5320,574 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 64     | BoundedChannel | 253,700 ms  | 6,223 ms    | 246,913 ms  | 260,562 ms  | 13,9%       | 25,9x   |
| XF 64P1C         | 64     | Adaptive       | 365,595 ms  | 24,184 ms   | 333,059 ms  | 392,041 ms  | 15,0%       | 18,0x   |
| XF 64P1C         | 64     | Locked         | 386,820 ms  | 11,705 ms   | 369,011 ms  | 395,920 ms  | 13,9%       | 17,0x   |
| XF 64P1C         | 64     | core.async     | 6566,013 ms | 50,496 ms   | 6505,904 ms | 6633,797 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 64     | BoundedChannel | 258,906 ms  | 3,983 ms    | 254,454 ms  | 263,386 ms  | 13,9%       | 30,0x   |
| XF 128P1C        | 64     | Adaptive       | 427,103 ms  | 12,233 ms   | 404,176 ms  | 436,009 ms  | 13,9%       | 18,2x   |
| XF 128P1C        | 64     | Locked         | 395,480 ms  | 16,329 ms   | 378,550 ms  | 416,016 ms  | 13,9%       | 19,7x   |
| XF 128P1C        | 64     | core.async     | 7773,117 ms | 900,030 ms  | 6519,663 ms | 8388,732 ms | 31,2%       | 1,0x    |
| XF 16P1C         | 1024   | BoundedChannel | 191,419 ms  | 10,571 ms   | 178,690 ms  | 204,402 ms  | 14,3%       | 13,6x   |
| XF 16P1C         | 1024   | Adaptive       | 223,857 ms  | 5,884 ms    | 216,099 ms  | 229,856 ms  | 13,9%       | 11,6x   |
| XF 16P1C         | 1024   | Locked         | 321,468 ms  | 6,996 ms    | 314,905 ms  | 328,588 ms  | 13,9%       | 8,1x    |
| XF 16P1C         | 1024   | core.async     | 2607,166 ms | 332,888 ms  | 2189,172 ms | 2961,672 ms | 31,6%       | 1,0x    |
| XF 32P1C         | 1024   | BoundedChannel | 186,624 ms  | 7,340 ms    | 176,721 ms  | 194,206 ms  | 13,9%       | 27,6x   |
| XF 32P1C         | 1024   | Adaptive       | 215,820 ms  | 2,267 ms    | 212,416 ms  | 218,160 ms  | 13,9%       | 23,9x   |
| XF 32P1C         | 1024   | Locked         | 307,429 ms  | 13,574 ms   | 295,227 ms  | 327,202 ms  | 13,9%       | 16,8x   |
| XF 32P1C         | 1024   | core.async     | 5153,826 ms | 433,439 ms  | 4286,700 ms | 5424,700 ms | 15,6%       | 1,0x    |
| XF 64P1C         | 1024   | BoundedChannel | 181,714 ms  | 7,556 ms    | 173,040 ms  | 189,859 ms  | 13,9%       | 35,8x   |
| XF 64P1C         | 1024   | Adaptive       | 213,193 ms  | 8,330 ms    | 205,691 ms  | 224,425 ms  | 13,9%       | 30,5x   |
| XF 64P1C         | 1024   | Locked         | 307,053 ms  | 8,777 ms    | 293,357 ms  | 315,801 ms  | 13,9%       | 21,2x   |
| XF 64P1C         | 1024   | core.async     | 6497,810 ms | 36,605 ms   | 6449,742 ms | 6538,585 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 1024   | BoundedChannel | 190,089 ms  | 5,584 ms    | 182,743 ms  | 195,699 ms  | 13,9%       | 42,7x   |
| XF 128P1C        | 1024   | Adaptive       | 226,533 ms  | 11,107 ms   | 212,357 ms  | 237,699 ms  | 13,9%       | 35,8x   |
| XF 128P1C        | 1024   | Locked         | 312,891 ms  | 8,485 ms    | 304,001 ms  | 325,529 ms  | 13,9%       | 25,9x   |
| XF 128P1C        | 1024   | core.async     | 8119,417 ms | 652,641 ms  | 6794,576 ms | 8461,935 ms | 15,5%       | 1,0x    |
