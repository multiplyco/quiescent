## Benchmark Results

| label            | buffer | channel   | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|-----------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | Quiescent | 83,320 ms   | 8,552 ms   | 73,120 ms   | 94,792 ms   | 30,6%       | 1,0x    |
| 1P4C             | 1024   | Quiescent | 278,017 ms  | 4,768 ms   | 271,859 ms  | 283,475 ms  | 13,9%       | 1,0x    |
| 4P1C             | 1024   | Quiescent | 346,622 ms  | 7,668 ms   | 337,458 ms  | 354,743 ms  | 13,9%       | 1,0x    |
| 4P4C             | 1024   | Quiescent | 459,102 ms  | 16,552 ms  | 428,395 ms  | 475,332 ms  | 13,9%       | 1,0x    |
| Ping-pong        | 1      | Quiescent | 156,217 ms  | 5,335 ms   | 148,487 ms  | 160,928 ms  | 13,9%       | 1,0x    |
| 1P1C             | 1      | Quiescent | 1572,762 ms | 126,326 ms | 1417,280 ms | 1701,954 ms | 15,5%       | 1,0x    |
| 1P1C             | 16     | Quiescent | 204,985 ms  | 32,006 ms  | 163,511 ms  | 234,612 ms  | 47,3%       | 1,0x    |
| 4P4C             | 1      | Quiescent | 3107,054 ms | 31,585 ms  | 3074,818 ms | 3155,695 ms | 13,9%       | 1,0x    |
| 4P4C             | 16     | Quiescent | 1076,499 ms | 20,069 ms  | 1052,641 ms | 1103,227 ms | 13,9%       | 1,0x    |
| 50×1P1C          |        | Quiescent | 46,168 ms   | 2,002 ms   | 44,531 ms   | 49,433 ms   | 13,9%       | 1,0x    |
| 50×4P4C          |        | Quiescent | 82,068 ms   | 6,340 ms   | 71,807 ms   | 89,243 ms   | 15,5%       | 1,0x    |
| Mixed (40 ch)    |        | Quiescent | 62,131 ms   | 5,550 ms   | 58,514 ms   | 71,639 ms   | 15,8%       | 1,0x    |
| 200×1P1C         |        | Quiescent | 84,379 ms   | 3,042 ms   | 79,468 ms   | 87,351 ms   | 13,9%       | 1,0x    |
| 200×1P1C buf=1   |        | Quiescent | 1614,439 ms | 18,347 ms  | 1591,356 ms | 1636,965 ms | 13,9%       | 1,0x    |
| 24×16P1C         |        | Quiescent | 39,531 ms   | 4,335 ms   | 34,227 ms   | 45,153 ms   | 30,9%       | 1,0x    |
| 24×32P1C         |        | Quiescent | 37,480 ms   | 5,795 ms   | 30,074 ms   | 42,948 ms   | 47,3%       | 1,0x    |
| 24×64P1C         |        | Quiescent | 43,624 ms   | 2,580 ms   | 40,027 ms   | 46,702 ms   | 14,6%       | 1,0x    |
| 24×128P1C        |        | Quiescent | 46,342 ms   | 3,451 ms   | 42,213 ms   | 50,522 ms   | 15,4%       | 1,0x    |
| XF map 1P1C      | 1024   | Quiescent | 101,552 ms  | 8,645 ms   | 89,752 ms   | 110,006 ms  | 15,7%       | 1,0x    |
| XF map 4P4C      | 1024   | Quiescent | 1805,453 ms | 73,719 ms  | 1683,291 ms | 1886,963 ms | 13,9%       | 1,0x    |
| XF filter 1P1C   | 1024   | Quiescent | 61,520 ms   | 3,927 ms   | 56,658 ms   | 64,972 ms   | 14,9%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | Quiescent | 77,831 ms   | 2,215 ms   | 75,188 ms   | 80,233 ms   | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | Quiescent | 1062,652 ms | 7,762 ms   | 1049,460 ms | 1069,347 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | Quiescent | 2306,511 ms | 38,979 ms  | 2263,749 ms | 2358,047 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | Quiescent | 672,440 ms  | 34,721 ms  | 636,641 ms  | 710,896 ms  | 14,0%       | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | Quiescent | 2259,094 ms | 38,309 ms  | 2202,696 ms | 2299,675 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | Quiescent | 437,189 ms  | 13,061 ms  | 420,127 ms  | 448,793 ms  | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | Quiescent | 2278,093 ms | 31,557 ms  | 2250,742 ms | 2315,336 ms | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 58,878 ms   | 6,760 ms   | 50,271 ms   | 66,530 ms   | 31,1%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 43,962 ms   | 2,996 ms   | 40,614 ms   | 47,505 ms   | 15,1%       | 1,0x    |
| 16P1C            | 64     | Quiescent | 714,713 ms  | 16,268 ms  | 686,138 ms  | 731,267 ms  | 13,9%       | 1,0x    |
| 32P1C            | 64     | Quiescent | 848,705 ms  | 23,671 ms  | 820,190 ms  | 879,879 ms  | 13,9%       | 1,0x    |
| 64P1C            | 64     | Quiescent | 930,748 ms  | 13,918 ms  | 917,610 ms  | 949,071 ms  | 13,9%       | 1,0x    |
| 128P1C           | 64     | Quiescent | 941,219 ms  | 12,999 ms  | 925,253 ms  | 957,766 ms  | 13,9%       | 1,0x    |
| 16P1C            | 1024   | Quiescent | 356,492 ms  | 19,051 ms  | 336,627 ms  | 375,330 ms  | 14,1%       | 1,0x    |
| 32P1C            | 1024   | Quiescent | 398,631 ms  | 18,254 ms  | 367,513 ms  | 414,601 ms  | 13,9%       | 1,0x    |
| 64P1C            | 1024   | Quiescent | 353,944 ms  | 5,570 ms   | 349,344 ms  | 360,611 ms  | 13,9%       | 1,0x    |
| 128P1C           | 1024   | Quiescent | 350,155 ms  | 3,856 ms   | 345,320 ms  | 353,796 ms  | 13,9%       | 1,0x    |
| XF 16P1C         | 64     | Quiescent | 2450,288 ms | 17,246 ms  | 2429,070 ms | 2474,133 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 64     | Quiescent | 2513,221 ms | 34,875 ms  | 2466,172 ms | 2554,902 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 64     | Quiescent | 2549,796 ms | 18,713 ms  | 2526,983 ms | 2568,247 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 64     | Quiescent | 2628,113 ms | 67,056 ms  | 2560,383 ms | 2705,560 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 1024   | Quiescent | 2435,900 ms | 46,823 ms  | 2364,726 ms | 2484,640 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 1024   | Quiescent | 2510,026 ms | 25,486 ms  | 2488,308 ms | 2548,716 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 1024   | Quiescent | 2560,174 ms | 48,286 ms  | 2508,261 ms | 2611,418 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 1024   | Quiescent | 2583,876 ms | 36,483 ms  | 2553,103 ms | 2630,852 ms | 13,9%       | 1,0x    |
