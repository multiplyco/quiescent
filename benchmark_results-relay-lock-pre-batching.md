## Benchmark Results

| label            | buffer | channel   | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|-----------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | Quiescent | 91,247 ms   | 9,987 ms   | 79,311 ms   | 104,244 ms  | 30,9%       | 1,0x    |
| 1P4C             | 1024   | Quiescent | 1034,579 ms | 35,561 ms  | 975,967 ms  | 1078,063 ms | 13,9%       | 1,0x    |
| 4P1C             | 1024   | Quiescent | 2146,300 ms | 50,792 ms  | 2069,778 ms | 2197,076 ms | 13,9%       | 1,0x    |
| 4P4C             | 1024   | Quiescent | 1659,248 ms | 30,182 ms  | 1626,629 ms | 1693,993 ms | 13,9%       | 1,0x    |
| Ping-pong        | 1      | Quiescent | 140,333 ms  | 3,771 ms   | 136,584 ms  | 146,368 ms  | 13,9%       | 1,0x    |
| 1P1C             | 1      | Quiescent | 1477,301 ms | 71,311 ms  | 1399,359 ms | 1577,251 ms | 13,9%       | 1,0x    |
| 1P1C             | 16     | Quiescent | 196,615 ms  | 18,563 ms  | 180,869 ms  | 221,634 ms  | 30,1%       | 1,0x    |
| 4P4C             | 1      | Quiescent | 3049,790 ms | 45,074 ms  | 3005,873 ms | 3098,414 ms | 13,9%       | 1,0x    |
| 4P4C             | 16     | Quiescent | 1816,579 ms | 70,482 ms  | 1752,993 ms | 1925,946 ms | 13,9%       | 1,0x    |
| 50×1P1C          |        | Quiescent | 42,819 ms   | 4,397 ms   | 38,544 ms   | 47,911 ms   | 30,6%       | 1,0x    |
| 50×4P4C          |        | Quiescent | 796,516 ms  | 46,055 ms  | 737,693 ms  | 840,427 ms  | 14,5%       | 1,0x    |
| Mixed (40 ch)    |        | Quiescent | 206,496 ms  | 10,370 ms  | 189,986 ms  | 215,864 ms  | 13,9%       | 1,0x    |
| 200×1P1C         |        | Quiescent | 75,784 ms   | 5,589 ms   | 69,719 ms   | 83,031 ms   | 15,3%       | 1,0x    |
| 200×1P1C buf=1   |        | Quiescent | 1641,577 ms | 32,906 ms  | 1603,715 ms | 1682,242 ms | 13,9%       | 1,0x    |
| XF map 1P1C      | 1024   | Quiescent | 89,306 ms   | 6,050 ms   | 82,624 ms   | 95,877 ms   | 15,1%       | 1,0x    |
| XF map 4P4C      | 1024   | Quiescent | 1623,879 ms | 16,633 ms  | 1601,433 ms | 1643,682 ms | 13,9%       | 1,0x    |
| XF filter 1P1C   | 1024   | Quiescent | 64,506 ms   | 7,559 ms   | 58,429 ms   | 76,943 ms   | 31,2%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | Quiescent | 78,945 ms   | 3,308 ms   | 74,318 ms   | 82,589 ms   | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | Quiescent | 2120,567 ms | 24,836 ms  | 2098,961 ms | 2150,089 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | Quiescent | 2182,831 ms | 22,964 ms  | 2154,950 ms | 2209,211 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | Quiescent | 2143,745 ms | 15,946 ms  | 2122,754 ms | 2160,246 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | Quiescent | 2207,439 ms | 31,790 ms  | 2174,721 ms | 2241,692 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | Quiescent | 2144,101 ms | 15,488 ms  | 2129,105 ms | 2162,839 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | Quiescent | 2193,599 ms | 27,624 ms  | 2159,983 ms | 2228,607 ms | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 436,077 ms  | 15,715 ms  | 416,858 ms  | 452,333 ms  | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 446,916 ms  | 22,694 ms  | 421,166 ms  | 482,159 ms  | 13,9%       | 1,0x    |
| 16P1C            | 64     | Quiescent | 2249,483 ms | 18,122 ms  | 2227,266 ms | 2267,439 ms | 13,9%       | 1,0x    |
| 32P1C            | 64     | Quiescent | 2277,421 ms | 14,961 ms  | 2260,426 ms | 2296,216 ms | 13,9%       | 1,0x    |
| 64P1C            | 64     | Quiescent | 2318,420 ms | 5,340 ms   | 2314,070 ms | 2324,478 ms | 13,9%       | 1,0x    |
| 128P1C           | 64     | Quiescent | 2338,468 ms | 17,227 ms  | 2314,455 ms | 2357,217 ms | 13,9%       | 1,0x    |
| 16P1C            | 1024   | Quiescent | 2246,285 ms | 22,199 ms  | 2215,136 ms | 2271,737 ms | 13,9%       | 1,0x    |
| 32P1C            | 1024   | Quiescent | 2306,001 ms | 21,381 ms  | 2283,882 ms | 2332,789 ms | 13,9%       | 1,0x    |
| 64P1C            | 1024   | Quiescent | 2375,836 ms | 107,010 ms | 2296,390 ms | 2510,150 ms | 13,9%       | 1,0x    |
| 128P1C           | 1024   | Quiescent | 2351,231 ms | 25,112 ms  | 2317,961 ms | 2378,638 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 64     | Quiescent | 2337,109 ms | 18,470 ms  | 2319,242 ms | 2359,477 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 64     | Quiescent | 2367,351 ms | 19,334 ms  | 2344,906 ms | 2393,322 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 64     | Quiescent | 2395,156 ms | 18,725 ms  | 2372,003 ms | 2412,812 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 64     | Quiescent | 2423,970 ms | 19,467 ms  | 2391,803 ms | 2440,167 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 1024   | Quiescent | 2310,271 ms | 23,381 ms  | 2284,707 ms | 2336,557 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 1024   | Quiescent | 2375,297 ms | 43,796 ms  | 2344,342 ms | 2448,413 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 1024   | Quiescent | 2401,932 ms | 11,847 ms  | 2389,324 ms | 2415,257 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 1024   | Quiescent | 2420,049 ms | 34,994 ms  | 2376,142 ms | 2456,899 ms | 13,9%       | 1,0x    |
