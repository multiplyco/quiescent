## Benchmark Results

| label          | buffer | channel        | mean        | std-dev   | lower-q     | upper-q     | outlier-var | speedup |
|----------------|--------|----------------|-------------|-----------|-------------|-------------|-------------|---------|
| 1P1C           | 1024   | BoundedChannel | 28,629 ms   | 2,544 ms  | 23,814 ms   | 32,759 ms   | 63,6%       |         |
| 1P1C           | 1024   | core.async     | 213,965 ms  | 24,884 ms | 182,884 ms  | 270,971 ms  | 75,5%       | 7,5x    |
| 1P4C           | 1024   | BoundedChannel | 66,350 ms   | 1,834 ms  | 63,335 ms   | 69,861 ms   | 14,2%       |         |
| 1P4C           | 1024   | core.async     | 320,485 ms  | 5,907 ms  | 315,839 ms  | 333,242 ms  | 7,8%        | 4,8x    |
| 4P1C           | 1024   | BoundedChannel | 95,947 ms   | 3,631 ms  | 89,434 ms   | 101,969 ms  | 23,9%       |         |
| 4P1C           | 1024   | core.async     | 329,037 ms  | 4,729 ms  | 323,945 ms  | 342,563 ms  | 1,6%        | 3,4x    |
| 4P4C           | 1024   | BoundedChannel | 131,450 ms  | 6,325 ms  | 119,510 ms  | 147,528 ms  | 33,6%       |         |
| 4P4C           | 1024   | core.async     | 225,873 ms  | 2,050 ms  | 223,349 ms  | 231,600 ms  | 1,6%        | 1,7x    |
| Ping-pong      | 1      | BoundedChannel | 19,080 ms   | 0,686 ms  | 18,434 ms   | 20,768 ms   | 22,2%       |         |
| Ping-pong      | 1      | core.async     | 539,815 ms  | 31,188 ms | 519,801 ms  | 623,049 ms  | 43,4%       | 28,3x   |
| 1P1C           | 1      | BoundedChannel | 190,135 ms  | 13,820 ms | 168,519 ms  | 222,399 ms  | 55,1%       |         |
| 1P1C           | 1      | core.async     | 1913,252 ms | 18,278 ms | 1893,355 ms | 1955,930 ms | 1,6%        | 10,1x   |
| 1P1C           | 16     | BoundedChannel | 86,590 ms   | 11,076 ms | 65,389 ms   | 106,147 ms  | 79,0%       |         |
| 1P1C           | 16     | core.async     | 431,123 ms  | 9,733 ms  | 412,955 ms  | 450,092 ms  | 11,0%       | 5,0x    |
| 4P4C           | 1      | BoundedChannel | 7212,994 ms | 97,670 ms | 7021,656 ms | 7412,500 ms | 1,6%        |         |
| 4P4C           | 1      | core.async     | 3161,297 ms | 16,013 ms | 3135,493 ms | 3190,120 ms | 1,6%        | 0,4x    |
| 4P4C           | 16     | BoundedChannel | 161,395 ms  | 23,777 ms | 150,558 ms  | 220,796 ms  | 84,1%       |         |
| 4P4C           | 16     | core.async     | 1176,049 ms | 7,105 ms  | 1165,821 ms | 1189,937 ms | 1,6%        | 7,3x    |
| 50×1P1C        |        | BoundedChannel | 36,259 ms   | 1,053 ms  | 34,143 ms   | 38,091 ms   | 15,8%       |         |
| 50×1P1C        |        | core.async     | 586,436 ms  | 45,291 ms | 528,872 ms  | 647,648 ms  | 56,8%       | 16,2x   |
| 50×4P4C        |        | BoundedChannel | 56,782 ms   | 1,103 ms  | 55,104 ms   | 58,274 ms   | 7,9%        |         |
| 50×4P4C        |        | core.async     | 1906,569 ms | 44,208 ms | 1801,706 ms | 1973,671 ms | 11,0%       | 33,6x   |
| Mixed (40 ch)  |        | BoundedChannel | 39,116 ms   | 1,712 ms  | 37,066 ms   | 43,113 ms   | 30,3%       |         |
| Mixed (40 ch)  |        | core.async     | 1162,304 ms | 19,373 ms | 1108,496 ms | 1194,133 ms | 6,3%        | 29,7x   |
| 200×1P1C       |        | BoundedChannel | 67,411 ms   | 2,048 ms  | 59,936 ms   | 69,803 ms   | 17,4%       |         |
| 200×1P1C       |        | core.async     | 1999,944 ms | 44,707 ms | 1945,943 ms | 2094,285 ms | 11,0%       | 29,7x   |
| XF map 1P1C    | 1024   | BoundedChannel | 101,481 ms  | 2,342 ms  | 94,446 ms   | 104,620 ms  | 11,0%       |         |
| XF map 1P1C    | 1024   | core.async     | 323,069 ms  | 14,148 ms | 295,446 ms  | 349,230 ms  | 30,3%       | 3,2x    |
| XF map 4P4C    | 1024   | BoundedChannel | 298,550 ms  | 10,658 ms | 276,009 ms  | 317,849 ms  | 22,2%       |         |
| XF map 4P4C    | 1024   | core.async     | 338,086 ms  | 2,796 ms  | 334,036 ms  | 344,624 ms  | 1,6%        | 1,1x    |
| XF filter 1P1C | 1024   | BoundedChannel | 45,381 ms   | 3,967 ms  | 43,485 ms   | 55,061 ms   | 63,6%       |         |
| XF filter 1P1C | 1024   | core.async     | 232,728 ms  | 7,118 ms  | 221,419 ms  | 243,618 ms  | 17,4%       | 5,1x    |
| XF mapcat 1P1C | 1024   | BoundedChannel | 58,534 ms   | 3,046 ms  | 54,761 ms   | 66,391 ms   | 38,5%       |         |
| XF mapcat 1P1C | 1024   | core.async     | 236,357 ms  | 4,950 ms  | 227,646 ms  | 247,271 ms  | 9,4%        | 4,0x    |
