## Benchmark Results

| label         | channel        | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|---------------|----------------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C buf=1024 | BoundedChannel | 26,288 ms   | 2,923 ms   | 22,740 ms   | 32,980 ms   | 73,8%       |         |
| 1P1C buf=1024 | core.async     | 212,320 ms  | 17,745 ms  | 183,259 ms  | 240,461 ms  | 61,8%       | 8,1x    |
| 1P4C buf=1024 | BoundedChannel | 56,474 ms   | 1,990 ms   | 53,373 ms   | 60,658 ms   | 22,2%       |         |
| 1P4C buf=1024 | core.async     | 346,598 ms  | 35,727 ms  | 306,755 ms  | 456,629 ms  | 70,4%       | 6,1x    |
| 4P1C buf=1024 | BoundedChannel | 63,424 ms   | 2,307 ms   | 59,251 ms   | 67,592 ms   | 22,3%       |         |
| 4P1C buf=1024 | core.async     | 346,853 ms  | 9,481 ms   | 338,684 ms  | 368,537 ms  | 14,2%       | 5,5x    |
| 4P4C buf=1024 | BoundedChannel | 101,487 ms  | 6,293 ms   | 93,820 ms   | 113,662 ms  | 46,8%       |         |
| 4P4C buf=1024 | core.async     | 253,791 ms  | 1,654 ms   | 251,825 ms  | 257,485 ms  | 1,6%        | 2,5x    |
| Ping-pong     | BoundedChannel | 19,943 ms   | 0,816 ms   | 18,964 ms   | 21,496 ms   | 27,1%       |         |
| Ping-pong     | core.async     | 513,549 ms  | 6,439 ms   | 505,940 ms  | 529,878 ms  | 1,6%        | 25,8x   |
| 1P1C buf=1    | BoundedChannel | 172,479 ms  | 8,954 ms   | 155,770 ms  | 187,349 ms  | 38,5%       |         |
| 1P1C buf=1    | core.async     | 1869,351 ms | 20,412 ms  | 1841,687 ms | 1920,145 ms | 1,6%        | 10,8x   |
| 1P1C buf=16   | BoundedChannel | 105,189 ms  | 25,389 ms  | 59,219 ms   | 138,511 ms  | 92,9%       |         |
| 1P1C buf=16   | core.async     | 454,017 ms  | 20,174 ms  | 432,161 ms  | 492,962 ms  | 30,3%       | 4,3x    |
| 4P4C buf=1    | BoundedChannel | 7156,231 ms | 185,233 ms | 6854,170 ms | 7516,979 ms | 12,6%       |         |
| 4P4C buf=1    | core.async     | 3262,017 ms | 145,638 ms | 3141,904 ms | 3574,233 ms | 30,4%       | 0,5x    |
| 4P4C buf=16   | BoundedChannel | 125,252 ms  | 3,974 ms   | 119,773 ms  | 132,227 ms  | 19,0%       |         |
| 4P4C buf=16   | core.async     | 1168,290 ms | 45,405 ms  | 1134,824 ms | 1273,613 ms | 25,4%       | 9,3x    |
| 50×1P1C       | BoundedChannel | 33,652 ms   | 1,460 ms   | 31,255 ms   | 36,454 ms   | 30,3%       |         |
| 50×1P1C       | core.async     | 571,551 ms  | 22,863 ms  | 548,574 ms  | 649,223 ms  | 27,0%       | 17,0x   |
| 50×4P4C       | BoundedChannel | 55,172 ms   | 2,584 ms   | 52,226 ms   | 60,795 ms   | 33,5%       |         |
| 50×4P4C       | core.async     | 1905,244 ms | 45,378 ms  | 1817,539 ms | 1993,230 ms | 11,0%       | 34,5x   |
| Mixed (40 ch) | BoundedChannel | 38,360 ms   | 1,182 ms   | 36,093 ms   | 40,271 ms   | 17,4%       |         |
| Mixed (40 ch) | core.async     | 1169,248 ms | 27,234 ms  | 1127,866 ms | 1224,978 ms | 11,0%       | 30,5x   |
| 200×1P1C      | BoundedChannel | 64,132 ms   | 2,650 ms   | 59,797 ms   | 69,091 ms   | 27,1%       |         |
| 200×1P1C      | core.async     | 2003,067 ms | 48,676 ms  | 1916,308 ms | 2107,031 ms | 12,5%       | 31,2x   |
