## Benchmark Results

| label          | buffer | channel        | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|----------------|--------|----------------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C           | 1024   | BoundedChannel | 27,905 ms   | 1,977 ms   | 24,864 ms   | 31,910 ms   | 53,4%       |         |
| 1P1C           | 1024   | core.async     | 208,594 ms  | 21,087 ms  | 181,842 ms  | 242,944 ms  | 70,4%       | 7,5x    |
| 1P4C           | 1024   | BoundedChannel | 67,344 ms   | 2,027 ms   | 63,300 ms   | 70,995 ms   | 17,4%       |         |
| 1P4C           | 1024   | core.async     | 320,512 ms  | 11,062 ms  | 313,222 ms  | 333,224 ms  | 20,6%       | 4,8x    |
| 4P1C           | 1024   | BoundedChannel | 95,766 ms   | 3,620 ms   | 86,477 ms   | 102,767 ms  | 23,9%       |         |
| 4P1C           | 1024   | core.async     | 319,887 ms  | 3,355 ms   | 316,387 ms  | 329,561 ms  | 1,6%        | 3,3x    |
| 4P4C           | 1024   | BoundedChannel | 129,691 ms  | 2,576 ms   | 123,916 ms  | 134,127 ms  | 7,9%        |         |
| 4P4C           | 1024   | core.async     | 224,417 ms  | 2,343 ms   | 221,616 ms  | 230,291 ms  | 1,6%        | 1,7x    |
| Ping-pong      | 1      | BoundedChannel | 19,339 ms   | 0,557 ms   | 18,454 ms   | 20,558 ms   | 15,8%       |         |
| Ping-pong      | 1      | core.async     | 543,527 ms  | 8,668 ms   | 535,101 ms  | 559,901 ms  | 1,6%        | 28,1x   |
| 1P1C           | 1      | BoundedChannel | 185,962 ms  | 17,015 ms  | 157,766 ms  | 218,522 ms  | 65,3%       |         |
| 1P1C           | 1      | core.async     | 1915,647 ms | 20,949 ms  | 1888,865 ms | 1968,785 ms | 1,6%        | 10,3x   |
| 1P1C           | 16     | BoundedChannel | 90,383 ms   | 12,112 ms  | 71,381 ms   | 112,073 ms  | 80,7%       |         |
| 1P1C           | 16     | core.async     | 445,814 ms  | 4,584 ms   | 437,627 ms  | 454,648 ms  | 1,6%        | 4,9x    |
| 4P4C           | 1      | BoundedChannel | 7319,548 ms | 149,990 ms | 7026,833 ms | 7740,477 ms | 9,4%        |         |
| 4P4C           | 1      | core.async     | 3171,003 ms | 107,647 ms | 3135,058 ms | 3239,589 ms | 20,6%       | 0,4x    |
| 4P4C           | 16     | BoundedChannel | 155,279 ms  | 4,803 ms   | 149,685 ms  | 163,101 ms  | 17,4%       |         |
| 4P4C           | 16     | core.async     | 1168,281 ms | 6,528 ms   | 1156,221 ms | 1183,023 ms | 1,6%        | 7,5x    |
| 50×1P1C        |        | BoundedChannel | 37,737 ms   | 1,663 ms   | 33,709 ms   | 41,448 ms   | 30,3%       |         |
| 50×1P1C        |        | core.async     | 579,946 ms  | 6,928 ms   | 568,778 ms  | 590,546 ms  | 1,6%        | 15,4x   |
| 50×4P4C        |        | BoundedChannel | 59,000 ms   | 1,021 ms   | 57,389 ms   | 60,763 ms   | 6,3%        |         |
| 50×4P4C        |        | core.async     | 1949,573 ms | 32,872 ms  | 1872,008 ms | 2015,737 ms | 6,3%        | 33,0x   |
| Mixed (40 ch)  |        | BoundedChannel | 40,524 ms   | 0,976 ms   | 38,877 ms   | 42,454 ms   | 11,1%       |         |
| Mixed (40 ch)  |        | core.async     | 1120,729 ms | 64,500 ms  | 1016,865 ms | 1208,435 ms | 43,4%       | 27,7x   |
| 200×1P1C       |        | BoundedChannel | 68,897 ms   | 1,933 ms   | 64,751 ms   | 72,399 ms   | 15,7%       |         |
| 200×1P1C       |        | core.async     | 1999,174 ms | 25,732 ms  | 1953,744 ms | 2035,399 ms | 1,6%        | 29,0x   |
| XF map 1P1C    | 1024   | BoundedChannel | 100,525 ms  | 4,915 ms   | 94,999 ms   | 113,283 ms  | 35,2%       |         |
| XF map 1P1C    | 1024   | core.async     | 281,136 ms  | 19,523 ms  | 249,909 ms  | 315,293 ms  | 51,8%       | 2,8x    |
| XF map 4P4C    | 1024   | BoundedChannel | 294,474 ms  | 15,523 ms  | 278,954 ms  | 319,124 ms  | 38,5%       |         |
| XF map 4P4C    | 1024   | core.async     | 323,833 ms  | 14,236 ms  | 313,573 ms  | 358,406 ms  | 30,3%       | 1,1x    |
| XF filter 1P1C | 1024   | BoundedChannel | 43,418 ms   | 0,889 ms   | 42,557 ms   | 45,985 ms   | 9,4%        |         |
| XF filter 1P1C | 1024   | core.async     | 209,420 ms  | 6,574 ms   | 196,225 ms  | 220,924 ms  | 18,9%       | 4,8x    |
| XF mapcat 1P1C | 1024   | BoundedChannel | 59,495 ms   | 3,720 ms   | 56,506 ms   | 65,633 ms   | 46,8%       |         |
| XF mapcat 1P1C | 1024   | core.async     | 241,539 ms  | 4,039 ms   | 231,671 ms  | 247,453 ms  | 6,3%        | 4,1x    |
