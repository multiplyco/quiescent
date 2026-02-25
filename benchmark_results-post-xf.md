## Benchmark Results

| label          | channel        | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|----------------|----------------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C buf=1024  | BoundedChannel | 25,343 ms   | 2,622 ms   | 22,611 ms   | 31,216 ms   | 70,4%       |         |
| 1P1C buf=1024  | core.async     | 207,894 ms  | 18,416 ms  | 180,561 ms  | 245,807 ms  | 63,6%       | 8,2x    |
| 1P4C buf=1024  | BoundedChannel | 59,205 ms   | 2,859 ms   | 54,920 ms   | 65,651 ms   | 33,6%       |         |
| 1P4C buf=1024  | core.async     | 333,114 ms  | 29,541 ms  | 305,909 ms  | 388,260 ms  | 63,6%       | 5,6x    |
| 4P1C buf=1024  | BoundedChannel | 69,248 ms   | 1,579 ms   | 66,218 ms   | 72,102 ms   | 11,0%       |         |
| 4P1C buf=1024  | core.async     | 349,711 ms  | 36,999 ms  | 309,871 ms  | 430,392 ms  | 72,1%       | 5,1x    |
| 4P4C buf=1024  | BoundedChannel | 104,784 ms  | 6,989 ms   | 93,043 ms   | 118,591 ms  | 50,1%       |         |
| 4P4C buf=1024  | core.async     | 243,889 ms  | 6,046 ms   | 233,420 ms  | 255,757 ms  | 12,6%       | 2,3x    |
| Ping-pong      | BoundedChannel | 23,078 ms   | 1,448 ms   | 20,577 ms   | 25,393 ms   | 46,8%       |         |
| Ping-pong      | core.async     | 555,173 ms  | 23,699 ms  | 533,284 ms  | 614,634 ms  | 28,7%       | 24,1x   |
| 1P1C buf=1     | BoundedChannel | 183,464 ms  | 14,900 ms  | 161,480 ms  | 212,887 ms  | 60,2%       |         |
| 1P1C buf=1     | core.async     | 1949,536 ms | 74,545 ms  | 1902,928 ms | 2164,070 ms | 25,4%       | 10,6x   |
| 1P1C buf=16    | BoundedChannel | 81,881 ms   | 18,617 ms  | 63,629 ms   | 123,860 ms  | 92,9%       |         |
| 1P1C buf=16    | core.async     | 453,857 ms  | 17,813 ms  | 418,970 ms  | 488,489 ms  | 25,5%       | 5,5x    |
| 4P4C buf=1     | BoundedChannel | 7362,573 ms | 286,546 ms | 7045,387 ms | 7929,495 ms | 25,4%       |         |
| 4P4C buf=1     | core.async     | 3254,881 ms | 141,435 ms | 3125,414 ms | 3582,810 ms | 30,3%       | 0,4x    |
| 4P4C buf=16    | BoundedChannel | 166,997 ms  | 20,662 ms  | 141,491 ms  | 208,991 ms  | 78,9%       |         |
| 4P4C buf=16    | core.async     | 1197,108 ms | 13,091 ms  | 1181,574 ms | 1226,400 ms | 1,6%        | 7,2x    |
| 50×1P1C        | BoundedChannel | 33,253 ms   | 1,923 ms   | 31,604 ms   | 39,701 ms   | 43,4%       |         |
| 50×1P1C        | core.async     | 539,355 ms  | 16,648 ms  | 511,777 ms  | 573,713 ms  | 17,4%       | 16,2x   |
| 50×4P4C        | BoundedChannel | 61,866 ms   | 1,278 ms   | 59,971 ms   | 64,617 ms   | 9,4%        |         |
| 50×4P4C        | core.async     | 1832,749 ms | 64,089 ms  | 1760,687 ms | 1971,722 ms | 22,2%       | 29,6x   |
| Mixed (40 ch)  | BoundedChannel | 42,702 ms   | 2,052 ms   | 39,502 ms   | 46,491 ms   | 33,6%       |         |
| Mixed (40 ch)  | core.async     | 1099,010 ms | 31,569 ms  | 1053,044 ms | 1170,326 ms | 15,8%       | 25,7x   |
| 200×1P1C       | BoundedChannel | 61,123 ms   | 3,799 ms   | 58,048 ms   | 71,817 ms   | 46,8%       |         |
| 200×1P1C       | core.async     | 1877,785 ms | 38,420 ms  | 1792,178 ms | 1942,157 ms | 9,4%        | 30,7x   |
| XF map 1P1C    | BoundedChannel | 77,955 ms   | 3,143 ms   | 73,216 ms   | 85,085 ms   | 27,0%       |         |
| XF map 1P1C    | core.async     | 268,819 ms  | 14,363 ms  | 244,146 ms  | 296,907 ms  | 38,5%       | 3,4x    |
| XF map 4P4C    | BoundedChannel | 246,902 ms  | 33,635 ms  | 172,877 ms  | 302,669 ms  | 80,7%       |         |
| XF map 4P4C    | core.async     | 323,415 ms  | 6,459 ms   | 315,991 ms  | 337,878 ms  | 7,9%        | 1,3x    |
| XF filter 1P1C | BoundedChannel | 48,397 ms   | 2,109 ms   | 44,018 ms   | 51,500 ms   | 30,3%       |         |
| XF filter 1P1C | core.async     | 209,156 ms  | 10,955 ms  | 189,958 ms  | 230,553 ms  | 38,5%       | 4,3x    |
| XF mapcat 1P1C | BoundedChannel | 60,225 ms   | 5,340 ms   | 51,602 ms   | 70,791 ms   | 63,6%       |         |
| XF mapcat 1P1C | core.async     | 229,706 ms  | 4,692 ms   | 222,768 ms  | 239,174 ms  | 9,4%        | 3,8x    |
