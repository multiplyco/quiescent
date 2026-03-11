## Benchmark Results

| label            | buffer | channel   | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|-----------|-------------|------------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | Quiescent | 106,277 ms  | 9,925 ms   | 94,170 ms   | 115,920 ms  | 15,8%       | 1,0x    |
| 1P4C             | 1024   | Quiescent | 309,104 ms  | 5,027 ms   | 303,904 ms  | 314,421 ms  | 13,9%       | 1,0x    |
| 4P1C             | 1024   | Quiescent | 368,175 ms  | 8,316 ms   | 358,893 ms  | 376,802 ms  | 13,9%       | 1,0x    |
| 4P4C             | 1024   | Quiescent | 542,368 ms  | 25,687 ms  | 495,746 ms  | 562,214 ms  | 13,9%       | 1,0x    |
| Ping-pong        | 1      | Quiescent | 162,493 ms  | 5,203 ms   | 157,023 ms  | 169,577 ms  | 13,9%       | 1,0x    |
| 1P1C             | 1      | Quiescent | 1465,037 ms | 262,316 ms | 1211,685 ms | 1770,529 ms | 48,0%       | 1,0x    |
| 1P1C             | 16     | Quiescent | 223,387 ms  | 28,804 ms  | 188,979 ms  | 260,961 ms  | 31,6%       | 1,0x    |
| 4P4C             | 1      | Quiescent | 5552,200 ms | 75,933 ms  | 5413,998 ms | 5618,918 ms | 13,9%       | 1,0x    |
| 4P4C             | 16     | Quiescent | 1166,515 ms | 19,669 ms  | 1147,403 ms | 1189,052 ms | 13,9%       | 1,0x    |
| 50×1P1C          |        | Quiescent | 50,698 ms   | 2,934 ms   | 47,052 ms   | 54,069 ms   | 14,5%       | 1,0x    |
| 50×4P4C          |        | Quiescent | 79,819 ms   | 4,827 ms   | 75,409 ms   | 86,673 ms   | 14,7%       | 1,0x    |
| Mixed (40 ch)    |        | Quiescent | 65,385 ms   | 4,563 ms   | 58,967 ms   | 70,353 ms   | 15,2%       | 1,0x    |
| 200×1P1C         |        | Quiescent | 89,321 ms   | 4,749 ms   | 83,902 ms   | 94,061 ms   | 14,1%       | 1,0x    |
| 200×1P1C buf=1   |        | Quiescent | 1534,785 ms | 51,347 ms  | 1475,944 ms | 1598,096 ms | 13,9%       | 1,0x    |
| 24×16P1C         | 1024   | Quiescent | 38,966 ms   | 3,771 ms   | 35,661 ms   | 43,475 ms   | 30,2%       | 1,0x    |
| 24×32P1C         | 1024   | Quiescent | 46,512 ms   | 3,758 ms   | 41,587 ms   | 50,920 ms   | 15,6%       | 1,0x    |
| 24×64P1C         | 1024   | Quiescent | 47,968 ms   | 4,002 ms   | 41,967 ms   | 52,315 ms   | 15,6%       | 1,0x    |
| 24×128P1C        | 1024   | Quiescent | 54,411 ms   | 11,638 ms  | 43,749 ms   | 67,246 ms   | 64,1%       | 1,0x    |
| XF map 1P1C      | 1024   | Quiescent | 106,367 ms  | 7,892 ms   | 95,726 ms   | 115,219 ms  | 15,4%       | 1,0x    |
| XF map 4P4C      | 1024   | Quiescent | 617,175 ms  | 12,545 ms  | 601,134 ms  | 630,423 ms  | 13,9%       | 1,0x    |
| XF filter 1P1C   | 1024   | Quiescent | 84,537 ms   | 5,106 ms   | 79,329 ms   | 90,180 ms   | 14,7%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | Quiescent | 128,387 ms  | 22,715 ms  | 101,872 ms  | 152,909 ms  | 47,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | Quiescent | 1023,939 ms | 34,687 ms  | 977,903 ms  | 1053,986 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | Quiescent | 1129,954 ms | 12,782 ms  | 1117,500 ms | 1148,812 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | Quiescent | 705,118 ms  | 6,367 ms   | 697,689 ms  | 713,846 ms  | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | Quiescent | 767,256 ms  | 13,076 ms  | 749,018 ms  | 782,113 ms  | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | Quiescent | 517,238 ms  | 12,457 ms  | 503,600 ms  | 531,204 ms  | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | Quiescent | 626,578 ms  | 8,195 ms   | 621,326 ms  | 639,148 ms  | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 62,130 ms   | 4,072 ms   | 55,376 ms   | 66,214 ms   | 15,0%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 47,770 ms   | 2,672 ms   | 45,257 ms   | 51,609 ms   | 14,4%       | 1,0x    |
| 16P1C            | 64     | Quiescent | 722,232 ms  | 8,618 ms   | 710,581 ms  | 731,750 ms  | 13,9%       | 1,0x    |
| 32P1C            | 64     | Quiescent | 793,589 ms  | 10,083 ms  | 776,022 ms  | 804,614 ms  | 13,9%       | 1,0x    |
| 64P1C            | 64     | Quiescent | 879,661 ms  | 13,516 ms  | 866,781 ms  | 897,141 ms  | 13,9%       | 1,0x    |
| 128P1C           | 64     | Quiescent | 951,818 ms  | 70,395 ms  | 886,484 ms  | 1032,373 ms | 15,3%       | 1,0x    |
| 16P1C            | 1024   | Quiescent | 409,871 ms  | 6,848 ms   | 403,153 ms  | 417,551 ms  | 13,9%       | 1,0x    |
| 32P1C            | 1024   | Quiescent | 444,609 ms  | 8,445 ms   | 433,429 ms  | 454,637 ms  | 13,9%       | 1,0x    |
| 64P1C            | 1024   | Quiescent | 422,783 ms  | 23,872 ms  | 385,790 ms  | 445,939 ms  | 14,4%       | 1,0x    |
| 128P1C           | 1024   | Quiescent | 358,194 ms  | 15,239 ms  | 331,283 ms  | 371,048 ms  | 13,9%       | 1,0x    |
| XF 16P1C         | 64     | Quiescent | 736,603 ms  | 8,688 ms   | 726,371 ms  | 744,473 ms  | 13,9%       | 1,0x    |
| XF 32P1C         | 64     | Quiescent | 809,156 ms  | 28,716 ms  | 787,193 ms  | 856,796 ms  | 13,9%       | 1,0x    |
| XF 64P1C         | 64     | Quiescent | 888,984 ms  | 8,077 ms   | 879,271 ms  | 900,170 ms  | 13,9%       | 1,0x    |
| XF 128P1C        | 64     | Quiescent | 945,067 ms  | 69,606 ms  | 887,624 ms  | 1036,539 ms | 15,3%       | 1,0x    |
| XF 16P1C         | 1024   | Quiescent | 439,220 ms  | 9,182 ms   | 426,094 ms  | 449,641 ms  | 13,9%       | 1,0x    |
| XF 32P1C         | 1024   | Quiescent | 469,337 ms  | 7,438 ms   | 460,106 ms  | 476,242 ms  | 13,9%       | 1,0x    |
| XF 64P1C         | 1024   | Quiescent | 429,209 ms  | 12,918 ms  | 411,094 ms  | 444,355 ms  | 13,9%       | 1,0x    |
| XF 128P1C        | 1024   | Quiescent | 363,083 ms  | 7,134 ms   | 350,969 ms  | 370,077 ms  | 13,9%       | 1,0x    |
