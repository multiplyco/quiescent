## Benchmark Results

| label            | buffer | channel   | mean        | std-dev   | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|-----------|-------------|-----------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | Quiescent | 80,165 ms   | 11,462 ms | 61,504 ms   | 91,585 ms   | 31,9%       | 1,0x    |
| 1P4C             | 1024   | Quiescent | 141,949 ms  | 9,557 ms  | 132,184 ms  | 152,905 ms  | 15,1%       | 1,0x    |
| 4P1C             | 1024   | Quiescent | 198,598 ms  | 50,485 ms | 147,140 ms  | 269,724 ms  | 64,9%       | 1,0x    |
| 4P4C             | 1024   | Quiescent | 235,245 ms  | 31,425 ms | 196,928 ms  | 272,083 ms  | 31,7%       | 1,0x    |
| Ping-pong        | 1      | Quiescent | 166,319 ms  | 28,137 ms | 137,224 ms  | 192,320 ms  | 47,7%       | 1,0x    |
| 1P1C             | 1      | Quiescent | 1567,270 ms | 39,110 ms | 1535,220 ms | 1621,299 ms | 13,9%       | 1,0x    |
| 1P1C             | 16     | Quiescent | 219,809 ms  | 83,062 ms | 157,780 ms  | 357,406 ms  | 82,1%       | 1,0x    |
| 4P4C             | 1      | Quiescent | 2148,668 ms | 62,851 ms | 2062,269 ms | 2210,482 ms | 13,9%       | 1,0x    |
| 4P4C             | 16     | Quiescent | 623,094 ms  | 10,546 ms | 611,264 ms  | 636,044 ms  | 13,9%       | 1,0x    |
| 50×1P1C          |        | Quiescent | 37,359 ms   | 2,624 ms  | 33,740 ms   | 40,220 ms   | 15,2%       | 1,0x    |
| 50×4P4C          |        | Quiescent | 86,851 ms   | 3,429 ms  | 82,702 ms   | 91,178 ms   | 13,9%       | 1,0x    |
| Mixed (40 ch)    |        | Quiescent | 59,965 ms   | 2,728 ms  | 55,749 ms   | 62,668 ms   | 13,9%       | 1,0x    |
| 200×1P1C         |        | Quiescent | 66,751 ms   | 0,889 ms  | 65,343 ms   | 67,572 ms   | 13,9%       | 1,0x    |
| 200×1P1C buf=1   |        | Quiescent | 1689,607 ms | 19,436 ms | 1652,177 ms | 1703,083 ms | 13,9%       | 1,0x    |
| XF map 1P1C      | 1024   | Quiescent | 127,861 ms  | 36,327 ms | 88,540 ms   | 167,058 ms  | 65,2%       | 1,0x    |
| XF map 4P4C      | 1024   | Quiescent | 235,475 ms  | 21,309 ms | 214,260 ms  | 266,427 ms  | 15,8%       | 1,0x    |
| XF filter 1P1C   | 1024   | Quiescent | 88,164 ms   | 23,588 ms | 64,997 ms   | 123,452 ms  | 65,0%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | Quiescent | 102,703 ms  | 21,453 ms | 85,159 ms   | 137,367 ms  | 64,0%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | Quiescent | 921,139 ms  | 28,611 ms | 889,798 ms  | 954,487 ms  | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | Quiescent | 908,772 ms  | 27,623 ms | 874,773 ms  | 932,888 ms  | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | Quiescent | 389,175 ms  | 16,160 ms | 367,884 ms  | 407,785 ms  | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | Quiescent | 412,315 ms  | 7,440 ms  | 405,589 ms  | 421,821 ms  | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | Quiescent | 244,903 ms  | 17,342 ms | 225,334 ms  | 265,515 ms  | 15,2%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | Quiescent | 260,408 ms  | 17,680 ms | 241,292 ms  | 281,576 ms  | 15,1%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 60,517 ms   | 2,552 ms  | 57,822 ms   | 63,556 ms   | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 28,398 ms   | 2,182 ms  | 26,236 ms   | 30,521 ms   | 15,4%       | 1,0x    |
| 16P1C            | 64     | Quiescent | 476,488 ms  | 11,165 ms | 463,646 ms  | 488,698 ms  | 13,9%       | 1,0x    |
| 32P1C            | 64     | Quiescent | 454,668 ms  | 22,893 ms | 427,666 ms  | 479,832 ms  | 13,9%       | 1,0x    |
| 64P1C            | 64     | Quiescent | 481,409 ms  | 23,171 ms | 456,117 ms  | 513,463 ms  | 13,9%       | 1,0x    |
| 128P1C           | 64     | Quiescent | 556,384 ms  | 8,692 ms  | 548,417 ms  | 570,540 ms  | 13,9%       | 1,0x    |
| 16P1C            | 1024   | Quiescent | 424,954 ms  | 22,163 ms | 388,320 ms  | 446,389 ms  | 14,0%       | 1,0x    |
| 32P1C            | 1024   | Quiescent | 392,952 ms  | 12,849 ms | 380,204 ms  | 408,562 ms  | 13,9%       | 1,0x    |
| 64P1C            | 1024   | Quiescent | 364,798 ms  | 24,713 ms | 345,391 ms  | 403,009 ms  | 15,1%       | 1,0x    |
| 128P1C           | 1024   | Quiescent | 388,260 ms  | 24,252 ms | 357,931 ms  | 420,244 ms  | 14,8%       | 1,0x    |
| XF 16P1C         | 64     | Quiescent | 600,894 ms  | 24,356 ms | 571,636 ms  | 624,158 ms  | 13,9%       | 1,0x    |
| XF 32P1C         | 64     | Quiescent | 545,869 ms  | 12,327 ms | 531,799 ms  | 558,632 ms  | 13,9%       | 1,0x    |
| XF 64P1C         | 64     | Quiescent | 547,933 ms  | 15,639 ms | 521,746 ms  | 563,134 ms  | 13,9%       | 1,0x    |
| XF 128P1C        | 64     | Quiescent | 630,236 ms  | 19,157 ms | 611,841 ms  | 650,874 ms  | 13,9%       | 1,0x    |
| XF 16P1C         | 1024   | Quiescent | 554,732 ms  | 36,039 ms | 500,549 ms  | 590,564 ms  | 15,0%       | 1,0x    |
| XF 32P1C         | 1024   | Quiescent | 466,422 ms  | 18,408 ms | 450,178 ms  | 489,216 ms  | 13,9%       | 1,0x    |
| XF 64P1C         | 1024   | Quiescent | 379,922 ms  | 20,168 ms | 362,842 ms  | 412,166 ms  | 14,1%       | 1,0x    |
| XF 128P1C        | 1024   | Quiescent | 425,071 ms  | 31,642 ms | 381,909 ms  | 452,431 ms  | 15,4%       | 1,0x    |
