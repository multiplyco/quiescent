## Benchmark Results

| label            | buffer | channel   | mean        | std-dev   | lower-q     | upper-q     | outlier-var | speedup |
|------------------|--------|-----------|-------------|-----------|-------------|-------------|-------------|---------|
| 1P1C             | 1024   | Quiescent | 95,295 ms   | 10,708 ms | 82,782 ms   | 107,115 ms  | 31,0%       | 1,0x    |
| 1P4C             | 1024   | Quiescent | 1071,456 ms | 18,730 ms | 1050,016 ms | 1095,385 ms | 13,9%       | 1,0x    |
| 4P1C             | 1024   | Quiescent | 2281,707 ms | 54,183 ms | 2224,553 ms | 2343,647 ms | 13,9%       | 1,0x    |
| 4P4C             | 1024   | Quiescent | 1688,235 ms | 24,246 ms | 1663,381 ms | 1721,685 ms | 13,9%       | 1,0x    |
| Ping-pong        | 1      | Quiescent | 169,561 ms  | 4,900 ms  | 163,844 ms  | 174,762 ms  | 13,9%       | 1,0x    |
| 1P1C             | 1      | Quiescent | 1750,083 ms | 56,230 ms | 1684,260 ms | 1810,929 ms | 13,9%       | 1,0x    |
| 1P1C             | 16     | Quiescent | 235,844 ms  | 37,243 ms | 191,222 ms  | 275,410 ms  | 47,4%       | 1,0x    |
| 4P4C             | 1      | Quiescent | 3261,776 ms | 63,632 ms | 3148,494 ms | 3323,816 ms | 13,9%       | 1,0x    |
| 4P4C             | 16     | Quiescent | 1796,198 ms | 29,663 ms | 1761,390 ms | 1825,802 ms | 13,9%       | 1,0x    |
| 50×1P1C          |        | Quiescent | 41,657 ms   | 2,394 ms  | 37,823 ms   | 44,142 ms   | 14,5%       | 1,0x    |
| 50×4P4C          |        | Quiescent | 852,376 ms  | 33,614 ms | 815,128 ms  | 887,535 ms  | 13,9%       | 1,0x    |
| Mixed (40 ch)    |        | Quiescent | 227,932 ms  | 18,225 ms | 208,086 ms  | 247,432 ms  | 15,5%       | 1,0x    |
| 200×1P1C         |        | Quiescent | 75,718 ms   | 5,238 ms  | 70,692 ms   | 82,138 ms   | 15,2%       | 1,0x    |
| 200×1P1C buf=1   |        | Quiescent | 1692,353 ms | 21,290 ms | 1667,325 ms | 1714,861 ms | 13,9%       | 1,0x    |
| 24×16P1C         |        | Quiescent | 189,484 ms  | 22,961 ms | 161,220 ms  | 211,692 ms  | 31,4%       | 1,0x    |
| 24×32P1C         |        | Quiescent | 187,370 ms  | 11,058 ms | 176,581 ms  | 201,607 ms  | 14,6%       | 1,0x    |
| 24×64P1C         |        | Quiescent | 194,151 ms  | 10,278 ms | 174,862 ms  | 201,767 ms  | 14,1%       | 1,0x    |
| 24×128P1C        |        | Quiescent | 186,220 ms  | 14,301 ms | 167,514 ms  | 202,679 ms  | 15,4%       | 1,0x    |
| XF map 1P1C      | 1024   | Quiescent | 120,733 ms  | 19,703 ms | 89,691 ms   | 139,530 ms  | 47,6%       | 1,0x    |
| XF map 4P4C      | 1024   | Quiescent | 1719,698 ms | 18,574 ms | 1702,850 ms | 1739,842 ms | 13,9%       | 1,0x    |
| XF filter 1P1C   | 1024   | Quiescent | 76,681 ms   | 8,609 ms  | 65,374 ms   | 86,627 ms   | 31,0%       | 1,0x    |
| XF mapcat 1P1C   | 1024   | Quiescent | 84,954 ms   | 7,465 ms  | 76,590 ms   | 92,365 ms   | 15,7%       | 1,0x    |
| Pipe 4P→1P→4C    | 16     | Quiescent | 2175,347 ms | 13,048 ms | 2153,798 ms | 2189,227 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 16     | Quiescent | 2245,025 ms | 22,767 ms | 2226,408 ms | 2274,187 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 64     | Quiescent | 2186,973 ms | 21,219 ms | 2161,287 ms | 2211,226 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 64     | Quiescent | 2230,160 ms | 49,894 ms | 2160,795 ms | 2284,975 ms | 13,9%       | 1,0x    |
| Pipe 4P→1P→4C    | 1024   | Quiescent | 2205,349 ms | 45,862 ms | 2145,245 ms | 2250,126 ms | 13,9%       | 1,0x    |
| Pipe XF 4P→1P→4C | 1024   | Quiescent | 2288,053 ms | 23,136 ms | 2266,704 ms | 2316,045 ms | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 487,342 ms  | 15,391 ms | 466,472 ms  | 508,228 ms  | 13,9%       | 1,0x    |
| 20×Pipe 4P→1P→4C |        | Quiescent | 491,726 ms  | 25,392 ms | 463,325 ms  | 519,465 ms  | 14,0%       | 1,0x    |
| 16P1C            | 64     | Quiescent | 2297,412 ms | 21,305 ms | 2272,895 ms | 2324,304 ms | 13,9%       | 1,0x    |
| 32P1C            | 64     | Quiescent | 2364,625 ms | 17,118 ms | 2342,173 ms | 2380,850 ms | 13,9%       | 1,0x    |
| 64P1C            | 64     | Quiescent | 2385,471 ms | 13,832 ms | 2368,111 ms | 2401,468 ms | 13,9%       | 1,0x    |
| 128P1C           | 64     | Quiescent | 2408,032 ms | 17,897 ms | 2386,419 ms | 2426,209 ms | 13,9%       | 1,0x    |
| 16P1C            | 1024   | Quiescent | 2271,474 ms | 9,816 ms  | 2261,941 ms | 2282,441 ms | 13,9%       | 1,0x    |
| 32P1C            | 1024   | Quiescent | 2334,561 ms | 28,602 ms | 2295,647 ms | 2363,652 ms | 13,9%       | 1,0x    |
| 64P1C            | 1024   | Quiescent | 2451,046 ms | 70,043 ms | 2334,447 ms | 2519,579 ms | 13,9%       | 1,0x    |
| 128P1C           | 1024   | Quiescent | 2411,279 ms | 21,379 ms | 2387,293 ms | 2434,443 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 64     | Quiescent | 2354,225 ms | 12,421 ms | 2339,627 ms | 2370,845 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 64     | Quiescent | 2422,178 ms | 11,529 ms | 2410,561 ms | 2434,527 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 64     | Quiescent | 2465,755 ms | 32,991 ms | 2429,000 ms | 2497,174 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 64     | Quiescent | 2471,910 ms | 35,116 ms | 2417,285 ms | 2507,980 ms | 13,9%       | 1,0x    |
| XF 16P1C         | 1024   | Quiescent | 2370,831 ms | 74,443 ms | 2241,803 ms | 2441,837 ms | 13,9%       | 1,0x    |
| XF 32P1C         | 1024   | Quiescent | 2474,895 ms | 29,965 ms | 2434,638 ms | 2509,408 ms | 13,9%       | 1,0x    |
| XF 64P1C         | 1024   | Quiescent | 2497,004 ms | 11,134 ms | 2480,140 ms | 2506,532 ms | 13,9%       | 1,0x    |
| XF 128P1C        | 1024   | Quiescent | 2490,213 ms | 53,291 ms | 2431,292 ms | 2548,506 ms | 13,9%       | 1,0x    |
