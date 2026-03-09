## Benchmark Results

| label  | buffer | channel        | mean        | std-dev    | lower-q     | upper-q     | outlier-var | speedup |
|--------|--------|----------------|-------------|------------|-------------|-------------|-------------|---------|
| 16P1C  | 64     | BoundedChannel | 663,716 ms  | 18,254 ms  | 629,476 ms  | 709,891 ms  | 14,2%       |         |
| 16P1C  | 64     | core.async     | 2895,663 ms | 226,814 ms | 2659,700 ms | 3300,366 ms | 58,5%       | 4,4x    |
| 32P1C  | 64     | BoundedChannel | 777,892 ms  | 33,508 ms  | 745,090 ms  | 855,474 ms  | 28,7%       |         |
| 32P1C  | 64     | core.async     | 4744,068 ms | 163,347 ms | 4243,366 ms | 4890,963 ms | 20,6%       | 6,1x    |
| 64P1C  | 64     | BoundedChannel | 1156,240 ms | 62,034 ms  | 1072,960 ms | 1268,645 ms | 38,6%       |         |
| 64P1C  | 64     | core.async     | 5770,033 ms | 122,019 ms | 5528,462 ms | 5956,601 ms | 9,4%        | 5,0x    |
| 128P1C | 64     | BoundedChannel | 5601,658 ms | 599,692 ms | 4468,860 ms | 6671,738 ms | 72,1%       |         |
| 128P1C | 64     | core.async     | 7455,747 ms | 109,239 ms | 7170,684 ms | 7638,177 ms | 1,6%        | 1,3x    |
| 16P1C  | 1024   | BoundedChannel | 143,888 ms  | 15,132 ms  | 137,279 ms  | 190,952 ms  | 72,1%       |         |
| 16P1C  | 1024   | core.async     | 2021,106 ms | 163,379 ms | 1828,934 ms | 2353,948 ms | 60,2%       | 14,0x   |
| 32P1C  | 1024   | BoundedChannel | 170,069 ms  | 4,023 ms   | 163,183 ms  | 176,711 ms  | 11,0%       |         |
| 32P1C  | 1024   | core.async     | 4749,333 ms | 281,464 ms | 3608,243 ms | 4940,897 ms | 43,5%       | 27,9x   |
| 64P1C  | 1024   | BoundedChannel | 277,419 ms  | 13,680 ms  | 252,433 ms  | 293,903 ms  | 35,2%       |         |
| 64P1C  | 1024   | core.async     | 5804,535 ms | 211,379 ms | 4857,306 ms | 5974,190 ms | 22,3%       | 20,9x   |
| 128P1C | 1024   | BoundedChannel | 288,702 ms  | 4,339 ms   | 280,204 ms  | 295,956 ms  | 1,6%        |         |
| 128P1C | 1024   | core.async     | 7377,502 ms | 433,382 ms | 6024,006 ms | 7633,173 ms | 43,5%       | 25,6x   |
