## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|1P1C|1024|BoundedChannel|58,760 ms|13,064 ms|47,069 ms|80,355 ms|64,3%||
|1P1C|1024|Adaptive|109,675 ms|43,616 ms|57,246 ms|164,008 ms|82,2%|1,9x|
|1P1C|1024|Locked|152,239 ms|46,949 ms|111,436 ms|228,360 ms|81,4%|2,6x|
|1P4C|1024|BoundedChannel|65,141 ms|18,264 ms|47,207 ms|88,080 ms|65,2%||
|1P4C|1024|Adaptive|99,047 ms|6,166 ms|88,088 ms|104,462 ms|14,8%|1,5x|
|1P4C|1024|Locked|157,634 ms|19,836 ms|134,627 ms|183,587 ms|31,5%|2,4x|
|4P1C|1024|BoundedChannel|73,878 ms|8,289 ms|64,157 ms|84,438 ms|31,0%||
|4P1C|1024|Adaptive|111,145 ms|5,202 ms|104,697 ms|117,436 ms|13,9%|1,5x|
|4P1C|1024|Locked|151,818 ms|14,392 ms|135,494 ms|171,684 ms|30,1%|2,1x|
|4P4C|1024|BoundedChannel|110,793 ms|12,277 ms|90,683 ms|121,704 ms|31,0%||
|4P4C|1024|Adaptive|145,567 ms|32,519 ms|108,388 ms|196,839 ms|64,3%|1,3x|
|4P4C|1024|Locked|157,236 ms|22,640 ms|131,609 ms|183,281 ms|31,9%|1,4x|
|Ping-pong|1|BoundedChannel|89,705 ms|31,261 ms|66,084 ms|137,563 ms|81,8%||
|Ping-pong|1|Adaptive|222,603 ms|103,806 ms|108,398 ms|347,784 ms|82,5%|2,5x|
|Ping-pong|1|Locked|186,955 ms|62,120 ms|94,569 ms|256,647 ms|81,7%|2,1x|
