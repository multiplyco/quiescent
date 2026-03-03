## Benchmark Results

|label|buffer|channel|mean|std-dev|lower-q|upper-q|outlier-var|speedup|
|---|---|---|---|---|---|---|---|---|
|XF map 1P1C|1024|BoundedChannel|91,671 ms|3,527 ms|87,478 ms|95,954 ms|13,9%||
|XF map 1P1C|1024|Adaptive|128,774 ms|10,683 ms|117,981 ms|141,160 ms|15,6%|1,4x|
|XF map 1P1C|1024|Locked|175,691 ms|51,203 ms|131,498 ms|259,412 ms|65,3%|1,9x|
|XF map 1P1C|1024|core.async|285,430 ms|38,772 ms|242,550 ms|327,891 ms|31,8%|3,1x|
|XF map 4P4C|1024|BoundedChannel|333,842 ms|30,119 ms|298,196 ms|362,751 ms|15,8%|1,9x|
|XF map 4P4C|1024|Adaptive|178,954 ms|13,353 ms|167,159 ms|199,984 ms|15,4%||
|XF map 4P4C|1024|Locked|261,300 ms|7,024 ms|252,784 ms|268,621 ms|13,9%|1,5x|
|XF map 4P4C|1024|core.async|387,422 ms|18,636 ms|370,182 ms|408,342 ms|13,9%|2,2x|
|XF filter 1P1C|1024|BoundedChannel|59,274 ms|7,724 ms|51,082 ms|69,248 ms|31,6%||
|XF filter 1P1C|1024|Adaptive|86,454 ms|14,993 ms|70,198 ms|104,140 ms|47,8%|1,5x|
|XF filter 1P1C|1024|Locked|128,915 ms|52,057 ms|70,136 ms|196,481 ms|82,2%|2,2x|
|XF filter 1P1C|1024|core.async|181,298 ms|4,859 ms|177,016 ms|187,447 ms|13,9%|3,1x|
|XF mapcat 1P1C|1024|BoundedChannel|60,841 ms|6,442 ms|55,438 ms|71,076 ms|30,8%||
|XF mapcat 1P1C|1024|Adaptive|112,729 ms|9,742 ms|103,084 ms|122,725 ms|15,7%|1,9x|
|XF mapcat 1P1C|1024|Locked|172,020 ms|45,637 ms|139,371 ms|241,013 ms|65,0%|2,8x|
|XF mapcat 1P1C|1024|core.async|222,668 ms|13,532 ms|206,270 ms|239,326 ms|14,7%|3,7x|
