# Native ARM/macOS wide-fan-in comparison

User-requested local comparison of the existing :wide-fan-in fixture against the
completed Totem x86 result. Uses the exact source snapshot at commit 32ead33 in
/private/tmp/quiescent-arm-baseline-20261030, with all 125 input files and all 20
compiled Java classes verified byte-identical to the x86 capture.

Apple M1 Max (8 performance + 2 efficiency cores, 64 GiB RAM), native arm64
Corretto 26.0.2+10-FR, macOS 27.0. Match Clojure 1.12.4, core.async 1.9.865,
2 GiB heap, G1, default schedulers, and the full measurement profile. The Mac
retains its default ten available processors; Totem has four. This compares
whole machine/runtime configurations, not ISA alone. OS, core count and mix,
cache/memory system, and background work remain possible contributors.

One case: 24 channels, 128 producers and one consumer/channel, buffer 1024,
96,000 values/channel (2,304,000 total). Four fresh JVMs per implementation,
AB/BA/AB/BA order, ten seconds/three trials minimum warmup, twelve timing trials
and five separate allocation trials per worker. Eight full JVMs, 96 timing and
40 allocation trials, plus two smoke workers and library/harness preflight.
Per-phase timeout remains 300 seconds, matching the Totem capture.

The launcher holds a local advisory lock and uses caffeinate for this capture
only. It does not quit applications or change permanent power/scheduler settings.
Host snapshots, periodic top/vm_stat output and start/end power/thermal state are
retained separately from measurements. Initial samples showed iTerm2 and
WindowServer activity; review the recorded background load before interpreting
small differences. No SSH/server availability is required for this local run.

The integrity check requires identical source/harness/class hashes, dependency
coordinates, Java build, Clojure version, heap, collectors, scheduler override
properties and JVM options (apart from the classpath-basis file location).
Hardware, OS, available processor count, checkout/path and Git metadata differences
are intentional and retained. All raw samples, source snapshots and process logs
remain available. No experimental channel branch is measured.
