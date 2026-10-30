# ARM/macOS versus x86/Linux: wide fan-in

Case: 24 channels, 128 producers and one consumer per channel, buffer 1024; 96,000 values/channel (2,304,000 delivered values/trial).

Local capture completed 2026-10-30T01:43:11+01:00; elapsed 0:58:35, including preflight.

All eight full workers, 96 timing trials, 40 allocation trials and two smoke workers passed. All 44 measurement artifacts were independently checksum-verified. All 125 source files and 20 compiled Java classes match the x86 reference; dependency versions, JDK build, heap, collectors, scheduler override properties and normalized JVM flags match.

## Results

Each time is the median of four independent JVM means. Allocation is whole-JVM transfer bytes per delivered value, measured separately after timing. Ratios are core.async / Quiescent.

| Machine | Quiescent ms | core.async ms | Time ratio | Q bytes/value | async bytes/value | Allocation ratio |
|---|---:|---:|---:|---:|---:|---:|
| M1 Max / macOS | 66.879 | 39093.396 | 584.54x | 100.26 | 873.48 | 8.71x |
| i5-4590S / Linux | 97.516 | 12139.455 | 124.49x | 98.69 | 879.12 | 8.91x |

The relative elapsed-time advantage is **4.70× larger on the Mac**. Quiescent’s Mac elapsed time is 0.686× its Totem time; core.async’s is 3.22×. The main change is therefore core.async taking substantially longer on this machine, alongside a smaller improvement in Quiescent elapsed time. Allocation per value remains close across machines.

The Mac paired time ratios are 613.13x, 570.72x, 585.70x, 598.64x. All observations are retained.

## Scope of the comparison

The Mac is an Apple M1 Max with eight performance and two efficiency cores, 64 GiB RAM, and macOS 27.0. Totem is an Intel i5-4590S with four cores, about 16 GiB RAM, and Linux 7.0.0-38. Both run native Corretto 26.0.2+10-FR, Clojure 1.12.4, core.async 1.9.865, a 2 GiB heap and G1 with default schedulers. Mac Java reports ten processors; Totem reports four. The source snapshot matches commit 32ead33.

This controls the implementation and Java-level harness while changing the complete machine/runtime environment. Architecture, OS synchronization/scheduling, core count and mix, caches/memory, and background activity remain coupled. It does not identify ISA alone as the cause. Absolute timings and ratios are reported separately; samples from the two machines are not pooled.

Timing covers prepared worker release through channel transfer/drain, including runtime scheduling and count/sum bookkeeping. Setup is excluded from elapsed timing and recorded separately for allocation. Transfer allocation includes worker teardown and all JVM/background allocation. The default Quiescent task and core.async go execution models are part of the comparison.

## Host activity and variability

- During quiescent worker lifetimes (25 host samples), median system-wide CPU shares were 30.64% user, 5.12% system and 62.33% idle.
- During core-async worker lifetimes (205 host samples), median system-wide CPU shares were 20.82% user, 78.80% system and 0.10% idle.
- iTerm2 appeared in 231 top-process samples, with median CPU use 54.30% of one core when present. This is recorded background activity, not benchmark JVM work.
- WindowServer appeared in 231 top-process samples, with median CPU use 23.60% of one core when present. This is recorded background activity, not benchmark JVM work.
- com.apple.Virtua appeared in 231 top-process samples, with median CPU use 11.40% of one core when present. This is recorded background activity, not benchmark JVM work.

The vm_stat log contains 351 intervals after its initial cumulative row: 7 with nonzero swap-in, 0 with nonzero swap-out, and 0 with nonzero pageout. Totals: 737280 bytes swapped in, 0 bytes swapped out; periodic cumulative rows are reconciled with interval deltas.

Within-worker timing trends (last three trial mean / first three trial mean):
- quiescent, fork 1: 71.801 ms → 54.083 ms (0.753x).
- core-async, fork 1: 35231.206 ms → 41345.405 ms (1.174x).
- core-async, fork 2: 33274.390 ms → 43613.865 ms (1.311x).
- quiescent, fork 2: 80.239 ms → 51.278 ms (0.639x).
- quiescent, fork 3: 63.308 ms → 67.971 ms (1.074x).
- core-async, fork 3: 37713.632 ms → 43262.197 ms (1.147x).
- core-async, fork 4: 36694.871 ms → 40387.727 ms (1.101x).
- quiescent, fork 4: 63.947 ms → 71.811 ms (1.123x).

These system-wide samples cover complete worker lifetimes, including JVM loading, warmup, setup and allocation, not just timed windows. Background processes contribute to the figures. High system CPU is a useful diagnostic clue for a later synchronization/scheduling profile, not proof of which function or OS mechanism caused the difference.

The core.async last-three/first-three timing ratios range from 1.10× to 1.31× across JVMs. These results are averages for the prescribed finite-batch profile, not an established stationary throughput estimate. All early and late trials are retained. The repeated large ratios support a machine/runtime interaction, but do not prove the original historical 837.6× number or identify architecture alone as its cause.

The high system CPU share during core.async workers makes native synchronization, parking/waking and scheduling useful profiling targets. This is an inference from the host observations, not a stack-level attribution. Background load remained: iTerm2 used roughly half of one core and WindowServer about a quarter of one core in the sampled process lists. The run therefore was not an entirely idle-host experiment. Only 720 KiB was swapped in, with no recorded swap-out or pageout during capture. Start, mid-run and end snapshots show AC power and no thermal/performance warning recorded by pmset; this is not a measurement of CPU frequency.

The initial launch failed in process-list logging before tests or measurements and is retained separately. The successful capture exited zero, and its temporary caffeinate assertion and monitors have exited. Existing user applications and power settings were left intact.

## Evidence

- [Every JVM and trial](measurement/summary.md)
- [Portable measurements](review.json)
- [Host activity summary](host-review.json)
- [SHA-256 manifest](artifact-sha256.txt)
- [Matched x86 reference](../20261029T200137Z-wide-fan-in-32ead33/ASSESSMENT.md)
