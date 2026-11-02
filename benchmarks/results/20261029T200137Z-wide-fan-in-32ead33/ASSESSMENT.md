# Isolated wide-fan-in result

Completed **2026-10-29 at 21:27:54 Europe/Stockholm**, after 23 minutes 41 seconds including preflight.

Full-profile capture of **24 channels × 128 producers / 1 consumer, buffer 1024**, using 96,000 inputs per channel: 2,304,000 delivered values per trial.

All **eight fresh JVM workers, 96 timing trials, 40 allocation trials and two smoke workers** completed. The library and benchmark preflights passed. Automatic verification confirms identical source, compiled classes, dependencies, JVM/OS/CPU settings and harness context to the primary baseline. All 44 downloaded measurement artifacts match their server SHA-256 checksums.

The median elapsed-time ratio is **124.49×** in favor of Quiescent. The four adjacent paired ratios are 133.23×, 117.16×, 126.14×, 122.89×.

| Implementation | Median JVM mean, ms/trial | JVM means, ms | Between-JVM spread | Transfer bytes/value | Setup bytes/trial | Process CPU ms/trial |
|---|---:|---|---:|---:|---:|---:|
| Quiescent | 97.516 | 91.756, 102.891, 97.025, 98.008 | 11.42% | 98.69 | 76935506 | 365.00 |
| core.async | 12139.455 | 12224.246, 12054.664, 12238.876, 12043.728 | 1.61% | 879.12 | 76439398 | 48029.58 |

core.async allocated **8.91×** as many transfer bytes per delivered value. Allocation was measured in five separate passes after each JVM’s timing trials. Setup allocation is reported separately; the transfer counter covers all JVM threads through worker teardown and includes runtime/harness/background allocation.

The 837.6× magnitude did not reproduce on Totem: the new paired ratios are 117.16×–133.23×. The large advantage is nevertheless consistent across all four pairs and exceeds the maximum observed in the earlier nine-case primary capture. Retain this as a supplemental baseline case for future comparisons.

Quiescent’s JVM-mean timing spread is 11.42%, versus 1.61% for core.async. Allocation spreads are approximately 0.20% and 0.08%, respectively. The host log retained 284 interval observations with no recorded swap traffic or I/O wait. Boot identity remained stable; no benchmark worker remains and the shared lock is free. System-wide monitoring cannot rule out all unrelated CPU activity. No outliers were removed and no performance cause is established by these measurements alone.

## Relation to the historical result

The historical macOS table recorded 56.206 ms for Quiescent and 47,079.060 ms for core.async, a ratio of 837.6×. That is a different capture, not a control: core.async was 1.8.741 rather than 1.9.865; the channel implementation, hardware/JVM/OS, timing boundary, workload bookkeeping and JVM isolation also differ. Its 100,000 requested values/channel became 99,968 after integer producer quotas, versus the present fixture’s 96,000.

This follow-up tests the same topology under the current methodology. It does not recreate or establish the cause of the historical 837.6× result. Keep both captures. Timing includes both libraries’ execution models, not just the channel data structure. A large difference in this harness is not a claim about whole-application speedups.

## Configuration and artifacts

Frozen source snapshot matching commit `32ead33`; Corretto 26.0.2+10-FR, Clojure 1.12.4, core.async 1.9.865, Intel i5-4590S (four cores), 2 GiB heap, G1 and default schedulers. Four forks per implementation in AB/BA/AB/BA order. Each worker warms up for at least ten seconds/three trials, then performs twelve timing and five allocation trials. Every worker has exact-content pre/postflight validation and every trial checks counts/sums.

- [All JVMs and trial-level reports](measurement/summary.md)
- [Portable observations and paired ratios](review.json)
- [Artifact hashes](artifact-sha256.txt)
