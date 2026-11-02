# Reviewed initial JVM channel baseline

Capture completed on **2026-10-29 at 16:26:59 Europe/Stockholm**. Total capture time was 2:54:22, including compile, correctness and smoke checks; full measurement ran 13:34:53–16:26:58 (2:52:05). Reviewed that evening.

All **72 fresh JVM workers, 864 timing trials, 360 allocation trials and 12 smoke workers** completed successfully. The server preflight passed 488 library tests / 1,196 assertions and six harness tests / 175 assertions. Automatic integrity verification passed; the retrieved 364 measurement artifacts were independently SHA-256 verified locally, and all 125 captured source inputs match commit `32ead331a48a70904c96b0b58ac82b2814ea6da0`.

This is the initial reference for the JVM implementation now on `channel/main`, before the three isolated experiments. Later branch changes added shared tests and documentation, without changing its runtime implementation. None of the node-only, release-store, or slot-retention variants is measured here.

Environment: Intel Core i5-4590S (four cores), Corretto 26.0.2+10-FR, Clojure 1.12.4, core.async 1.9.865, Linux 7.0.0-38, 2 GiB fixed heap, G1, default schedulers. Four fresh JVMs per implementation/case; implementation order alternates by fork. Each JVM has at least ten seconds/three trials of warmup, twelve timing trials and five separate allocation trials.

## Comparison

Times are medians of four JVM means. Ratios are core.async / Quiescent. Allocations are median whole-JVM **transfer bytes per delivered value**, measured separately after timing; setup allocation is retained separately in the detailed reports.

| Workload | Quiescent ms | core.async ms | Time ratio | Q bytes/value | async bytes/value | Allocation ratio |
|---|---:|---:|---:|---:|---:|---:|
| 24 × 32P1C | 170.461 | 9023.973 | 52.94x | 104.47 | 773.09 | 7.40x |
| 24 × 1P32C | 208.102 | 9117.374 | 43.81x | 109.14 | 710.78 | 6.51x |
| 50 × 1P1C | 193.538 | 9560.916 | 49.40x | 96.80 | 318.96 | 3.30x |
| 50 × 4P4C | 231.683 | 9971.203 | 43.04x | 98.60 | 326.04 | 3.31x |
| 50 × mapcat 1P1C | 172.579 | 7103.204 | 41.16x | 96.76 | 298.77 | 3.09x |
| Mixed 40 workloads | 150.874 | 6389.438 | 42.35x | 101.19 | 355.04 | 3.51x |
| 20 × map relay 4P→4C | 147.915 | 5589.187 | 37.79x | 187.01 | 606.43 | 3.24x |
| Single 1P1C, buffer 1024 | 178.224 | 4277.034 | 24.00x | 97.08 | 437.43 | 4.51x |
| 200 × 1P1C, buffer 1 | 2867.823 | 33638.929 | 11.73x | 144.03 | 629.51 | 4.37x |

## Interpretation and limits

All 36 adjacent implementation pairs favor Quiescent. Median elapsed-time ratios range from 11.73× to 52.94×, and transfer-allocation ratios from 3.09× to 7.40×. Process CPU counters agree with substantially less CPU work per completed workload in Quiescent; the elapsed differences are not explained by simply assigning it more cores.

These are results for this exact harness and the libraries’ default execution models: Quiescent tasks versus core.async go blocks. They include scheduling, start-gate release, counts/sums, transducers and close/drain. They exclude channel/payload/worker construction from elapsed timing. They do not establish a scheduler-independent channel-data-structure speedup, application speedup, operation latency distribution, or fairness proof. The shared generated loops and their bookkeeping are part of the measured workload.

Quiescent timing spread across JVM means is 3.24%–37.00%. Fan-in is the notable variable case: JVM means are 162.24, 168.38, 172.54 and 225.31 ms. Its fourth JVM is consistently slower across all twelve trials, with increased process CPU and essentially unchanged allocation. Its recorded GC time does not account for the whole difference; no cause is established. Retain that run. The corresponding paired advantages remain 39.93×–56.11×. Pipe-map and buffer-one spreads are also material at 14.45% and 13.39%.

Quiescent allocation spread stays below 0.8% for every case; core.async allocation spread is at most 5.13% (single-channel), with the remaining cases at or below 1.20%. Allocation is approximate allocated heap bytes, not retained heap or GC pause time. Transfer accounting extends through worker teardown and includes JVM background/harness allocation. Read-pair controls are retained without subtraction.

Across the equally sized timing selections, summed JVM GC time-counter deltas were 8,907 ms for Quiescent and 32,103 ms for core.async. These counters include collector-specific/concurrent accounting and must not be described as stop-the-world pause totals. Per-trial counters and allocation observations remain available for diagnosis.

The host activity log has 2,091 interval observations. Nonzero swap-out and I/O-wait samples occurred only during preflight, before full measurement; full measurement recorded no swap traffic or I/O wait. Boot identity remained unchanged. System-wide vmstat cannot attribute other CPU activity to individual processes, so this does not establish an entirely exclusive host. Host setup and operational logs remain archived on the server.

Accept this as the initial full-profile reference for the frozen harness/runtime. The broad Quiescent/core.async differences exceed the observed variation, but use fresh interleaved controls before assessing small changes in the three new experiments, especially fan-in. Preserve every repetition and keep the historical local Markdown tables separate.

## Retained evidence

- [Automatic integrity report](REVIEW.md)
- [All JVMs and individual timing/allocation trials](measurement/summary.md)
- [Portable observations and paired comparisons](review.json)
- [Measurement artifact checksums](artifact-sha256.txt)
- [Source manifest](source-manifest.json)
