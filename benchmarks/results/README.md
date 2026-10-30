# Retained channel benchmark captures

These captures have separate environments and scopes; do not pool observations.

| Capture | Scope | Report |
|---|---|---|
| Totem / x86 Linux primary | Nine workloads, 72 JVM workers | [Assessment](20261029T123400Z-initial-d3ed8d1/ASSESSMENT.md) |
| Totem / x86 Linux wide fan-in | 24×128P1C, buffer 1024, eight JVM workers | [Assessment](20261029T200137Z-wide-fan-in-32ead33/ASSESSMENT.md) |
| M1 Max / ARM macOS wide fan-in | Same fixture, source/classes/JDK/settings, eight JVM workers | [Cross-machine assessment](20261029T234600Z-macos-arm-wide-32ead33/ASSESSMENT.md) |

The Totem captures remain the primary JVM references. The Mac capture is a
user-requested machine/runtime comparison; OS, architecture, core count/mix and
background activity differ. All use the runtime implementation at commit 32ead33.
The later channel experiments are not measured in these captures.

Raw measurements, process logs, source archives, integrity manifests, and every
sample are retained with each report. The earlier failed Mac host-logging attempt
contains no measurements and remains in its own directory. runner-smoke and
allocation-smoke are exploratory harness checks, not performance references.
