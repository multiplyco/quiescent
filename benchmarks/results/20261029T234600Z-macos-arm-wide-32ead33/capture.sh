#!/usr/bin/env bash
set -euo pipefail
umask 077
capture=/Users/henrik/clojure/quiescent/benchmarks/results/20261029T234600Z-macos-arm-wide-32ead33
checkout=/private/tmp/quiescent-arm-baseline-20261030
reference=/Users/henrik/clojure/quiescent/benchmarks/results/20261029T200137Z-wide-fan-in-32ead33
export JAVA_HOME=/Users/henrik/.sdkman/candidates/java/26.0.2-amzn
export PATH="$JAVA_HOME/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
cd "$checkout"
finish() {
  result=$?
  trap - EXIT
  for monitor_pid in ${top_pid:-} ${vm_pid:-}; do kill "$monitor_pid" 2>/dev/null || true; wait "$monitor_pid" 2>/dev/null || true; done
  { date -u; pmset -g batt; pmset -g therm; sysctl vm.swapusage; } > "$capture/host-end.txt" 2>&1 || true
  printf '%s\n' "$result" > "$capture/exit-status"
  if (( result == 0 )); then printf 'complete\n' > "$capture/status"; else printf 'failed\n' > "$capture/status"; fi
  date -u +%FT%TZ > "$capture/finished-at"
  exit "$result"
}
trap finish EXIT
printf 'running\n' > "$capture/status"
date -u +%FT%TZ > "$capture/started-at"
cp "$reference/source-manifest.json" "$capture/source-manifest.json"
{
  date -u
  uname -a
  sw_vers
  sysctl machdep.cpu.brand_string hw.ncpu hw.memsize hw.perflevel0.physicalcpu hw.perflevel1.physicalcpu kern.boottime vm.swapusage
  uptime
  pmset -g batt
  pmset -g therm
  pmset -g custom
  java -version
  cat "$JAVA_HOME/release"
  clojure -Sdescribe
  ps -Ao pid,pcpu,pmem,comm -r | sed -n '1,20p'
} > "$capture/host.txt" 2>&1
/usr/bin/top -R -l 0 -s 15 -n 12 -o cpu -stats pid,cpu,command > "$capture/top.log" 2>&1 &
top_pid=$!
/usr/bin/vm_stat 10 > "$capture/vm-stat.log" 2>&1 &
vm_pid=$!
printf 'library-tests\n' > "$capture/stage"
clojure -Srepro -M:bench:test -m kaocha.runner --reporter kaocha.report/dots > "$capture/library-tests.log" 2>&1
printf 'harness-tests\n' > "$capture/stage"
clojure -Srepro -M:bench:bench-test > "$capture/harness-tests.log" 2>&1
printf 'smoke\n' > "$capture/stage"
clojure -Srepro -X:bench :profile :smoke :forks 1 :cases '[:wide-fan-in]' :timeout-ms 300000 :output '"/Users/henrik/clojure/quiescent/benchmarks/results/20261029T234600Z-macos-arm-wide-32ead33/smoke"'
printf 'full\n' > "$capture/stage"
clojure -Srepro -X:bench :forks 4 :cases '[:wide-fan-in]' :timeout-ms 300000 :label '"Native ARM/macOS wide-fan-in comparison"' :output '"/Users/henrik/clojure/quiescent/benchmarks/results/20261029T234600Z-macos-arm-wide-32ead33/measurement"'
printf 'verifying\n' > "$capture/stage"
bb "$capture/review.clj" "$capture" > "$capture/review.log" 2>&1
printf 'integrity-verified\n' > "$capture/review-status"
printf 'complete\n' > "$capture/stage"
