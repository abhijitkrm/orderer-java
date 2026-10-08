#!/usr/bin/env bash
# build-harness.sh — spec/HARNESS.md §6: build the five tools and expose them
# as harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}.
#
#   scripts/build-harness.sh            # optimized
#   CHECKED=1 scripts/build-harness.sh  # assertions on (-ea -esa), HARNESS.md §4.2
#
# Each tool is a wrapper script running the JVM on out/. Short harness runs
# use C1 only (startup dominates); orderbench gets the full JIT and a sized heap.
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/build.sh
ROOT=$(pwd)
EA=""; [ "${CHECKED:-0}" = 1 ] && EA="-ea -esa"
mkdir -p harness/bin
tool() {  # name class jvm-flags
  cat > "harness/bin/$1" <<W
#!/bin/sh
exec java $3 $EA -cp "$ROOT/out" io.github.abhijitkrm.orderer.tools.$2 "\$@"
W
  chmod +x "harness/bin/$1"
}
QUICK="-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Xss8m"
tool orderrun OrderRun "$QUICK"
tool ordererfuzz OrdererFuzz "$QUICK"
tool orderrecover OrderRecover "$QUICK"
tool ordersnap OrderSnap "$QUICK"
tool orderbench OrderBench "-XX:+UseParallelGC -Xms2g -Xmx6g -XX:+AlwaysPreTouch"
