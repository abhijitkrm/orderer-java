#!/usr/bin/env bash
# test.sh — the full suite, as the spec repo's verify.sh runs it.
set -euo pipefail
cd "$(dirname "$0")/.."
CHECKED=1 scripts/build-harness.sh
rm -rf out-test
javac -d out-test --release 17 -cp out $(find src/test/java -name '*.java')
for t in RingTest GoldenTest PipelineTest; do
  java -ea -cp out:out-test io.github.abhijitkrm.orderer.$t
done
java -cp out:out-test io.github.abhijitkrm.orderer.Quickstart > /dev/null
spec/conformance.sh harness/bin vectors
