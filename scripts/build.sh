#!/usr/bin/env bash
# build.sh — compile everything (library, vendored matcher-java, tools) into out/.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf out
javac -d out --release 17 $(find src/main/java -name '*.java')
