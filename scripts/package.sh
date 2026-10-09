#!/usr/bin/env bash
# package.sh — build dist/orderer-<version>.jar, -sources.jar and -javadoc.jar
# with plain JDK tools (the same artifacts pom.xml describes).
set -euo pipefail
cd "$(dirname "$0")/.."
V=$(sed -n 's:.*<version>\(.*\)</version>.*:\1:p' pom.xml | head -1)
scripts/build.sh
rm -rf dist && mkdir -p dist/javadoc
jar --create --file "dist/orderer-$V.jar" -C out .
jar --create --file "dist/orderer-$V-sources.jar" -C src/main/java .
javadoc -quiet -Xdoclint:none -d dist/javadoc -sourcepath src/main/java -subpackages io.github.abhijitkrm > /dev/null
jar --create --file "dist/orderer-$V-javadoc.jar" -C dist/javadoc .
rm -rf dist/javadoc
ls -1 dist
