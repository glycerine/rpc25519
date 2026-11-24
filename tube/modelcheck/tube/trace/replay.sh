#!/usr/bin/env bash
# Usage: TLA_LIB=/path/holding/jars ./replay.sh /path/to/tube.tla /path/to/trace.ndjson
set -euo pipefail
: "${TLA_LIB:?Set TLA_LIB to the directory with tla2tools.jar and CommunityModules-deps.jar}"
model=$(realpath "$1")
trace=$(realpath "$2")
tools=$(realpath "$TLA_LIB")
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
cp "$model" "$work/tube.tla"
cp "$here/Trace.tla" "$here/Trace.cfg" "$work/"
cd "$work"
export JSON="$trace"
java -XX:+UseParallelGC -Xmx"${TLC_HEAP:-1g}" -cp "$tools/tla2tools.jar:$tools/CommunityModules-deps.jar" \
    tlc2.TLC -workers 1 -noGenerateSpecTE -config Trace.cfg Trace.tla
