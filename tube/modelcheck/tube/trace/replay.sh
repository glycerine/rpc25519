#!/usr/bin/env bash
# Usage: ./replay.sh <model> <trace file or directory>
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
lib=${TLA_LIB:-"$here/../.."}
model=$(realpath "$1")
input=$(realpath "$2")
tlc=${TLA2TOOLS_JAR:-"$lib/tla2tools.jar"}
community="$lib/CommunityModules-deps.jar"
for jar in "$tlc" "$community"; do
    if [[ ! -r "$jar" ]]; then
        echo "Missing $jar. Set TLA_LIB to the directory containing the two TLA+ JARs." >&2
        exit 1
    fi
done
classpath="$(realpath "$tlc"):$(realpath "$community")"
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
cp "$model" "$work/tube.tla"
cp "$here/Trace.tla" "$here/Trace.cfg" "$work/"
cd "$work"
replay() {
    local trace=$1
    echo "Replaying $trace"
    if [[ $trace == *.gz ]]; then
        gzip -dc -- "$trace" > trace.ndjson || return
        trace="$work/trace.ndjson"
    fi
    JSON="$trace" java -XX:+UseParallelGC -Xmx"${TLC_HEAP:-1g}" -cp "$classpath" \
        tlc2.TLC -workers 1 -noGenerateSpecTE -config Trace.cfg Trace.tla
}

expect_mismatch() {
    local trace=$1 expected_event=$2 status=0 event
    replay "$trace" > control.log 2>&1 || status=$?
    # TraceAlias prints l, the next event, for each counterexample state.
    event=$(awk '$2 == "l" && $3 == "=" {cursor=$4} END {print cursor}' control.log)
    if [[ $status != 13 || $event != "$expected_event" ]] ||
       ! grep -Fxq 'Error: Temporal property TraceMatched was violated.' control.log; then
        cat control.log >&2
        echo "Expected TraceMatched to reject $trace at event $expected_event (TLC exit $status, event $event)." >&2
        return 1
    fi
    echo "TraceMatched rejected $(basename "$trace") at event $event as expected."
}

if [[ -d $input ]]; then
    while IFS= read -r scenario; do
        trace="$input/$scenario.ndjson"
        [[ -f $trace ]] || trace+=.gz
        replay "$trace"
    done < "$here/scenarios.txt"

    controls="$here/fixtures/controls"
    replay "$controls/valid-prefix.ndjson.gz"
    expect_mismatch "$controls/inconsistent-prefix.ndjson.gz" 13
    replay "$controls/watchdog-restarted-peer.ndjson.gz"
    expect_mismatch "$controls/watchdog-stale-peer.ndjson.gz" 447
else
    replay "$input"
fi
