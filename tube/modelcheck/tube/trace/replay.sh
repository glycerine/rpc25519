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

if [[ -d $input ]]; then
    while IFS= read -r scenario; do
        trace="$input/$scenario.ndjson"
        [[ -f $trace ]] || trace+=.gz
        replay "$trace"
    done < "$here/scenarios.txt"

    # The second copy of this election prefix reports an incorrect commit term.
    replay "$here/fixtures/controls/valid-prefix.ndjson.gz"
    status=0
    replay "$here/fixtures/controls/inconsistent-prefix.ndjson.gz" > control.log 2>&1 || status=$?
    if [[ $status == 13 ]] && grep -Fxq 'Error: Temporal property TraceMatched was violated.' control.log; then
        echo "TraceMatched rejected the altered commit term as expected."
    else
        cat control.log >&2
        echo "Expected TraceMatched to reject the altered commit term (TLC exit $status)." >&2
        exit 1
    fi
else
    replay "$input"
fi
