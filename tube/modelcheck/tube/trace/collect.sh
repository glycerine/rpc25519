#!/usr/bin/env bash
# Usage: bash collect.sh <repo> <revision> <fresh output dir> [scenario ...]
set -euo pipefail
repo=$(cd "$1" && pwd)
rev=$(git -C "$repo" rev-parse --verify "$2^{commit}")
mkdir "$3"
out=$(cd "$3" && pwd)
shift 3
here=$(cd "$(dirname "$0")" && pwd)
mkdir "$out/src" "$out/traces" "$out/logs"
git -C "$repo" archive "$rev" | tar -x -C "$out/src"
for change in instrumentation support; do
    patch --forward --batch -p1 -d "$out/src" < "$here/$change.patch"
done
cd "$out/src/tube"
go test -tags synctest -c -o "$out/trace.test" .
if (( $# == 0 )); then
    set -- cold_start_election leader_crash_restart follower_crash_restart leader_isolate_heal \
      leader_isolate_divergent_overwrite follower_drop_sends leader_deaf membership_add_remove \
      compaction_snapshot follower_torn_wal_restart follower_wal_loss_restart interrupted_uncommitted_tail
fi
for sc; do
    echo "$sc"
    TLA_TRACE_DIR="$out/traces" GOMAXPROCS=3 timeout 120 "$out/trace.test" \
      -test.count=1 -test.v -test.run "^TestTLATrace_$sc\$" > "$out/logs/$sc" 2>&1
    test -s "$out/traces/$sc.ndjson"
done
