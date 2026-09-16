#!/bin/sh
set -eu

out_dir=${1:-.}
mkdir -p "$out_dir"
frag_dir="$out_dir/.hermes_fragments"
mkdir -p "$frag_dir"

extract_until() {
    dst=$1
    end=$2
    awk -v end="$end" '
        index($0,end) { exit }
        { print }
    ' hermes_rmw_o3_prefix.ivy > "$dst"
}

extract_between() {
    dst=$1
    start=$2
    end=$3
    awk -v start="$start" -v end="$end" '
        found && index($0,end) { exit }
        index($0,start) { found=1 }
        found { print }
    ' hermes_rmw_o3_prefix.ivy > "$dst"
}

extract_from() {
    dst=$1
    start=$2
    awk -v start="$start" '
        index($0,start) { found=1 }
        found { print }
    ' hermes_rmw_o3_prefix.ivy > "$dst"
}

extract_until "$frag_dir/core_decls.ivy" "        before local_write {"
extract_between "$frag_dir/action_local_write.ivy" "        before local_write {" "        before local_rmw {"
extract_between "$frag_dir/action_local_rmw.ivy" "        before local_rmw {" "        before receive_write_inv {"
extract_between "$frag_dir/action_receive_write_inv.ivy" "        before receive_write_inv {" "        before receive_rmw_inv {"
extract_between "$frag_dir/action_receive_rmw_inv.ivy" "        before receive_rmw_inv {" "        before receive_ack {"
extract_between "$frag_dir/action_receive_ack.ivy" "        before receive_ack {" "        before mark_ready {"
extract_between "$frag_dir/action_mark_ready.ivy" "        before mark_ready {" "        before complete_current {"
extract_between "$frag_dir/action_complete_current.ivy" "        before complete_current {" "        before complete_overwritten {"
extract_between "$frag_dir/action_complete_overwritten.ivy" "        before complete_overwritten {" "        before receive_validate {"
extract_between "$frag_dir/action_receive_validate.ivy" "        before receive_validate {" "        before replay_after_failure {"
extract_between "$frag_dir/action_replay_after_failure.ivy" "        before replay_after_failure {" "        before fail {"
extract_between "$frag_dir/action_fail.ivy" "        before fail {" "        ################################################################################"
extract_between "$frag_dir/invariants_safety.ivy" "        # Safety invariants" "        # Liveness/progress obligation"
extract_between "$frag_dir/invariants_ts_order.ivy" "        invariant seen_epoch(init_epoch)" "        invariant ts_value(T,V) -> seen_ts(T)"
extract_between "$frag_dir/invariants_messages.ivy" "        invariant ts_value(T,V) -> seen_ts(T)" "        invariant seen_ts(cur_ts(N))"
extract_between "$frag_dir/invariants_node.ivy" "        invariant seen_ts(cur_ts(N))" "        # Once a timestamp is completed"
extract_between "$frag_dir/invariants_completion.ivy" "        # Once a timestamp is completed" "        # A live node may retain"
extract_between "$frag_dir/invariants_conflict.ivy" "        # A live node may retain" "        # Liveness/progress obligation"
extract_between "$frag_dir/action_complete_ready.ivy" "        before complete_ready {" "        # O3 broadcast-ACK optimization"
extract_between "$frag_dir/actions_o3.ivy" "        # O3 broadcast-ACK optimization" "        invariant ready(N) -> live(N)"
extract_from "$frag_dir/invariants_progress.ivy" "        invariant ready(N) -> live(N)"

compose_spec() {
    name=$1
    shift

    tmp="$out_dir/$name.ivy.tmp"
    dst="$out_dir/$name.ivy"
    first=1

    : > "$tmp"
    for frag in "$@"; do
        if [ "$first" = 1 ]; then
            awk '{ print }' "$frag" >> "$tmp"
            first=0
        else
            awk 'NR == 1 && /^#lang[[:space:]]/ { next } { print }' "$frag" >> "$tmp"
        fi
    done
    mv "$tmp" "$dst"
}

compose_spec hermes_rmw_o3_testing \
    hermes_rmw_o3_prefix.ivy \
    hermes_rmw_o3_temporal_ready.ivy \
    hermes_rmw_o3_temporal_o3_write.ivy \
    hermes_rmw_o3_temporal_o3_rmw.ivy \
    hermes_rmw_o3_suffix.ivy \
    hermes_rmw_o3_exports_all.ivy

compose_spec hermes_rmw_o3_safety \
    hermes_rmw_o3_prefix.ivy \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_write_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_safety.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_write_ts_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_ts_order.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_write_messages_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_messages.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_write_node_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_node.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_write_completion_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_completion.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_write_conflict_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_conflict.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_rmw_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_safety.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_rmw_ts_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_ts_order.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_rmw_messages_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_messages.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_rmw_node_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_node.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_rmw_completion_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_completion.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_rmw_conflict_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_conflict.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_failure_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_fail.ivy" \
    "$frag_dir/action_replay_after_failure.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_safety.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_failure_ts_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_fail.ivy" \
    "$frag_dir/action_replay_after_failure.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_ts_order.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_failure_messages_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_fail.ivy" \
    "$frag_dir/action_replay_after_failure.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_messages.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_failure_node_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_fail.ivy" \
    "$frag_dir/action_replay_after_failure.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_node.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_failure_completion_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_fail.ivy" \
    "$frag_dir/action_replay_after_failure.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_completion.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_failure_conflict_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_fail.ivy" \
    "$frag_dir/action_replay_after_failure.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_conflict.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ready_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_safety.ivy" \
    "$frag_dir/invariants_progress.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ready_ts_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_ts_order.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ready_messages_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_messages.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ready_node_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_node.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ready_completion_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_completion.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ready_conflict_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_conflict.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ready_progress_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_progress.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_safety.ivy" \
    "$frag_dir/invariants_progress.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_ts_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_ts_order.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_messages_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_messages.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_node_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_node.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_completion_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_completion.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_conflict_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_conflict.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_progress_safety \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/action_receive_validate.ivy" \
    "$frag_dir/invariants_progress.ivy" \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_ambient_safety \
    hermes_rmw_o3_prefix.ivy \
    hermes_rmw_o3_suffix.ivy \
    hermes_rmw_o3_exports_all.ivy

compose_spec hermes_rmw_o3_ready_liveness \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/action_complete_ready.ivy" \
    "$frag_dir/invariants_progress.ivy" \
    hermes_rmw_o3_temporal_ready.ivy \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_write_liveness \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/invariants_progress.ivy" \
    hermes_rmw_o3_temporal_o3_write.ivy \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_o3_rmw_liveness \
    "$frag_dir/core_decls.ivy" \
    "$frag_dir/actions_o3.ivy" \
    "$frag_dir/invariants_progress.ivy" \
    hermes_rmw_o3_temporal_o3_rmw.ivy \
    hermes_rmw_o3_close.ivy \
    hermes_rmw_o3_exports_core.ivy

compose_spec hermes_rmw_o3_write_testing \
    hermes_rmw_o3_prefix.ivy \
    hermes_rmw_o3_suffix.ivy \
    hermes_rmw_o3_interpret.ivy \
    hermes_rmw_o3_exports_write.ivy

compose_spec hermes_rmw_o3_rmw_testing \
    hermes_rmw_o3_prefix.ivy \
    hermes_rmw_o3_suffix.ivy \
    hermes_rmw_o3_interpret.ivy \
    hermes_rmw_o3_exports_rmw.ivy

compose_spec hermes_rmw_o3_failure_testing \
    hermes_rmw_o3_prefix.ivy \
    hermes_rmw_o3_suffix.ivy \
    hermes_rmw_o3_interpret.ivy \
    hermes_rmw_o3_exports_failure.ivy
