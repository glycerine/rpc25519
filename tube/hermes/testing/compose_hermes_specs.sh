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
extract_between "$frag_dir/action_complete_ready.ivy" "        before complete_ready {" "        # O3 broadcast-ACK optimization"
extract_between "$frag_dir/action_o3_observe_quorum.ivy" "        before o3_observe_quorum {" "        # Once the matching INV"
extract_between "$frag_dir/action_o3_complete.ivy" "        before o3_complete {" "        invariant ready(N) -> live(N)"
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

all_actions="
local_write
local_rmw
receive_write_inv
receive_rmw_inv
receive_ack
complete_current
complete_overwritten
complete_ready
o3_observe_quorum
o3_complete
mark_ready
receive_validate
replay_after_failure
fail
"

write_disabled_actions() {
    dst=$1
    shift

    : > "$dst"
    for action in $all_actions; do
        keep=0
        for allowed in "$@"; do
            if [ "$action" = "$allowed" ]; then
                keep=1
            fi
        done
        if [ "$keep" = 0 ]; then
            {
                printf '\n'
                printf '        before %s {\n' "$action"
                printf '            require false;\n'
                printf '        }\n'
            } >> "$dst"
        fi
    done
}

compose_safety_spec() {
    name=$1
    allowed=$2
    shift 2

    disabled="$frag_dir/disabled_$name.ivy"
    write_disabled_actions "$disabled" $allowed

    compose_spec "$name" \
        "$frag_dir/core_decls.ivy" \
        "$@" \
        "$disabled" \
        "$frag_dir/invariants_safety.ivy" \
        "$frag_dir/invariants_progress.ivy" \
        hermes_rmw_o3_close.ivy \
        hermes_rmw_o3_exports_core.ivy
}

compose_liveness_spec() {
    name=$1
    temporal=$2
    allowed=$3
    shift 3

    disabled="$frag_dir/disabled_$name.ivy"
    write_disabled_actions "$disabled" $allowed

    compose_spec "$name" \
        "$frag_dir/core_decls.ivy" \
        "$@" \
        "$disabled" \
        "$frag_dir/invariants_safety.ivy" \
        "$frag_dir/invariants_progress.ivy" \
        "$temporal" \
        hermes_rmw_o3_close.ivy \
        hermes_rmw_o3_exports_core.ivy
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

compose_safety_spec hermes_rmw_o3_local_write_safety \
    "local_write" \
    "$frag_dir/action_local_write.ivy"

compose_safety_spec hermes_rmw_o3_local_rmw_safety \
    "local_rmw" \
    "$frag_dir/action_local_rmw.ivy"

compose_safety_spec hermes_rmw_o3_receive_write_inv_safety \
    "receive_write_inv" \
    "$frag_dir/action_receive_write_inv.ivy"

compose_safety_spec hermes_rmw_o3_receive_rmw_inv_safety \
    "receive_rmw_inv" \
    "$frag_dir/action_receive_rmw_inv.ivy"

compose_safety_spec hermes_rmw_o3_receive_ack_safety \
    "receive_ack" \
    "$frag_dir/action_receive_ack.ivy"

compose_safety_spec hermes_rmw_o3_mark_ready_safety \
    "mark_ready" \
    "$frag_dir/action_mark_ready.ivy"

compose_safety_spec hermes_rmw_o3_complete_current_safety \
    "complete_current" \
    "$frag_dir/action_complete_current.ivy"

compose_safety_spec hermes_rmw_o3_complete_overwritten_safety \
    "complete_overwritten" \
    "$frag_dir/action_complete_overwritten.ivy"

compose_safety_spec hermes_rmw_o3_receive_validate_safety \
    "receive_validate" \
    "$frag_dir/action_receive_validate.ivy"

compose_safety_spec hermes_rmw_o3_replay_after_failure_safety \
    "replay_after_failure" \
    "$frag_dir/action_replay_after_failure.ivy"

compose_safety_spec hermes_rmw_o3_fail_safety \
    "fail" \
    "$frag_dir/action_fail.ivy"

compose_safety_spec hermes_rmw_o3_complete_ready_safety \
    "complete_ready" \
    "$frag_dir/action_complete_ready.ivy"

compose_safety_spec hermes_rmw_o3_o3_observe_quorum_safety \
    "o3_observe_quorum" \
    "$frag_dir/action_o3_observe_quorum.ivy"

compose_safety_spec hermes_rmw_o3_o3_complete_safety \
    "o3_complete" \
    "$frag_dir/action_o3_complete.ivy"

# Compatibility names used by older make targets and logs. These now use the
# action-slice decomposition above while keeping all supporting invariants.
compose_safety_spec hermes_rmw_o3_write_ts_safety "local_write" "$frag_dir/action_local_write.ivy"
compose_safety_spec hermes_rmw_o3_write_messages_safety "receive_write_inv" "$frag_dir/action_receive_write_inv.ivy"
compose_safety_spec hermes_rmw_o3_write_node_safety "receive_ack" "$frag_dir/action_receive_ack.ivy"
compose_safety_spec hermes_rmw_o3_write_completion_safety "mark_ready" "$frag_dir/action_mark_ready.ivy"
compose_safety_spec hermes_rmw_o3_write_conflict_safety "complete_current" "$frag_dir/action_complete_current.ivy"

compose_safety_spec hermes_rmw_o3_rmw_ts_safety "local_rmw" "$frag_dir/action_local_rmw.ivy"
compose_safety_spec hermes_rmw_o3_rmw_messages_safety "receive_rmw_inv" "$frag_dir/action_receive_rmw_inv.ivy"
compose_safety_spec hermes_rmw_o3_rmw_node_safety "receive_ack" "$frag_dir/action_receive_ack.ivy"
compose_safety_spec hermes_rmw_o3_rmw_completion_safety "complete_current" "$frag_dir/action_complete_current.ivy"
compose_safety_spec hermes_rmw_o3_rmw_conflict_safety "receive_validate" "$frag_dir/action_receive_validate.ivy"

compose_safety_spec hermes_rmw_o3_failure_ts_safety "fail" "$frag_dir/action_fail.ivy"
compose_safety_spec hermes_rmw_o3_failure_messages_safety "replay_after_failure" "$frag_dir/action_replay_after_failure.ivy"
compose_safety_spec hermes_rmw_o3_failure_node_safety "receive_write_inv" "$frag_dir/action_receive_write_inv.ivy"
compose_safety_spec hermes_rmw_o3_failure_completion_safety "complete_current" "$frag_dir/action_complete_current.ivy"
compose_safety_spec hermes_rmw_o3_failure_conflict_safety "receive_validate" "$frag_dir/action_receive_validate.ivy"

compose_safety_spec hermes_rmw_o3_ready_ts_safety "mark_ready" "$frag_dir/action_mark_ready.ivy"
compose_safety_spec hermes_rmw_o3_ready_messages_safety "receive_ack" "$frag_dir/action_receive_ack.ivy"
compose_safety_spec hermes_rmw_o3_ready_node_safety "complete_ready" "$frag_dir/action_complete_ready.ivy"
compose_safety_spec hermes_rmw_o3_ready_completion_safety "complete_overwritten" "$frag_dir/action_complete_overwritten.ivy"
compose_safety_spec hermes_rmw_o3_ready_conflict_safety "complete_current" "$frag_dir/action_complete_current.ivy"
compose_safety_spec hermes_rmw_o3_ready_progress_safety "complete_ready" "$frag_dir/action_complete_ready.ivy"

compose_safety_spec hermes_rmw_o3_o3_ts_safety "o3_observe_quorum" "$frag_dir/action_o3_observe_quorum.ivy"
compose_safety_spec hermes_rmw_o3_o3_messages_safety "o3_complete" "$frag_dir/action_o3_complete.ivy"
compose_safety_spec hermes_rmw_o3_o3_node_safety "receive_rmw_inv" "$frag_dir/action_receive_rmw_inv.ivy"
compose_safety_spec hermes_rmw_o3_o3_completion_safety "o3_complete" "$frag_dir/action_o3_complete.ivy"
compose_safety_spec hermes_rmw_o3_o3_conflict_safety "o3_complete" "$frag_dir/action_o3_complete.ivy"
compose_safety_spec hermes_rmw_o3_o3_progress_safety "o3_complete" "$frag_dir/action_o3_complete.ivy"

compose_safety_spec hermes_rmw_o3_write_safety \
    "local_write receive_write_inv receive_ack mark_ready complete_current complete_overwritten receive_validate" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_receive_validate.ivy"

compose_safety_spec hermes_rmw_o3_rmw_safety \
    "local_rmw receive_rmw_inv receive_ack mark_ready complete_current receive_validate" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy"

compose_safety_spec hermes_rmw_o3_failure_safety \
    "local_write receive_write_inv receive_ack mark_ready fail replay_after_failure complete_current receive_validate" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_fail.ivy" \
    "$frag_dir/action_replay_after_failure.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_receive_validate.ivy"

compose_safety_spec hermes_rmw_o3_ready_safety \
    "receive_ack mark_ready complete_current complete_overwritten complete_ready" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_mark_ready.ivy" \
    "$frag_dir/action_complete_current.ivy" \
    "$frag_dir/action_complete_overwritten.ivy" \
    "$frag_dir/action_complete_ready.ivy"

compose_safety_spec hermes_rmw_o3_o3_safety \
    "local_write local_rmw receive_write_inv receive_rmw_inv receive_ack o3_observe_quorum o3_complete receive_validate" \
    "$frag_dir/action_local_write.ivy" \
    "$frag_dir/action_local_rmw.ivy" \
    "$frag_dir/action_receive_write_inv.ivy" \
    "$frag_dir/action_receive_rmw_inv.ivy" \
    "$frag_dir/action_receive_ack.ivy" \
    "$frag_dir/action_o3_observe_quorum.ivy" \
    "$frag_dir/action_o3_complete.ivy" \
    "$frag_dir/action_receive_validate.ivy"

compose_spec hermes_rmw_o3_ambient_safety \
    hermes_rmw_o3_prefix.ivy \
    hermes_rmw_o3_suffix.ivy \
    hermes_rmw_o3_exports_all.ivy

compose_liveness_spec hermes_rmw_o3_ready_liveness \
    hermes_rmw_o3_temporal_ready.ivy \
    "complete_ready" \
    "$frag_dir/action_complete_ready.ivy"

compose_liveness_spec hermes_rmw_o3_o3_write_liveness \
    hermes_rmw_o3_temporal_o3_write.ivy \
    "o3_observe_quorum o3_complete" \
    "$frag_dir/action_o3_observe_quorum.ivy" \
    "$frag_dir/action_o3_complete.ivy"

compose_liveness_spec hermes_rmw_o3_o3_rmw_liveness \
    hermes_rmw_o3_temporal_o3_rmw.ivy \
    "o3_observe_quorum o3_complete" \
    "$frag_dir/action_o3_observe_quorum.ivy" \
    "$frag_dir/action_o3_complete.ivy"

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
