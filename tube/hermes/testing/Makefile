default: py

SPEC ?= hermes_rmw_o3_testing
CHECK_TIMEOUT ?= 60s
TEST_TIMEOUT ?= 60s
SEED ?= 20260916
RUNS ?= 1
ITERS ?= 200

CHECK_SPECS = \
	hermes_rmw_o3_local_write_safety \
	hermes_rmw_o3_local_rmw_safety \
	hermes_rmw_o3_receive_write_inv_safety \
	hermes_rmw_o3_receive_rmw_inv_safety \
	hermes_rmw_o3_receive_ack_safety \
	hermes_rmw_o3_mark_ready_safety \
	hermes_rmw_o3_complete_current_safety \
	hermes_rmw_o3_complete_overwritten_safety \
	hermes_rmw_o3_receive_validate_safety \
	hermes_rmw_o3_replay_after_failure_safety \
	hermes_rmw_o3_fail_safety \
	hermes_rmw_o3_complete_ready_safety \
	hermes_rmw_o3_o3_observe_quorum_safety \
	hermes_rmw_o3_o3_complete_safety \
	hermes_rmw_o3_ready_liveness \
	hermes_rmw_o3_o3_write_liveness \
	hermes_rmw_o3_o3_rmw_liveness

OBSOLETE_CHECK_SPECS = \
	hermes_rmw_o3_write_ts_safety \
	hermes_rmw_o3_write_messages_safety \
	hermes_rmw_o3_write_node_safety \
	hermes_rmw_o3_write_completion_safety \
	hermes_rmw_o3_write_conflict_safety \
	hermes_rmw_o3_rmw_ts_safety \
	hermes_rmw_o3_rmw_messages_safety \
	hermes_rmw_o3_rmw_node_safety \
	hermes_rmw_o3_rmw_completion_safety \
	hermes_rmw_o3_rmw_conflict_safety \
	hermes_rmw_o3_failure_ts_safety \
	hermes_rmw_o3_failure_messages_safety \
	hermes_rmw_o3_failure_node_safety \
	hermes_rmw_o3_failure_completion_safety \
	hermes_rmw_o3_failure_conflict_safety \
	hermes_rmw_o3_ready_ts_safety \
	hermes_rmw_o3_ready_messages_safety \
	hermes_rmw_o3_ready_node_safety \
	hermes_rmw_o3_ready_completion_safety \
	hermes_rmw_o3_ready_conflict_safety \
	hermes_rmw_o3_ready_progress_safety \
	hermes_rmw_o3_o3_ts_safety \
	hermes_rmw_o3_o3_messages_safety \
	hermes_rmw_o3_o3_node_safety \
	hermes_rmw_o3_o3_completion_safety \
	hermes_rmw_o3_o3_conflict_safety \
	hermes_rmw_o3_o3_progress_safety

TEST_SPECS = \
	hermes_rmw_o3_write_testing \
	hermes_rmw_o3_rmw_testing \
	hermes_rmw_o3_failure_testing

compose:
	sh compose_hermes_specs.sh .

go: compose
	XTRACE_OFF=1 ivy2cpp target=test build=true $(SPEC).ivy
	rm -rf goout/$(SPEC)
	mkdir -p goout/$(SPEC)
	cp $(SPEC).* goout/$(SPEC)/

py: compose
	XTRACE_OFF=1 ivy_to_cpp target=test build=true $(SPEC).ivy
	rm -rf pyout/$(SPEC)
	mkdir -p pyout/$(SPEC)
	cp $(SPEC).* pyout/$(SPEC)/

clean:
	rm -f hermes_rmw_o3_testing hermes_rmw_o3_testing.cpp hermes_rmw_o3_testing.h hermes_rmw_o3_testing.dsc
	rm -f hermes_rmw_o3_write_testing hermes_rmw_o3_write_testing.cpp hermes_rmw_o3_write_testing.h hermes_rmw_o3_write_testing.dsc
	rm -f hermes_rmw_o3_rmw_testing hermes_rmw_o3_rmw_testing.cpp hermes_rmw_o3_rmw_testing.h hermes_rmw_o3_rmw_testing.dsc
	rm -f hermes_rmw_o3_failure_testing hermes_rmw_o3_failure_testing.cpp hermes_rmw_o3_failure_testing.h hermes_rmw_o3_failure_testing.dsc

punish:
	timeout $(TEST_TIMEOUT) ./$(SPEC) runs=$(RUNS) iters=$(ITERS) seed=$(SEED)

check: check-one

check-one: compose check-one-run

check-one-run:
	if [ -f log.$(SPEC).ivy_check.out ]; then mv log.$(SPEC).ivy_check.out log.$(SPEC).ivy_check.out.prev; fi
	XTRACE_OFF=1 timeout $(CHECK_TIMEOUT) ivy_check diagnose=true trace=true $(SPEC).ivy > log.$(SPEC).ivy_check.out
	tail -4 log.$(SPEC).ivy_check.out

check-small: compose
	@set -e; for spec in $(OBSOLETE_CHECK_SPECS); do \
		if [ -f log.$$spec.ivy_check.out ]; then mv log.$$spec.ivy_check.out log.$$spec.ivy_check.out.prev; fi; \
	done
	@set -e; for spec in $(CHECK_SPECS); do \
		$(MAKE) check-one-run SPEC=$$spec; \
	done

check-safety:
	$(MAKE) check-one SPEC=hermes_rmw_o3_safety

check-write-safety:
	$(MAKE) check-one SPEC=hermes_rmw_o3_write_safety

check-write-small: check-write-ts check-write-messages check-write-node check-write-completion check-write-conflict

check-write-ts:
	$(MAKE) check-one SPEC=hermes_rmw_o3_write_ts_safety

check-write-messages:
	$(MAKE) check-one SPEC=hermes_rmw_o3_write_messages_safety

check-write-node:
	$(MAKE) check-one SPEC=hermes_rmw_o3_write_node_safety

check-write-completion:
	$(MAKE) check-one SPEC=hermes_rmw_o3_write_completion_safety

check-write-conflict:
	$(MAKE) check-one SPEC=hermes_rmw_o3_write_conflict_safety

check-rmw-safety:
	$(MAKE) check-one SPEC=hermes_rmw_o3_rmw_safety

check-rmw-small: check-rmw-ts check-rmw-messages check-rmw-node check-rmw-completion check-rmw-conflict

check-rmw-ts:
	$(MAKE) check-one SPEC=hermes_rmw_o3_rmw_ts_safety

check-rmw-messages:
	$(MAKE) check-one SPEC=hermes_rmw_o3_rmw_messages_safety

check-rmw-node:
	$(MAKE) check-one SPEC=hermes_rmw_o3_rmw_node_safety

check-rmw-completion:
	$(MAKE) check-one SPEC=hermes_rmw_o3_rmw_completion_safety

check-rmw-conflict:
	$(MAKE) check-one SPEC=hermes_rmw_o3_rmw_conflict_safety

check-failure-safety:
	$(MAKE) check-one SPEC=hermes_rmw_o3_failure_safety

check-failure-small: check-failure-ts check-failure-messages check-failure-node check-failure-completion check-failure-conflict

check-failure-ts:
	$(MAKE) check-one SPEC=hermes_rmw_o3_failure_ts_safety

check-failure-messages:
	$(MAKE) check-one SPEC=hermes_rmw_o3_failure_messages_safety

check-failure-node:
	$(MAKE) check-one SPEC=hermes_rmw_o3_failure_node_safety

check-failure-completion:
	$(MAKE) check-one SPEC=hermes_rmw_o3_failure_completion_safety

check-failure-conflict:
	$(MAKE) check-one SPEC=hermes_rmw_o3_failure_conflict_safety

check-ready-safety:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_safety

check-ready-small: check-ready-ts check-ready-messages check-ready-node check-ready-completion check-ready-conflict check-ready-progress

check-ready-ts:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_ts_safety

check-ready-messages:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_messages_safety

check-ready-node:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_node_safety

check-ready-completion:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_completion_safety

check-ready-conflict:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_conflict_safety

check-ready-progress:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_progress_safety

check-o3-safety:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_safety

check-o3-small: check-o3-ts check-o3-messages check-o3-node check-o3-completion check-o3-conflict check-o3-progress

check-o3-ts:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_ts_safety

check-o3-messages:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_messages_safety

check-o3-node:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_node_safety

check-o3-completion:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_completion_safety

check-o3-conflict:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_conflict_safety

check-o3-progress:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_progress_safety

check-ready:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ready_liveness

check-o3-write:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_write_liveness

check-o3-rmw:
	$(MAKE) check-one SPEC=hermes_rmw_o3_o3_rmw_liveness

check-ambient-safety:
	$(MAKE) check-one SPEC=hermes_rmw_o3_ambient_safety

py-small: py-write py-rmw py-failure

py-write:
	$(MAKE) py SPEC=hermes_rmw_o3_write_testing

py-rmw:
	$(MAKE) py SPEC=hermes_rmw_o3_rmw_testing

py-failure:
	$(MAKE) py SPEC=hermes_rmw_o3_failure_testing

go-small: go-write go-rmw go-failure

go-write:
	$(MAKE) go SPEC=hermes_rmw_o3_write_testing

go-rmw:
	$(MAKE) go SPEC=hermes_rmw_o3_rmw_testing

go-failure:
	$(MAKE) go SPEC=hermes_rmw_o3_failure_testing

test-small: test-write test-rmw test-failure

test-write:
	$(MAKE) py SPEC=hermes_rmw_o3_write_testing
	$(MAKE) punish SPEC=hermes_rmw_o3_write_testing

test-rmw:
	$(MAKE) py SPEC=hermes_rmw_o3_rmw_testing
	$(MAKE) punish SPEC=hermes_rmw_o3_rmw_testing

test-failure:
	$(MAKE) py SPEC=hermes_rmw_o3_failure_testing
	$(MAKE) punish SPEC=hermes_rmw_o3_failure_testing
