.PHONY: all wasm rr replay tla tla-profiles tla-traces
all:
	##go generate
	cd cmd/tube; go install
	cd cmd/tup; go install
	cd cmd/tubels; go install
	cd cmd/tuberm; go install
	cd cmd/tubeadd; go install
	cd cmd/member; go install
	cd cmd/hermes; go install

wasm:
	GOTRACEBACK=all GOEXPERIMENT=synctest GOOS=wasip1 GOARCH=wasm  goj test -c -o tube.test.wasm -tags goj -tags synctest
	GOTRACEBACK=all GOEXPERIMENT=synctest GOOS=wasip1 GOARCH=wasm  wasmtime --env HOME=/ --dir .::/ --env GO_DSIM_SEED=3 tube.test.wasm -test.v -test.run 016

rr:
	GOTRACEBACK=all GOEXPERIMENT=synctest go test -race -c -o tube.test
	rr record -h ./tube.test -test.v -test.run 016

replay:
	rr replay

##rrclean:
##     rm -rf ~/.local/share/rr/ ## careful! deletes all old traces!

# par_test.go takes 261 seconds (479 seconds under -race) so we leave it off by default,
# making par_test.go need the build tag par in order to run. Thus we get back
# our 30 second test run (under -tags synctest) that wer are used to.
partest:
	go test -v -tags synctest,par

# Run the Tube model suite with "make tla" using the bundled JARs.
TLA_LIB ?= modelcheck
TLA2TOOLS_JAR ?= $(TLA_LIB)/tla2tools.jar
TLC_WORKERS ?= 4
TLC_HEAP ?= 2g
TLA_TRACE_DIR ?= modelcheck/tube/trace/fixtures/44d0b71d

# Re-model check the TLA+ spec for Tube.
# Takes about 48 minutes on an linux/amd64 box. AMD Ryzen Threadripper 3960X 24-Core Processor
tla: tla-traces tla-profiles
	@echo "All Tube TLA+ profiles, witnesses, and trace checks passed."

tla-profiles:
	python3 modelcheck/tube/check.py --all --jar "$(TLA2TOOLS_JAR)" --workers "$(TLC_WORKERS)" --heap "$(TLC_HEAP)"

tla-traces:
	@test -d "$(TLA_TRACE_DIR)" || { echo "Missing trace directory: $(TLA_TRACE_DIR)" >&2; exit 1; }
	TLA_LIB="$(TLA_LIB)" TLA2TOOLS_JAR="$(TLA2TOOLS_JAR)" TLC_HEAP="$(TLC_HEAP)" \
	    bash modelcheck/tube/trace/replay.sh modelcheck/tube/tube.tla "$(TLA_TRACE_DIR)"
