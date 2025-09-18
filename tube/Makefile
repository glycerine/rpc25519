.PHONY: all wasm rr replay
all:
	##go generate
	cd cmd/tube; go install
	cd cmd/tup; go install
	cd cmd/tubels; go install
	cd cmd/tuberm; go install
	cd cmd/tubeadd; go install

wasm:
	GOTRACEBACK=all GOEXPERIMENT=synctest GOOS=wasip1 GOARCH=wasm  goj test -c -o tube.test.wasm -tags goj
	GOTRACEBACK=all GOEXPERIMENT=synctest GOOS=wasip1 GOARCH=wasm  wasmtime --env HOME=/ --dir .::/ --env GO_DSIM_SEED=3 tube.test.wasm -test.v -test.run 016

rr:
	GOTRACEBACK=all GOEXPERIMENT=synctest go test -race -c -o tube.test
	rr record -h ./tube.test -test.v -test.run 016

replay:
	rr replay

##rrclean:
##     rm -rf ~/.local/share/rr/ ## careful! deletes all old traces!
