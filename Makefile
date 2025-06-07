all: githash
	go generate
	go build -o cli ./cmd/cli
	go build -o srv ./cmd/srv
	go build -o selfy ./cmd/selfy
	go build -o samesame ./cmd/samesame
	go build -o jcp ./cmd/jcp
	go build -o jsrv ./cmd/jsrv
	cp -p ./cli ~/go/bin
	cp -p ./srv ~/go/bin
	cp -p ./selfy ~/go/bin
	cp -p ./jcp ~/go/bin
	cp -p ./jsrv ~/go/bin

run:
	./srv &
	sleep 1
	./cli
	sleep 1
	pkill -9 srv

runq:
	./srv -q &
	sleep 1
	./cli -q
	sleep 1
	pkill -9 srv

githash:
	/bin/echo "package rpc25519" > gitcommit.go
	/bin/echo "func init() { LAST_GIT_COMMIT_HASH = \"$(shell git rev-parse HEAD)\"; NEAREST_GIT_TAG= \"$(shell git describe --abbrev=0 --tags)\"; GIT_BRANCH=\"$(shell git rev-parse --abbrev-ref  HEAD)\"; GO_VERSION=\"$(shell go version)\";}" >> gitcommit.go

test:
	go test -v
	cd jsync && go test -v
	cd jcdc && go test -v
	cd bytes && go test -v

synctest:
	GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

rr:
	##rm -rf ~/.local/share/rr/ ## careful! deletes all old traces!
	#GOTRACEBACK=all GOEXPERIMENT=synctest go test -race -c -o rpc.test -count=1
	GOTRACEBACK=all GOEXPERIMENT=synctest go test -c -o rpc.test -count=1
	rr record ./rpc.test -test.v -test.run 707

rrh:
	GOTRACEBACK=all GOEXPERIMENT=synctest go test -c -o rpc.test -count=1
	rr record -h ./rpc.test -test.v #-test.run 707

rr2:
	go test -c -o rpc.test
	rr record ./rpc.test -test.v #-test.run 707

rr2h:
	go test -c -o rpc.test
	rr record -h ./rpc.test -test.v #-test.run 707

replay:
	rr replay

grid707test:
	GOTRACEBACK=all GOEXPERIMENT=synctest go test -v -run Test707_simnet_grid_does_not_lose_messages # in simgrid_test.go

cleanrr:
	rm -rf $(HOME)/issue74019/rr
	mkdir $(HOME)/issue74019/rr
