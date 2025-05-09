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

