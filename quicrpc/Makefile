all:
	go build -o qcli ./cmd/qcli
	go build -o qsrv ./cmd/qsrv

run:
	./qsrv &
	sleep 1
	./qcli
	sleep 1
	pkill -9 qsrv


