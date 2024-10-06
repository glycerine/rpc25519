all:
	go build -o cli ./cmd/cli
	go build -o srv ./cmd/srv

run:
	./srv &
	sleep 1
	./cli
	sleep 1
	pkill -9 srv


