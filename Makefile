all:
	go generate
	mkdir -p certs
	go build -o cli ./cmd/cli
	go build -o srv ./cmd/srv
	go build -o selfy ./cmd/selfy
	go build -o samesame ./cmd/samesame

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


