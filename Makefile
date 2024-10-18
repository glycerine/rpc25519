all:
	go generate
	mkdir -p certs
	touch certs/.placeholder 
	go build -o cli ./cmd/cli
	go build -o srv ./cmd/srv
	go build -o selfy ./cmd/selfy
	go build -o same ./cmd/samesame
	cp -p selfy ~/go/bin
	cp -p srv ~/go/bin
	cp -p cli ~/go/bin
	cp -p same ~/go/bin

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


