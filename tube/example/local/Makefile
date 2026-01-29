.PHONY:update beg clean kill build z one two magic

update:
	make build

beg:
	cd ../../cmd/tube; go install
	cd ../../cmd/tup; go install
	cd ../../cmd/tubels; go install
	cd ../../cmd/tuberm; go install
	cd ../../cmd/tubeadd; go install
	cd ../../cmd/member; go install
	./beg
	sleep 1
	(grep "remote\ URL" log.*) || true
	(ps -ef |grep tube |grep -v grep) || true
	./taillogs
	tubeadd node_0 || true
	tubeadd node_4 || true
	tubeadd node_5 || true
	sleep 1
	tubels

magic:
	make clean; make beg; make who

z:
	nohup tube tube.0.cfg &> log.0 &
	sleep 1
	tubeadd node_0
	sleep 1
	mkdir tmp; cd tmp; member &>log.1 &

one:
	nohup tube tube.1.cfg &> log.1 &

two:
	nohup tube tube.2.cfg &> log.2 &

build:
	cd ../../cmd/tube; go install
	cd ../../cmd/tup; go install
	cd ../../cmd/tubels; go install
	cd ../../cmd/tuberm; go install
	cd ../../cmd/tubeadd; go install
	cd ../../cmd/member; go install

clean:
	pkill -9 -x tube || true
	pkill -9 -x member || true
	sleep 1 # let them die so they don't replicate log after we wipe it
	rm -rf ${HOME}/.local/state/tube/123/
	rm -f log.*
	rm -rf tmp/
	(ps -ef |grep tube |grep -v grep) || true

kill:
	pkill -9 -x tube || true
	##pkill -9 -x member || true
	(ps -ef |grep tube |grep -v grep) || true

who:
	(ps -ef |grep member |grep -v grep) || true
	(ps -ef |grep tube |grep -v grep) || true

view0:
	################# raft log for node 0:
	tube -log tube.0.cfg

view1:
	################# raft log for node 1:
	tube -log tube.1.cfg

view2:
	################# raft log for node 2:
	tube -log tube.2.cfg



