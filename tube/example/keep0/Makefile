.PHONY:beg clean kill build z one two magic

beg:
	cd ../../cmd/tube; go install
	cd ../../cmd/tup; go install
	cd ../../cmd/tubels; go install
	cd ../../cmd/tuberm; go install
	cd ../../cmd/tubeadd; go install
	./beg
	sleep 1
	(grep "remote\ URL" log.*) || true
	(ps -ef |grep tube |grep -v grep) || true
	./taillogs

magic:
	make clean; make beg; make who

z:
	nohup tube tube.0.cfg &> log.0 &

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

clean:
	pkill -9 tube || true
	sleep 1 # let them die so they don't replicate log after we wipe it
	rm -rf /Users/jaten/.local/state/tube/123/
	rm -f log.*
	(ps -ef |grep tube |grep -v grep) || true

kill:
	pkill -9 tube || true
	(ps -ef |grep tube |grep -v grep) || true

who:
	(ps -ef |grep tube |grep -v grep) || true

view0:
	################# raft log for node 0:
	tube -log tube.0.cfg |tail -5

view1:
	################# raft log for node 1:
	tube -log tube.1.cfg |tail -5

view2:
	################# raft log for node 2:
	tube -log tube.2.cfg |tail -5

v: view0 view1 view2
