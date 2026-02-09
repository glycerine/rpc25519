.PHONY:update beg clean kill build z one two magic reup

update:
	git pull origin master --tags
	make build

reup:
	git pull origin master --tags
	make build
	make kill
	sleep 1
	./do

beg:
	cd ../../cmd/tube; go install
	cd ../../cmd/tup; go install
	cd ../../cmd/tubels; go install
	cd ../../cmd/tuberm; go install
	cd ../../cmd/tubeadd; go install
	cd ../../cmd/member; go install
	cd ../../cmd/hermes; go install
	./beg
	sleep 1
	grep "remote\ URL" log.*
	(ps -ef |grep tube |grep -v grep) || true
	./taillogs

magic:
	git pull origin master --tags; make clean; make build; sleep 10; ./do

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
	cd ../../cmd/member; go install
	cd ../../cmd/hermes; go install

clean:
	pkill -9 -x tube || true
	pkill -9 -x member || true
	pkill -9 -x hermes || true
	sleep 3 # let them die so they don't replicate log after we wipe it
	rm -rf ${HOME}/.local/state/tube/123/
	rm -f log.*
	rm -rf tmp/
	(ps -ef |grep tube |grep -v grep) || true
	git pull origin master --tags
	cd ../../cmd/tube; go install
	cd ../../cmd/tup; go install
	cd ../../cmd/tubels; go install
	cd ../../cmd/tuberm; go install
	cd ../../cmd/tubeadd; go install
	cd ../../cmd/member; go install
	cd ../../cmd/hermes; go install
	mkdir tmp

kill:
	pkill -9 -x tube || true
	(ps -ef |grep tube |grep -v grep) || true

who:
	(ps -ef |grep member |grep -v grep) || true
	(ps -ef |grep tube |grep -v grep) || true
	(ps -ef |grep hermes |grep -v grep) || true

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
