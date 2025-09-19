How to run a simple Tube RAFT cluster
=====================================

getting started
---------------

We will run a simple 3 node cluster, with
all three nodes on the same host. So we
will end up starting three processes, and
then connect to them using `tup`, the tube
updater CLI command for reading and write
the key/value store.

from this directory (rpc25519/tube/example/local):

1. install the default tup config file to its default location.
~~~
$ cd rpc25519/tube/example/local
$ mkdir -p ~/.config/tube
$ cp tup.default.config ~/.config/tube/
~~~

2. generate TLS certs for service 'tube' (one time task)
~~~
$ cd rpc25519
$ make # installs selfy key creation tool (only run if selfy not already on hand)
$ selfy -k tube -nopass
~~~

3. Be sure that your $GOPATH/bin is on your $PATH, so
that `go install` will leave `tube` in $GOPATH/bin/tube
and thus it will be runnable.

4. run `make magic` which automates building and starting
a three node cluster
~~~
$ make magic
~~~

In response to `make magic`, you should see output like this:
~~~
$ make magic

make clean; make beg; make who
pkill -9 tube || true
sleep 1 # let them die so they don't replicate log after we wipe it
rm -rf /Users/yourname/.local/state/tube/123/
rm -f log.*
(ps -ef |grep tube |grep -v grep) || true
cd ../../cmd/tube; go install
cd ../../cmd/tup; go install
cd ../../cmd/tubels; go install
cd ../../cmd/tuberm; go install
cd ../../cmd/tubeadd; go install
./beg
sleep 1
(grep "remote\ URL" log.*) || true
(ps -ef |grep tube |grep -v grep) || true
  501 61955     1   0  2:42AM ttys002    0:00.13 tube tube.0.cfg
  501 61956     1   0  2:42AM ttys002    0:00.10 tube tube.1.cfg
  501 61957     1   0  2:42AM ttys002    0:00.11 tube tube.2.cfg
./taillogs
cli.go:219 [goID 55] 2025-09-19 01:42:54.789797000 +0000 UTC connected to server '127.0.0.1:7002'; c.oneWayCh=0xc000014850; c.roundTripCh=0xc0000148c0; local(conn)=tcp://127.0.0.1:55670 -> remote(conn)=tcp://127.0.0.1:7002
cli.go:219 [goID 65] 2025-09-19 01:42:54.770668000 +0000 UTC connected to server '127.0.0.1:7002'; c.oneWayCh=0xc0004b82a0; c.roundTripCh=0xc0004b8310; local(conn)=tcp://127.0.0.1:55668 -> remote(conn)=tcp://127.0.0.1:7002
cli.go:219 [goID 49] 2025-09-19 01:42:54.774722000 +0000 UTC connected to server '127.0.0.1:7001'; c.oneWayCh=0xc0000363f0; c.roundTripCh=0xc000036460; local(conn)=tcp://127.0.0.1:55669 -> remote(conn)=tcp://127.0.0.1:7001
(ps -ef |grep tube |grep -v grep) || true
  501 61955     1   0  2:42AM ttys002    0:00.13 tube tube.0.cfg
  501 61956     1   0  2:42AM ttys002    0:00.10 tube tube.1.cfg
  501 61957     1   0  2:42AM ttys002    0:00.11 tube tube.2.cfg
$
~~~

5. verify the cluster is up and that your tup.default.config is 
found and working by running `tubels` to list the cluster membership.

~~~
$ tubels
existing membership: (no known leader)
~~~

Good. we are starting with an empty cluster. We
have three proceses up, but until they are told
to join the cluster they won't replicate any
commands or elect a leader among themselves.

6. add node_0 to the set of replicators.

~~~
$ tubeadd node_0

tubeadd.go:225 2025-09-19T01:52:47.389176000+00:00 tupadd is doing AddPeerIDToCluster using leaderURL='tcp://127.0.0.1:7000'
membership after adding 'node_0': ( leader)

$ tubels

existing membership: (node_0 leader)
  node_0:   tcp://127.0.0.1:7000/tube-replica/rtL0vvlHTZsZdIzUwE1iwalBlZpL
$
~~~

7. Since there is only one node, it elects itself leader of the
membership immediately.

8. Add node_1 (the second node to add to the cluster).

~~~
$ tubeadd node_1

tubeadd.go:225 2025-09-19T01:54:30.121842000+00:00 tupadd is doing AddPeerIDToCluster using leaderURL='tcp://127.0.0.1:7000/tube-replica/rtL0vvlHTZsZdIzUwE1iwalBlZpL'

membership after adding 'node_1': (node_0 leader)
  node_0:   tcp://127.0.0.1:7000/tube-replica/rtL0vvlHTZsZdIzUwE1iwalBlZpL
  node_1:   tcp://127.0.0.1:7001/tube-replica/SW4vyFus0SzSMr5VautVO40vkN2e
$
~~~

9. Add node_2 (third and final node).

~~~
$ tubeadd node_2

tubeadd.go:225 2025-09-19T01:56:42.282367000+00:00 tupadd is doing AddPeerIDToCluster using leaderURL='tcp://127.0.0.1:7000/tube-replica/rtL0vvlHTZsZdIzUwE1iwalBlZpL'

membership after adding 'node_2': (node_0 leader)
  node_0:   tcp://127.0.0.1:7000/tube-replica/rtL0vvlHTZsZdIzUwE1iwalBlZpL
  node_1:   tcp://127.0.0.1:7001/tube-replica/SW4vyFus0SzSMr5VautVO40vkN2e
  node_2:   tcp://127.0.0.1:7002/tube-replica/wkCLGRNnU9qQkKGUVPUKFIyFmXsz
$
~~~

10. launch the tup cli to access the sorted key/value tables.

~~~
$ tup

tup: the tube updater; use tup -v for diagnostics.
commands: .key               : read key from current table
          key                : read key from current table (if not keyword)
          !key newval        : write newval to key in current table
          @table key newval  : write newval to key in table
          ,table key         : read key from table
          +table {key} {endx}: read  ascending key, key+1, ..., endx from table
          -table {key} {endx}: read descending key, key-1, ..., endx from table
                             :  {key} {endx} optional. + alone for current table
          del key            : delete key from current table
          show               : show all tables
          show table         : show all keys in table
          ls                 : show all keys in current table
          mv old new         : rename table old to new
          use table          : table becomes the current table
          rmtable table      : drop the named table
          newtable table     : make a new table
          cas key old new    : if key holds old, replace old with new (compare and swap).

keywords: cas, newtable, rmtable, use, mv, ls, show, del

[node_0 connected](table 'base') > 
~~~

11. At the up command prompt, set key x to have value 10 in the base table,
then set y to have value 12.

~~~
[node_0 connected](table 'base') > !x 10
wrote: x <- 10 (in table 'base')
[node_0 connected](table 'base') > !y 10
wrote: y <- 12 (in table 'base')
~~~

12. Read back all the keys in the current default table:

~~~
[node_0 connected](table 'base') > -
(from table 'base') read key 'y': 12
(from table 'base') read key 'x': 10
(2 keys back)
~~~

Congratulations. That is the basics. Press ctrl-d to exit tup.

13. You can remove nodes from the cluster
using `tuberm`. Let's remove the leader and 
verify that a new node is elected leader.

Notice that the leader does not update instantaneously.
The election timeout has to happen and then node_1 and
node_2 realize that they have not be hearing
heartbeats, and so they start an election. After
a few seconds, run tubels. You should see either
node_1 or node_2 has won leadership.

~~~
$ tuberm node_0

membership after removing 'node_0': (node_0 leader)
  node_1:   tcp://127.0.0.1:7001/tube-replica/SW4vyFus0SzSMr5VautVO40vkN2e
  node_2:   tcp://127.0.0.1:7002/tube-replica/wkCLGRNnU9qQkKGUVPUKFIyFmXsz

$ tubels
existing membership: (node_1 leader)
  node_1:   tcp://127.0.0.1:7001/tube-replica/SW4vyFus0SzSMr5VautVO40vkN2e
  node_2:   tcp://127.0.0.1:7002/tube-replica/wkCLGRNnU9qQkKGUVPUKFIyFmXsz

~~~

14. Verify that the keys are still present, despite having
lost replica node_0.

~~~
$ tup
...
[node_1 connected](table 'base') > -
(from table 'base') read key 'y': 12
(from table 'base') read key 'x': 10
(2 keys back)
~~~

By entering '-' at the tup prompt, we ask for all the
keys in the current table (sorted in descending order).

By entering '+' we will get ascending order, which might
be more expected. I used '-' as the first example simply
because it is easier to type (does not require the shift key).

15. FIN. 

Congratulations. You have finished this
getting started with Tube tutorial.

