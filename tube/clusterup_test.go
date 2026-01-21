package tube

import (
	"context"
	"fmt"
	"reflect"
	//"strings"
	"testing"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

// similar to rpc25519/simgrid_test.go, make sure we
// can get a grid up, for all the other tests to work.
func Test030_does_cluster_come_up_under_simnet(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		n := 3 // 0.4 sec
		//n := 2 // 0.4 sec
		//n := 10 // 5 sec
		cfg := NewTubeConfigTest(n, t.Name(), faketime)
		c := NewCluster(t.Name(), cfg)
		c.Start()
		defer c.Close()

		c.waitForConnectedGrid()
	})
}

// waitForConnectedGrid waits until all n*(n-1)
// circuit endpoints have been reported before returning.
// We verify that the first ckts established
// are to replicas, not clients.
func (c *TubeCluster) waitForConnectedGrid() (replicaCktCount int) {
	nodes := c.Nodes
	nNode := len(nodes)

	//vv("top waitForConnectedGrid(); nNode = %v", nNode)
	//defer vv("end waitForConnectedGrid()")

	for i, g := range nodes {
		_ = i
		select { // 031 hung intermit here
		case <-g.verifyPeersNeededSeen.Chan:
			//vv("i=%v all peer connections need have been seen(%v) by node '%v': '%#v'", i, g.verifyPeersNeeded, g.name, g.verifyPeersSeen.GetKeySlice()) // data race read vs prev write at tube.go:7267

			// failing test will just hang above.
			// we cannot really do case <-time.After(time.Minute) with faketime.
		case cktP := <-g.verifyPeerReplicaOrNot:
			replicaCktCount++
			//vv("grid connection seen from (%v)=='%v': cktP.isReplica = %v for ckt='%#v'", rpc.AliasDecode(cktP.ckt.RemotePeerID), cktP.ckt.RemotePeerID, cktP.isReplica, cktP.ckt)
			if !cktP.isReplica() {
				panic(fmt.Sprintf("all circuits during cluster setup should be replicas; this was not: '%#v'; cktP.ckt.CircuitID = '%v'", cktP, cktP.ckt.CircuitID))
			}
		}
	}
	// get them all if we did not above.
	var cases []reflect.SelectCase
	for k, g := range nodes {
		_ = k
		//vv("adding select case %v: '%v' (%v)", k, g.name, g.PeerID)
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(g.verifyPeerReplicaOrNot),
		})
	}
	for replicaCktCount < nNode*(nNode-1) {
		//vv("top of for, replicaCktCount = %v", replicaCktCount)
		chosenCase, recv, recvOK := reflect.Select(cases)
		_ = chosenCase
		if recvOK {
			cktP := recv.Interface().(*cktPlus)
			replicaCktCount++
			//vv("node=chosenCase=%v; cktP.isReplica = %v for ckt='%#v'", chosenCase, cktP.isReplica, cktP.ckt)
			if !cktP.isReplica() {
				panic(fmt.Sprintf("all circuits during cluster setup should be replicas; this was not: '%#v'; cktP.ckt.CircuitID = '%v'", cktP, cktP.ckt.CircuitID))
			}
		}
	}
	return
}

// client ckt should be distinguished from replica ckt,
// so we don't count clients towards quorum or try
// to AppendEntries to them.
func Test031_client_ckt_is_not_replica_ckt(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		n := 4
		cfgC := NewTubeConfigTest(n, t.Name(), faketime)
		c := NewCluster(t.Name(), cfgC)
		c.Start()
		defer c.Close()

		vv("cluster all started. n = %v", n)

		// the assertion below depends on us
		// waiting for the full grid to form
		// before allowing any clients
		// (non-replica peers) in to play.
		// We verify both ends of each circuit.
		// For n nodes, there are (n choose 2)
		// circuits with two ends each so
		// 2 * n*(n-1)/2 = n*(n-1)
		replicaCktCount := c.waitForConnectedGrid() // waiting in here on red 031 / hung intermit no synctest.

		if replicaCktCount != n*(n-1) {
			panic(fmt.Sprintf("expected %v, got %v", n*(n-1), replicaCktCount))
		}
		vv("good: all n*(n-1)=%v (where n=%v) of the first ckt seen by nodes were replica circuits.", n*(n-1), n)

		// non-raft-node client
		cliname := "client"

		// need to use the same cfg to locate the simnet
		cp := *cfgC.RpcCfg
		cfg := &cp
		cfg.UseSimNet = faketime
		addr := c.Nodes[0].Srv.LocalNetAddr()
		cfg.ClientDialToHostPort = addr.String()
		vv("cfg.ClientDialToHostPort = '%v'", cfg.ClientDialToHostPort)
		cfg.TCPonly_no_TLS = true

		cli, err := rpc.NewClient(cliname, cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)
		defer cli.Close()

		cliServiceName := "client"
		cliSync := newWriteReadClient(cliServiceName)
		err = cli.PeerAPI.RegisterPeerServiceFunc(cliServiceName, cliSync.Start)
		panicOn(err)

		ctx := context.Background()
		var peerServiceNameVersion string
		cliPeer, err := cli.PeerAPI.StartLocalPeer(ctx, cliServiceName, peerServiceNameVersion, nil, "", true)
		panicOn(err)
		defer cliPeer.Close()

		cliFrag := cliPeer.NewFragment()
		cliFrag.FragSubject = "client query"
		pushToURL := c.Nodes[0].URL
		pushToPeerName := c.Nodes[0].name
		var userString string
		vv("pushToURL = '%v'", pushToURL)
		// e.g.
		// tcp://127.0.0.1:50421/tube/FiA8WviFasBF6xLieszav7cnVmqX
		// or
		// simnet://srv_node_0/tube/RdS_qaJGypwiNX91APbw5QVLuChQ
		ckt, ctx, _, err := cliPeer.NewCircuitToPeerURL("tube-replica", pushToURL, cliFrag, 0, userString, pushToPeerName)
		vv("client made new ctk = '%v'", ckt.CircuitID)
		_ = ckt
		_ = ctx
		panicOn(err)
		//vv("031 select{}")
		//select {}
	})
}

func newWriteReadClient(name string) *wrCli {
	return &wrCli{
		name: name,
		Halt: idem.NewHalterNamed("wrCli"),
	}
}

type wrCli struct {
	name string
	Halt *idem.Halter
}

func (s *wrCli) Start(
	myPeer *rpc.LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *rpc.Circuit,
) (err0 error) {
	defer func() {
		s.Halt.Done.Close()
	}()
	rpc.AliasRegister(myPeer.PeerID, "("+myPeer.ServiceName()+")")
	done0 := ctx0.Done()

	for {
		select {
		// new Circuit connection arrives
		case ckt := <-newCircuitCh:
			vv("%v: wrCli has new ckt: '%v'", s.name, rpc.AliasDecode(ckt.RemotePeerID))
			go func(ckt *rpc.Circuit) (err0 error) {
				ctx := ckt.Context
				defer func() {
					ckt.Close(err0)
				}()
				done := ctx.Done()
				for {
					select {
					case frag := <-ckt.Reads:

						outFrag := myPeer.NewFragment()
						outFrag.Payload = frag.Payload
						outFrag.FragSubject = "echo reply"
						_, err := ckt.SendOneWay(outFrag, 0, 0)
						_ = err // context cancel normal on shutdown, don't freak.

					case fragerr := <-ckt.Errors:
						panic(fragerr)
					case <-ckt.Halt.ReqStop.Chan:
						return
					case <-done:
						return // server finishing on done!
					case <-done0:
						return
					case <-s.Halt.ReqStop.Chan:
						return
					}
				}
			}(ckt)
		case <-done0:
			return rpc.ErrContextCancelled
		case <-s.Halt.ReqStop.Chan:
			return rpc.ErrHaltRequested
		}
	}
	return nil
}
