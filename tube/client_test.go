package tube

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"testing"
)

// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

// work in progress on chapter 6 linearizability (linz)
// semantics for clients.
// 707 is a good baseline for correct operation, copy
// and introduce expected errors in 708,...
func Test707_client_linz_semantics(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 3
		forceLeader := 0
		c, leader, leadi, _ := setupTestCluster(t, numNodes, forceLeader, 707)
		defer c.Close()

		nodes := c.Nodes
		_ = leader
		vv("leader is '%v'", leader)
		leaderNode := c.Nodes[leadi]
		leaderURL := leaderNode.URL

		cliName := "client707"
		cliCfg := *c.Cfg
		cliCfg.MyName = cliName
		cliCfg.PeerServiceName = TUBE_CLIENT
		cli := NewTubeNode(cliName, &cliCfg)
		err := cli.InitAndStart()
		panicOn(err)
		defer cli.Close()

		// request new session
		// seems like we want RPC semantics for this
		// and maybe for other calls?
		_ = bkg.Done()
		sess, err := cli.CreateNewSession(bkg, leaderURL)
		panicOn(err)
		if sess.ctx == nil {
			panic(fmt.Sprintf("sess.ctx should be not nil"))
		}
		vv("got sess = '%v'", sess) // not seen.

		var v []byte
		i := 0
		N := 10
		for range N {
			i++

			// Write
			v = []byte(fmt.Sprintf("%v", i))
			vv("707 about to sess.Write '%v'; sess.SessionSerial=%v", string(v), sess.SessionSerial) // not seen.
			// sess.Write automatically does sess.SessionSerial++ for us.
			txtW, err := sess.Write(bkg, "", "a", v, 0, "", 0)
			panicOn(err)

			vv("good, past sess.Write at i = %v; now sess.SessionSerial = %v", i, sess.SessionSerial)
			//sess.SessionSerial++
			//txtW, err := cli.Write("a", v, 0, sess)
			//panicOn(err)
			_ = txtW

			// Read all member nodes
			for j := range numNodes {
				// don't use the session here to read from
				// other nodes.
				tktj, err := nodes[j].Read(bkg, "", "a", 0, nil)
				panicOn(err)
				vj := tktj.Val

				if !bytes.Equal(v, vj) {
					t.Fatalf("write a:'%v' to node0, but read back from node j=%v: '%v'", string(v), j, string(vj))
				}
				vv("good, we read from node j=%v val a = '%v'", j, string(vj))
			}
			// Read from the client using the Session

			vv("about to try a sess.Read; sess.SessionSerial = %v", sess.SessionSerial)
			tktj, err := sess.Read(bkg, "", "a", 0)
			panicOn(err)
			vj := tktj.Val

			if !bytes.Equal(v, vj) {
				t.Fatalf("write a:'%v' to node0, but cli session read back from leader the value: '%v'", string(v), string(vj))
			}
			vv("good, as expected we read from Session on client->leader val a = '%v'", string(vj))
		} // end for range N

		vv("end of t.Loop i = %v", i)

	})
}

// 708
// Test for session being dropped/error-ed out
// if SessionSerial gap detected,
// since that means that a client did not recognize their earlier
// request being returned... right?
//
// Test that repeated read of same with same SessionSerial
// gives same reply / preserves linearizability.
func Test708_client_linz_SessionSerial_gap_caught(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 3

		c, _, _, _ := setupTestCluster(t, numNodes, 0, 708)
		defer c.Close()

		nodes := c.Nodes
		_ = nodes

		leaderNode := c.Nodes[0]
		leaderURL := leaderNode.URL

		cliName := "client708"
		cliCfg := *c.Cfg
		cliCfg.PeerServiceName = TUBE_CLIENT
		cli := NewTubeNode(cliName, &cliCfg)
		err := cli.InitAndStart()
		panicOn(err)
		defer cli.Close()

		// request new session
		// seems like we want RPC semantics for this
		// and maybe for other calls?
		sess, err := cli.CreateNewSession(bkg, leaderURL)
		panicOn(err)
		//vv("got sess = '%v'", sess)

		var v []byte
		i := 0
		N := 10
		itargetSkip := 6
		itargetDup := 3
		var dupOrigAns *Ticket
		for range N {
			i++

			// Write
			v = []byte(fmt.Sprintf("%v", i))
			//vv("about to write '%v'", string(v))

			if i == itargetSkip {
				sess.SessionSerial++ // should be enough to crash the session.
			}
			if i == itargetDup {
				// resubmitting the same session serial number should
				// should get same answer as before, and not add to
				// the raft log any length.
				sess.SessionSerial--
			}
			tktW, err := sess.Write(bkg, "", "a", v, 0, "", 0)
			if i == itargetSkip {
				// see test 710:
				// our new understanding is that gaps do need
				// to be allowed for fast local reads and
				// leadership change together. So commenting:
				/*
					if err == nil {
						panic("wanted serial number gap error")
					}
					vv("good: see error when gap in SessionSerial: '%v'", err)
					return // session is dead anyway, right?
				*/
			} else {
				panicOn(err)
			}

			if i == itargetDup-1 {
				dupOrigAns = tktW
			}
			if i == itargetDup {
				if !tktW.DupDetected {
					panicf("expected tktW.DupDetected at itargetDup=%v", itargetDup)
				}
				if tktW.LogIndex != dupOrigAns.LogIndex {
					panicf("expected dup SN to give same LogIndex in answer, but tktW.LogIndex(%v) != dupOrigAns.LogIndex(%v)", tktW.LogIndex, dupOrigAns.LogIndex)
				}
				if tktW.AsOfLogIndex != dupOrigAns.AsOfLogIndex {
					panicf("expected dup SN to give same LogIndex in answer, but tktW.AsOfLogIndex(%v) != dupOrigAns.AsOfLogIndex(%v)", tktW.AsOfLogIndex, dupOrigAns.AsOfLogIndex)
				}
				if tktW.Term != dupOrigAns.Term {
					panicf("expected dup SN to give same Term in answer, but tktW.Term(%v) != dupOrigAns.Term(%v)", tktW.Term, dupOrigAns.Term)
				}
				vv("good: server handled when client duplicated session number")
			}

			// if we repeat a SessionSerial, it should just be
			// idempotent, and not error out.
			if i == 2 {
				// using cli.Write means no automatic increment
				// of sess.SessionSerial
				tktW, err := cli.Write(bkg, "", "a", v, 0, sess, "", 0)
				panicOn(err)
				if !tktW.DupDetected {
					panic("expected to see DupDetected")
				}
			}
			sess.MinSessSerialWaiting = int64(i)

			/*
				// Read all member nodes
				for j := range numNodes {
					// don't use the session here to read from
					// other nodes.
					tktj, err := nodes[j].Read("a", 0, nil)
					panicOn(err)
					vj := tktj.Val

					if !bytes.Equal(v, vj) {
						t.Fatalf("write a:'%v' to node0, but read back from node j=%v: '%v'", string(v), j, string(vj))
					}
					vv("good, we read from node j=%v val a = '%v'", j, string(vj))
				}
				// Read from the client using the Session

				tktj, err := sess.Read("a", 0)
				panicOn(err)
				vj := tktj.Val

				if !bytes.Equal(v, vj) {
					t.Fatalf("write a:'%v' to node0, but cli session read back from leader the value: '%v'", string(v), string(vj))
				}
				vv("good, as expected we read from Session on client->leader val a = '%v'", string(vj))
			*/
		}

		vv("end of t.Loop i = %v", i)

	})
}

// This test no longer makes sense to me. Old S/N retries could
// be from network duplicates, but fine, the cache and dedup
// mecahnism is supposed to allow retries and just provide
// idempotency, not errors.
//
/*
// if the client decreases (uses an old SN) the Write is rejected.
func Test709_client_linz_SessionSerial_old_or_decreasing_SN_caught(t *testing.T) {

	// subtracting 1, 2, or 3 from the session serial number
	// before doing the Write--each of these should be caught
	// as a problem (like same client somehow using an old number again).
	for subtractMe := 1; subtractMe < 4; subtractMe++ {

		bubbleOrNot(t, func(t *testing.T) {

			numNodes := 3

			c, _, _, _ := setupTestCluster(t, numNodes, 0, 709)
			defer c.Close()

			nodes := c.Nodes
			_ = nodes

			leaderNode := c.Nodes[0]
			leaderURL := leaderNode.URL

			cliName := "client709"
			cliCfg := *c.Cfg
			cliCfg.PeerServiceName = TUBE_CLIENT
			cli := NewTubeNode(cliName, &cliCfg)
			err := cli.InitAndStart()
			panicOn(err)
			defer cli.Close()

			// request new session
			// seems like we want RPC semantics for this
			// and maybe for other calls?
			sess, err := cli.CreateNewSession(bkg, leaderURL)
			panicOn(err)
			//vv("got sess = '%v'", sess)

			var v []byte

			N := 10
			itargetDecrease := 0
			for i := range N {

				// Write
				v = []byte(fmt.Sprintf("%v", i))
				vv("about to write '%v'", string(v))

				if i == itargetDecrease {
					// since sess automatically increments, it
					// takes two decrements to actually decrease,
					// not just repeat, a serial number.
					sess.SessionSerial -= int64(subtractMe)
					vv("at itargetDecrease = %v, we set sess.SessionSerial = %v", i, sess.SessionSerial)
				}
				tktW, err := sess.Write(bkg, "", "a", v, 0, "", 0)
				_ = tktW
				if i == itargetDecrease {
					if err == nil {
						panic("wanted serial number decrease error")
					}
					vv("good: see error when gap in SessionSerial: '%v'", err)
					return // session is dead anyway, right?
				} else {
					panicOn(err)
				}
				if i > itargetDecrease {
					panicf("should have exited on error at when i was equal itargetDecrease(%v), but i is now %v", itargetDecrease, i)
				}

				// if we repeat a SessionSerial, it should just be
				// idempotent, and not error out.
				if i == 2 {
					// using cli.Write means no automatic increment
					// of sess.SessionSerial
					tktW, err := cli.Write(bkg, "", "a", v, 0, sess, "", 0)
					panicOn(err)
					if !tktW.DupDetected {
						panic("expected to see DupDetected")
					}
				}
				sess.MinSessSerialWaiting = int64(i)

			}

			//vv("end of t.Loop i = %v", i)

		})
	}
}
*/

// if the client session writes and then reads locally, and
// then the leadership changes, does the new leader always
// reject the next serial number as having gapped? The client
// and the server disagree on what the next serial number
// should be, but only the s/n for the write matters, right?
// So we think we can delete the logic that crashes the
// client session on a gap in serial numbers. Done/green now.
func Test710_client_linz_SessionSerial_leadership_change(t *testing.T) {

	onlyBubbled(t, func(t *testing.T) {

		numNodes := 3

		c, _, _, _ := setupTestCluster(t, numNodes, 0, 710)
		defer c.Close()

		nodes := c.Nodes
		_ = nodes

		leaderNode0 := c.Nodes[0]
		leaderURL0 := leaderNode0.URL
		vv("leaderURL0 = '%v'", leaderURL0)
		vv("URL1 = '%v'", c.Nodes[1].URL)
		vv("URL2 = '%v'", c.Nodes[2].URL)

		cliName := "client710"
		cliCfg := *c.Cfg
		cliCfg.PeerServiceName = TUBE_CLIENT
		for _, node := range c.Nodes {
			cliCfg.Node2Addr[node.name] = node.URL
		}
		cli := NewTubeNode(cliName, &cliCfg)
		err := cli.InitAndStart()
		panicOn(err)
		defer cli.Close()

		sess, err := cli.CreateNewSession(bkg, leaderURL0)
		panicOn(err)
		//vv("got sess = '%v'", sess)

		var v []byte

		// Write
		v = []byte(fmt.Sprintf("%v", 99))
		//vv("about to write '%v'", string(v))
		tktW, err := sess.Write(bkg, "", "a", v, 0, "", 0)
		panicOn(err)
		_ = tktW

		// make sure write goes through
		time.Sleep(time.Second * 10)

		// Read from the client using the Session to increment the SessionSerial
		// on leader(node 0)

		tktj, err := sess.Read(bkg, "", "a", 0)
		panicOn(err)
		vj := tktj.Val

		if !bytes.Equal(v, vj) {
			t.Fatalf("write a:'%v' to node0, but cli session read back from leader the value: '%v'", string(v), string(vj))
		}

		snap0 := c.SimnetSnapshot()
		vv("before crashing the leader, snap0 = '%v'", snap0) // .LongString())

		// now change leaders, new leader will not know about the
		// previous read.
		vv("about to crash the leader")
		leaderNode0.SimCrash()

		time.Sleep(time.Second * 50)

		snap := c.SimnetSnapshot()
		vv("50 seconds after crashing the leader, snap = '%v'", snap) // .LongString())

		vv("try 2nd read from the session, after leader change; current cli.leaderName = '%v'", cli.leaderName)

		leaderURL, leaderName, _, reallyLeader, _, err := cli.HelperFindLeader(&cli.cfg, c.Nodes[1].name, false)
		panicOn(err)
		vv("HelperFindLeader returned leaderURL='%v', reallyLeader = %v", leaderURL, reallyLeader)

		// try to fix the sporadic race where
		// cli might not have updated its own s.leaderName !?!
		// and so sends the next Read off into the void at the old dead leader.
		// getCircuitToLeader sets the updated s.leaderName.
		//ckt2, onlyPossibleAddr2, _, err2 :=
		_, _, _, err2 := cli.getCircuitToLeader(bkg, leaderURL, nil, false)
		panicOn(err2)

		if !reallyLeader {
			panicf("could not really find leader; leaderName='%v'; leaderURL = '%v'", leaderName, leaderURL)
		}
		vv("2nd leaderName = '%v'; url = '%v'", leaderName, leaderURL)

		to, canc := context.WithTimeout(bkg, time.Second*10)
		tktj, err = sess.Read(to, "", "a", 0)
		vv("back from sess.Read of a from 2nd leader; err='%v'", err)

		if err != nil && err.Error() == "context deadline exceeded" {
			vv("timed out!")
			// network seems fine, read message never gets to leader.
			// not in dropped queue, so means it was not sent??
			//snap := c.SimnetSnapshot()
			//vv("timeout contacting 2nd leader '%v', snap = '%v'", leaderName, snap) // .LongString())
			//vv("timeout contacting 2nd leader '%v', snap full = '%v'", leaderName, snap.LongString())
		}

		// without the cli.getCircuitToLeader() call after
		// the HelperFindLeader() call, we got
		// sporadic context deadline exceeded here
		// due to s.leaderName still pointing to the dead leader.
		panicOn(err)
		canc()
		vj = tktj.Val

		if !bytes.Equal(v, vj) {
			t.Fatalf("write a:'%v' to node0, but cli session read back from leader the value: '%v'", string(v), string(vj))
		}
		vv("ok 2nd read from the session, after leader change")

		//vv("good, as expected we read from Session on client->leader val a = '%v'", string(vj))

	})
}

// TODO: test for deletion of session after they timeout/go stale.
// TODO: test all the stuff that chapter 6 discusses.
