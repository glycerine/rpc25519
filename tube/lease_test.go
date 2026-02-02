package tube

import (
	"strings"
	//"fmt"
	"time"

	"context"
	"testing"
)

func Test079_lease_from_leader(t *testing.T) {

	// leases allow a "Czar and master of partition" model:
	// a master's sublease must never be longer than the Czar's lease,
	// so that master can keep them in memory.

	// How heirarchical leases work to create RAM-only
	// fast updates of some state;
	// from Butler Lampson's 1996 paper, "How to Build
	// a Highly Available System Using Consensus":
	//
	// "Run consensus once to elect a czar C and give C
	// a lease on a large part of the state.
	// Now C gives out sub-leases on x and y to masters.
	// Each master controls its own resources. The masters
	// renew their sub-leases with the czar. This is cheap since it
	// doesn’t require any coordination. The czar renews
	// its lease by consensus. This costs more, but there's
	// only one czar lease. Also, the czar can be simple
	// and less likely to fail, so a longer lease may be
	// acceptable. Hierarchical leases are commonly used in
	// replicated file systems and in clusters.
	// By combining the ideas of consensus, leases, and
	// hierierarchy, it’s possible to build highly
	// available systems that are also highly efficient."

	// here we test that our write of a key can be leased
	// and not re-leased until that lease has expired.
	// This is the mechanism for the Czar to be established
	// via a lease.
	onlyBubbled(t, func(t *testing.T) {

		//now := time.Now()
		//vv("start at '%v' -> '%v'", now.UnixNano(), nice(now))

		numNodes := 3
		//n := 3

		cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
		cfg.MinElectionDur = 10 * time.Second
		cfg.HeartbeatDur = time.Second
		c, _, leadi, _ := setupTestClusterWithCustomConfig(cfg, t, numNodes, 0, 79)

		defer c.Close()
		//vv("c.Cfg = '%#v'", c.Cfg)

		if leadi != 0 {
			panicf("leader should be 0, not %v", leadi)
		}

		nodes := c.Nodes
		leader := nodes[0]
		ctx := context.Background()

		sess, err := nodes[1].CreateNewSession(ctx, leader.name, leader.URL)
		panicOn(err)
		defer sess.Close()
		vv("good: got session to leader (maybe from leader to leader, but meh, we cannot always know who is leader...). sess=%p", sess)

		leaseTable := Key("leaseTableA")
		leaseKey := Key("leaseKeyA")
		leaseVal := Val("leaseValA")
		leaseVtype := "lease"
		leaseRequestDur := time.Minute
		leaseTkt, err := sess.Write(ctx, leaseTable, leaseKey, leaseVal, 0, leaseVtype, leaseRequestDur, leaseAutoDelFalse)
		panicOn(err)
		now := time.Now()
		until := leaseTkt.LeaseUntilTm
		left := until.Sub(now)
		if left <= 0 {
			panicf("left(%v) <= 0; until='%v'; now='%v'", left, until, now)
		}
		vv("lease has left %v", left) // above true 3, below false 2

		// now have a second write attempted from a different
		// server. It should fail because the key is already leased out.

		time.Sleep(15 * time.Second)
		sess2, err := nodes[2].CreateNewSession(ctx, leader.name, leader.URL)
		leaseTkt2, err2 := sess2.Write(ctx, leaseTable, leaseKey, leaseVal, 0, leaseVtype, leaseRequestDur, leaseAutoDelFalse)
		if leaseTkt2.Err == nil || err2 == nil {
			panic("expected error from re-lease attempt")
		}
		if !strings.HasPrefix(err2.Error(), rejectedWritePrefix) {
			vv("expected prefix rejectedWritePrefix='%v', but err2 = '%v'", rejectedWritePrefix, err2)
		}
		vv("good: got rejection of premature re-lease by another = '%v'", leaseTkt2.Err)

		// original leasor should be allowed to extend before expiry.
		time.Sleep(15 * time.Second)

		leaseTkt, err = sess.Write(ctx, leaseTable, leaseKey, leaseVal, 0, leaseVtype, leaseRequestDur, leaseAutoDelFalse)
		panicOn(err)
		now = time.Now()
		until = leaseTkt.LeaseUntilTm
		left = until.Sub(now)
		if left <= 0 {
			panicf("left(%v) <= 0; until='%v'; now='%v'", left, until, now)
		}
		vv("good: re-newed lease has left %v", left)

		// can we read back the vtype?
		readTkt, err := sess.Read(ctx, leaseTable, leaseKey, 0)
		panicOn(err)
		if readTkt.Vtype != "lease" {
			panicf("expected lease vtype back, got '%v'", readTkt.Vtype)
		}

		// avoid bubble complaint about still live goro by sleeping 1 sec after.
		// since halter can take 500 msec to shutdown.
		c.Close()
		time.Sleep(time.Second)
	})
}

func Test078_leases_not_broken_early(t *testing.T) {

	// test kvstoreWrite(tkt *Ticket, dry, testingImmut bool) alone
	s := &TubeNode{
		state: &RaftState{},
	}
	now := time.Unix(0, 946684800000000000) // 2000-01-01T00:00:00.000Z
	dur := time.Minute
	until0 := now.Add(dur)
	tkt := &Ticket{
		Table:           Key("table"),
		Key:             Key("key"),
		Val:             Val("val"),
		FromID:          "A",
		RaftLogEntryTm:  now,
		LeaseRequestDur: dur,
		LeaseUntilTm:    until0,
		LogIndex:        1,
		Leasor:          "A",
	}
	until1 := until0.Add(dur)
	casEarly := &Ticket{
		Table:            Key("table"),
		Key:              Key("key"),
		Val:              Val("val"),
		OldLeaseEpochCAS: 1,
		FromID:           "B",
		RaftLogEntryTm:   until0,
		LeaseRequestDur:  dur,
		LeaseUntilTm:     until1,
		LogIndex:         2,
		Leasor:           "B",
	}
	casEarlyWrongPriorEpoch := &Ticket{
		Table:            Key("table"),
		Key:              Key("key"),
		Val:              Val("val"),
		OldLeaseEpochCAS: 2, // accurate is 1
		FromID:           "B",
		RaftLogEntryTm:   until0,
		LeaseRequestDur:  dur,
		LeaseUntilTm:     until1,
		LogIndex:         2,
		Leasor:           "B",
	}
	casExpired := &Ticket{
		Table:            Key("table"),
		Key:              Key("key"),
		Val:              Val("val"),
		OldLeaseEpochCAS: 1,
		FromID:           "B",
		RaftLogEntryTm:   until0.Add(1),
		LeaseRequestDur:  dur,
		LeaseUntilTm:     until1.Add(1),
		LogIndex:         2,
		Leasor:           "B",
	}
	casExpiredWrongPriorEpoch := &Ticket{
		Table:            Key("table"),
		Key:              Key("key"),
		Val:              Val("val"),
		OldLeaseEpochCAS: 2, // wrong, correct is 1.
		FromID:           "B",
		RaftLogEntryTm:   until0.Add(1),
		LeaseRequestDur:  dur,
		LeaseUntilTm:     until1.Add(1),
		LogIndex:         2,
		Leasor:           "B",
	}
	renew := &Ticket{
		Table:            Key("table"),
		Key:              Key("key"),
		Val:              Val("val"),
		OldLeaseEpochCAS: 1,
		FromID:           "A",
		RaftLogEntryTm:   until0,
		LeaseRequestDur:  dur,
		LeaseUntilTm:     until1,
		LogIndex:         2,
		Leasor:           "A",
	}
	renewWrongPriorEpoch := &Ticket{
		Table:            Key("table"),
		Key:              Key("key"),
		Val:              Val("val"),
		OldLeaseEpochCAS: 2, // wrong, should be 1.
		FromID:           "A",
		RaftLogEntryTm:   until0,
		LeaseRequestDur:  dur,
		LeaseUntilTm:     until1,
		LogIndex:         2,
		Leasor:           "A",
	}

	const dry = true
	const wet = false
	const testingImmut = true
	if !s.kvstoreWrite(tkt, wet, testingImmut) {
		panic("expect to write")
	}

	// leasor A can renew early
	if !s.kvstoreWrite(renew, dry, testingImmut) {
		panicf("expect cas accepted from renewer/same leaor; cas.Err='%v'", renew.Err)
	}
	renew.Err = nil

	// leasor B cannot lease until strictly after the expiry; [beg,endpoint] inclusive.
	if s.kvstoreWrite(casEarly, dry, testingImmut) {
		panicf("expect casEarly rejected; cas.Err='%v'", casEarly.Err)
	}
	casEarly.Err = nil
	// leasor B can lease after prior lease expiry.
	if !s.kvstoreWrite(casExpired, dry, testingImmut) {
		panicf("expect cas accepted; cas.Err='%v'", casExpired.Err)
	}
	casExpired.Err = nil

	// both A and B should be rejected on Compare-And-Swap check if OldLeaseEpochCAS is wrong
	s.doCAS(renewWrongPriorEpoch)
	if renewWrongPriorEpoch.Err == nil {
		panicf("expect renewWrongPriorEpoch rejected with wrong OldLeaseEpochCAS")
	}
	//vv("good, got cas err = '%v'", renewWrongPriorEpoch.Err)

	s.doCAS(casEarlyWrongPriorEpoch)
	if casEarlyWrongPriorEpoch.Err == nil {
		panicf("expect error on wrong prior epoch when lease is in force and CAS applied")
	}
	//vv("good, got cas err = '%v'", casEarlyWrongPriorEpoch.Err)

	// once lease is expired, we ignore CAS? so only prior leasor can
	// use CAS to renew early? what about new contendors for lease to prevent
	// double quick leasing in turn? methinks this should reject; else
	// there is no point in CAS other than a subset of a valid lease.

	s.doCAS(casExpiredWrongPriorEpoch)
	if casExpiredWrongPriorEpoch.Err == nil {
		panicf("expect casExpiredWrongPriorEpoch rejected with wrong OldLeaseEpochCAS") // hit
	}
	//vv("good, got cas err = '%v'", casExpiredWrongPriorEpoch.Err)
}
