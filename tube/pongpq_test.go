package tube

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func Test302_pong_quorum(t *testing.T) {
	pq := newPongPQ("test302")

	now := time.Now()
	var slc []*Pong
	n := 3
	for i := range n {
		pong := &Pong{
			PeerID: fmt.Sprintf("%v", i),
			RecvTm: now.Add(time.Duration(-i) * time.Second),
		}
		slc = append(slc, pong)
		pq.add(*pong)
	}
	//vv("slc = '%#v'", slc)
	//vv("pq = %v", pq)
	for quor := 0; quor <= 4; quor++ {
		tm, ok, oldestPong := pq.quorumPongTm(quor, func(string) bool { return true })
		if quor <= 3 {
			if !ok {
				panic(fmt.Sprintf("why not ok? quor=%v", quor))
			}
		} else {
			if ok {
				panic(fmt.Sprintf("quor=%v, why ok?", quor))
			}
		}
		if quor > 3 || quor <= 0 {
			continue
		}
		// quor = 1,2,3
		if got, want := oldestPong.PeerID, slc[quor-1].PeerID; got != want {
			panic(fmt.Sprintf("quor=%v; got '%v', want '%v'", quor, got, want))
		}
		if !tm.Equal(slc[quor-1].RecvTm) {
			panic(fmt.Sprintf("expected RecvTm from slc[quor], got '%v'", tm))
		}
	}
}

func Test311_noEpoch(t *testing.T) {
	if want, got := int64(1), noEpoch(1<<32+1); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
	if want, got := int64(0), noEpoch(1<<32); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
	if want, got := int64(1<<32-1), noEpoch(1<<32-1); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
	if want, got := int64(1<<32-1), noEpoch(1<<32-1); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
}

func Test312_justEpoch(t *testing.T) {
	if want, got := int32(math.MinInt32), justEpoch(math.MinInt64); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
	if want, got := int32(0), justEpoch(1<<32-1); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
	if want, got := int32(1), justEpoch(1<<32); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
	if want, got := int32(math.MaxInt32), justEpoch(math.MaxInt64); got != want {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
}

func TestEmptyPongQueueCannotSatisfyRemoteQuorum(t *testing.T) {
	pq := newPongPQ("leader")
	if _, ok, _ := pq.quorumPongTm(1, func(string) bool { return true }); ok {
		t.Fatal("empty queue satisfied a quorum requiring a remote voter")
	}
}

func TestLeaderPongQuorumTracksCurrentMembership(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, nil, "n1", "n2", "n3")
	now := time.Now()
	n.leaderFullPongPQ.add(Pong{PeerID: "n2_id", PeerName: "n2", RecvTm: now})
	check := func(want bool) {
		t.Helper()
		_, ok, _ := n.leaderPongQuorumTm()
		if ok != want {
			t.Errorf("members %v: pong quorum %v, want %v", n.state.MC.Short(), ok, want)
		}
	}
	check(true) // n1 and n2 form a majority
	n.state.MC = specularMC(n, "n1", "n3", "n4")
	check(false) // n2's previously valid pong outlived its membership
	n.state.MC = specularMC(n, "n2", "n3", "n4")
	check(false) // n1 can no longer count itself
	n.leaderFullPongPQ.add(Pong{PeerID: "n3_id", PeerName: "n3", RecvTm: now})
	check(true)
	n.state.MC = specularMC(n, "n1")
	check(true)
}
