package tube

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// SCEN0=1001 SCEN_BATCH=1 go test -v -short -tags synctest -run '^Test104_swizzle_membership_changes$' -count=1 -timeout=0
// CHAINBLOCKS_WRITES controls the number of writes per seed, as in Test103.
func Test104_swizzle_membership_changes(t *testing.T) {
	runChainSeeds(t, runMemberFuzz)
}

// Only continuing voters can be relied on throughout a transition. Leave
// enough of them healthy for a majority of each configuration, even before
// the joiner catches up or after the departing voter stops.
func memberFaultBudget(oldSize, newSize int) int {
	return min(oldSize, newSize) - (max(oldSize, newSize)/2 + 1)
}

func TestMemberFuzzFaultBudget(t *testing.T) {
	for _, tc := range []struct{ from, to, want int }{
		{3, 3, 1}, {4, 4, 1}, {5, 5, 2}, {6, 6, 2}, {7, 7, 3},
		{3, 4, 0}, {4, 5, 1}, {5, 6, 1}, {6, 7, 2},
	} {
		for _, sizes := range [][2]int{{tc.from, tc.to}, {tc.to, tc.from}} {
			if got := memberFaultBudget(sizes[0], sizes[1]); got != tc.want {
				t.Errorf("%d -> %d voters: allowed %d faults, want %d", sizes[0], sizes[1], got, tc.want)
			}
		}
	}
}

func runMemberFuzz(t *testing.T, seed, writes int64, initial int) {
	const minVoters, maxVoters = 3, 7
	c, leader, faultRNG := startChainCluster(t, seed, writes, initial, 104)
	defer closeChainCluster(c)
	client := chainBlockStream{rng: newPRNG(intSeedToBytes(seed))}
	membershipSeed := intSeedToBytes(seed)
	membershipSeed[31] = 2
	rng := newPRNG(membershipSeed)
	faults := chainFaults{cluster: c, rng: faultRNG}
	defer faults.heal()
	voters := make([]int, initial)
	for i := range voters {
		voters[i] = i
	}
	var lastIndex, acked, adds, removes, changesWithFault int64
	minSeen, maxSeen := initial, initial
	defer func() {
		t.Logf("seed %d: %d/%d acknowledged, %d additions, %d removals (%d changes under fault), voters %d..%d, %d faults including %d swizzles; last fault %s",
			seed, acked, writes, adds, removes, changesWithFault, minSeen, maxSeen, faults.count, faults.swizzles, faults.last)
	}()

	for range writes {
		n := len(voters)
		nextSize, target := n, -1
		change := acked > 0 && rng.pseudoRandNonNegInt64Range(8) == 0
		if change {
			faults.heal()
			leader = waitMemberConfig(t, c, voters)
			var candidates []int
			if n == minVoters || (n < maxVoters && rng.pseudoRandBool()) {
				nextSize++
				// Removed voters stay online as Tube's nonvoting shadows. Up
				// to two additional processes can join this five-voter bootstrap.
				for i := range min(len(c.Nodes)+1, maxVoters) {
					if !slices.Contains(voters, i) {
						candidates = append(candidates, i)
					}
				}
			} else {
				nextSize--
				for _, i := range voters {
					if i != leader {
						candidates = append(candidates, i)
					}
				}
			}
			target = candidates[rng.pseudoRandNonNegInt64Range(int64(len(candidates)))]
			if target == len(c.Nodes) {
				startMemberFuzzReplica(t, c, seed, writes)
			}
		}

		if faultRNG.pseudoRandNonNegInt64Range(3) == 0 {
			faults.maxFaults = memberFaultBudget(n, nextSize)
			faults.nodes = faults.nodes[:0]
			for _, i := range voters {
				if change && (i == leader || i == target) {
					continue
				}
				faults.nodes = append(faults.nodes, i)
			}
			if faults.maxFaults > 0 {
				leader = faults.inject(t, leader, acked+1)
				if change {
					changesWithFault++
				}
			}
		}

		if change {
			var next []int
			if nextSize > n {
				next = append(slices.Clone(voters), target)
				slices.Sort(next)
			} else {
				pos := slices.Index(voters, target)
				next = slices.Delete(slices.Clone(voters), pos, pos+1)
			}
			changeMemberVoters(t, c, leader, target, voters, next, &faults)
			leader = waitMemberConfig(t, c, next)
			if nextSize > n {
				adds++
				checkChainHead(t, c.Nodes[target], lastIndex, acked, client.digest)
			} else {
				removes++
			}
			voters = next
			minSeen, maxSeen = min(minSeen, nextSize), max(maxSeen, nextSize)
		}

		leader = latestChainLeader(c, leader)
		payload := client.next()
		leader, lastIndex = submitChainBlock(t, c, leader, payload, &client, voters)
		acked++
		if t.Failed() {
			return
		}
		if acked == 1 || acked%1_000 == 0 {
			t.Logf("seed %d: %d/%d writes acknowledged at committed index %d, %d voters, %d additions, %d removals, %d faults, %d swizzles",
				seed, acked, writes, lastIndex, len(voters), adds, removes, faults.count, faults.swizzles)
		}
	}
	faults.heal()
	waitMemberConfig(t, c, voters)
	// Removed voters remain live shadow replicas and replicate the same log.
	for _, node := range c.Nodes {
		checkChainHead(t, node, lastIndex, writes, client.digest)
	}
}

func startMemberFuzzReplica(t *testing.T, c *TubeCluster, seed, writes int64) {
	t.Helper()
	i := len(c.Nodes)
	name := fmt.Sprintf("node_%d", i)
	cfg := *c.Cfg
	oracle := chainBlockStream{rng: newPRNG(intSeedToBytes(seed))}
	cfg.testApplyUserDefinedOp = func(node *TubeNode, cmd *Ticket) { applyChainBlock(t, node, cmd, &oracle, writes) }
	node := NewTubeNode(name, &cfg)
	// NoDisk test startup preserves the supplied state. The bootstrap
	// helper creates an in-memory persistor for each original voter.
	// Joining voters need one too to apply entries committed by heartbeat.
	var err error
	node.saver, _, err = cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
	if err != nil {
		t.Fatalf("open joining voter %s state: %v", name, err)
	}
	if err := node.InitAndStart(); err != nil {
		t.Fatalf("start joining voter %s: %v", name, err)
	}
	c.Name2num[name] = i
	c.Nodes = append(c.Nodes, node)
	drainChainDiagnostics(c, node)
}

func memberSetMatches(mc *MemberConfig, c *TubeCluster, voters []int) bool {
	if mc == nil || mc.PeerNames.Len() != len(voters) {
		return false
	}
	for _, i := range voters {
		d, ok := mc.PeerNames.Get2(c.Nodes[i].name)
		if !ok || d == nil || d.NonVoting {
			return false
		}
	}
	return true
}

// One non-forced voting change is in flight at a time. A lost acknowledgment
// can leave the new configuration installed. Inspect the committed state
// before deciding whether to retry the same change.
func changeMemberVoters(t *testing.T, c *TubeCluster, leader, target int, old, next []int, f *chainFaults) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	node := c.Nodes[target]
	var lastErr error
	for {
		l := c.Nodes[leader]
		if len(next) > len(old) {
			_, _, lastErr = node.AddPeerIDToCluster(ctx, false, false, node.name, node.PeerID, node.PeerServiceName, node.BaseServerHostPort(), l.URL, 0)
		} else {
			_, _, lastErr = l.RemovePeerIDFromCluster(ctx, false, false, node.name, node.PeerID, node.PeerServiceName, node.BaseServerHostPort(), l.URL, 0)
		}
		f.heal()
		if lastErr == nil {
			return
		}
		for {
			if ctx.Err() != nil {
				t.Fatalf("change voter %s from %v to %v: %v (last RPC: %v)", node.name, old, next, ctx.Err(), lastErr)
			}
			time.Sleep(c.Cfg.HeartbeatDur)
			leader = latestChainLeader(c, leader)
			look := c.Nodes[leader].Inspect()
			if look.State == nil {
				t.Fatalf("leader %s stopped during change: %v", c.Nodes[leader].name, lastErr)
			}
			look.State.KVstore.Close()
			if look.Role != LEADER || look.State.LastAppliedTerm != look.State.CurrentTerm || look.MC == nil || !look.MC.IsCommitted {
				continue
			}
			if memberSetMatches(look.MC, c, next) {
				return
			}
			if !memberSetMatches(look.MC, c, old) {
				t.Fatalf("unexpected committed configuration %s during change %v -> %v", look.MC.Short(), old, next)
			}
			break // Old configuration is still committed. Retry on its leader.
		}
	}
}

// Require each expected voter to see the committed voting set. A connected
// process or a successful administrative RPC alone does not prove admission.
func waitMemberConfig(t *testing.T, c *TubeCluster, voters []int) int {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		leader, ready := -1, 0
		var version *MemberConfig
		for _, i := range voters {
			voter := c.Nodes[i]
			look := voter.Inspect()
			if look.State == nil {
				t.Fatalf("%s stopped while in the voting set", voter.name)
			}
			look.State.KVstore.Close()
			if memberSetMatches(look.MC, c, voters) && look.MC.IsCommitted {
				if version == nil {
					version = look.MC
				} else if !version.sameVersionNumber(look.MC) {
					continue
				}
				ready++
				if look.Role == LEADER && look.State.LastAppliedTerm == look.State.CurrentTerm {
					leader = i
				}
			}
		}
		if ready == len(voters) && leader >= 0 {
			return leader
		}
		if !time.Now().Before(deadline) {
			for _, i := range voters {
				look := c.Nodes[i].Inspect()
				t.Logf("%s: role=%v term=%d commit=%d applied=%d config=%s",
					c.Nodes[i].name, look.Role, look.State.CurrentTerm, look.State.CommitIndex, look.State.LastApplied, look.MC.Short())
				look.State.KVstore.Close()
			}
			t.Fatalf("committed voting set %v reached %d/%d voters, leader index %d", voters, ready, len(voters), leader)
		}
		time.Sleep(c.Cfg.HeartbeatDur)
	}
}
