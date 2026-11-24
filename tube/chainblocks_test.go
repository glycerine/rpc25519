package tube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"time"

	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/tube/leased"
)

// Chain of Blocks keeps one leaf per replica. Version counts application
// commands (Raft no-ops and membership changes do not count), and Value is
// SHA256(previous digest || command), starting with a 32-byte zero digest.
const chainTable, chainKey = "chainblocks", "head"

// Each seed drives one cluster for CHAINBLOCKS_WRITES commands (default one million).
// SCEN0=990 SCEN_BATCH=1 go test -tags synctest -run '^Test103_ChainOfBlocks$' -count=1 -timeout=0
func Test103_ChainOfBlocks(t *testing.T) {
	if !faketime {
		t.Skip("Chain of Blocks uses simnet with the synctest build tag")
	}
	envInt := func(name string, fallback int64) int64 {
		s, ok := os.LookupEnv(name)
		if !ok {
			return fallback
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			t.Fatalf("%s must be a nonnegative integer, got %q", name, s)
		}
		return n
	}
	first, batch := int64(1001), int64(1)
	if os.Getenv("SCEN0") != "" {
		first, batch = envInt("SCEN0", first), envInt("SCEN_BATCH", batch)
	}
	if batch == 0 || first > math.MaxInt64-(batch-1) {
		t.Fatalf("invalid scenario range SCEN0=%d SCEN_BATCH=%d", first, batch)
	}
	writes := envInt("CHAINBLOCKS_WRITES", 1_000_000)
	if writes == 0 {
		t.Fatal("CHAINBLOCKS_WRITES must be positive")
	}
	voters := 5
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	for offset := range batch {
		seed := first + offset
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			onlyBubbled(t, func(t *testing.T) { runChainBlocks(t, seed, writes, voters) })
		})
	}
}

// Check the transport effect directly: traffic queues on the clogged hosts,
// and the first release drains just that host while the other stays stalled.
func TestChainBlocksSwizzleRecovery(t *testing.T) {
	if !faketime {
		t.Skip("swizzle clogging uses simnet with the synctest build tag")
	}
	onlyBubbled(t, func(t *testing.T) {
		c, _, _, _ := SetupTestCluster(t, 5, 0, 103)
		defer c.Close()
		steps := swizzleClog{
			{node: 1},
			{at: time.Second, node: 2},
			{at: 2 * time.Second, node: 2, release: true},
			{at: 3 * time.Second, node: 1, release: true},
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			steps.run(c)
		}()
		defer func() { <-done }()
		check := func(stalled ...int) {
			t.Helper()
			snap := c.SimnetSnapshot(false)
			for i, n := range c.Nodes {
				peer := snap.Peermap[n.srvname]
				if peer == nil || len(peer.Conn) == 0 {
					t.Fatalf("%s has no simnet connections", n.name)
				}
				want := slices.Contains(stalled, i)
				queued := false
				for _, conn := range peer.Conn {
					if (conn.DropSendProb == 1) != want {
						t.Fatalf("%s to %s: drop probability %g, want stalled=%v", conn.Origin, conn.Target, conn.DropSendProb, want)
					}
					queued = queued || conn.DroppedSendQ.Len() > 0
				}
				if queued != want {
					t.Fatalf("%s: queued sends=%v, want %v", n.name, queued, want)
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
		check(1)
		time.Sleep(time.Second)
		check(1, 2)
		time.Sleep(time.Second)
		check(1)
		<-done
		check()
	})
}

// Each replica has its own cursor. A snapshot can advance its applied count
// past several callbacks, in which case the oracle regenerates that prefix.
type chainBlockStream struct {
	rng    *prng
	count  int64
	digest [sha256.Size]byte
}

func (s *chainBlockStream) next() []byte {
	payload := make([]byte, s.rng.pseudoRandNonNegInt64Range(513))
	s.rng.cha8.Read(payload)
	s.digest = sha256.Sum256(append(s.digest[:], payload...))
	s.count++
	return payload
}

func runChainBlocks(t *testing.T, seed, writes int64, voters int) {
	seedBytes := intSeedToBytes(seed)
	client := chainBlockStream{rng: newPRNG(seedBytes)}
	replicas := make([]chainBlockStream, voters)
	nodes := make([]int, voters)
	for i := range voters {
		replicas[i].rng = newPRNG(seedBytes)
		nodes[i] = i
	}
	// Changing the fault schedule must not change the command stream.
	faultSeed := seedBytes
	faultSeed[31] = 1
	rng := newPRNG(faultSeed)
	rnd := rng.pseudoRandNonNegInt64Range
	// PrepareForSimnet reserves zero for continuing the process-wide stream.
	rpcSeed := seed
	if rpcSeed == 0 {
		rpcSeed = -1
	}
	rpc.PrepareForSimnet(rpcSeed, true)

	cfg := NewTubeConfigTest(voters, t.Name(), true)
	cfg.InitialSimnetScenario = seed
	cfg.RpcCfg.InitialSimnetScenario = seed
	cfg.prng = newPRNG(seedBytes)
	if rnd(2) == 0 {
		cfg.CompactionThresholdBytes = 1
	}
	cfg.testApplyUserDefinedOp = func(node *TubeNode, cmd *Ticket) {
		i := node.cfg.testCluster.Name2num[node.name]
		applyChainBlock(t, node, cmd, &replicas[i], writes)
	}
	c, _, leader, _ := SetupTestClusterWithCustomConfig(cfg, t, voters, 0, 103)
	sim := cfg.RpcCfg.GetSimnet()
	defer func() {
		c.Close()
		<-sim.Halt.Done.Chan
	}()
	// Setup has consumed the startup notifications. Drain the diagnostic
	// copies of each AppendEntries and ACK throughout the long run.
	for _, node := range c.Nodes {
		go func() {
			for {
				select {
				case <-node.testGotAeFromLeader:
				case <-node.testGotAeAckFromFollower:
				case <-node.testGotGoodAE_lastLogIndexNow:
				case <-node.testWatchdogTimeoutReconnectCh:
				case <-node.verifyPeerReplicaOrNot:
				case <-c.termChanges:
				case <-c.LeaderElectedCh:
				case <-node.Halt.ReqStop.Chan:
					return
				}
			}
		}()
	}

	t.Logf("seed %d, %d voters, %d writes, compaction threshold bytes %d", seed, voters, writes, cfg.CompactionThresholdBytes)
	var lastIndex, acked, faults, swizzles int64
	var lastFault string
	defer func() {
		t.Logf("seed %d: %d/%d acknowledged, %d faults including %d swizzles; last fault %s", seed, acked, writes, faults, swizzles, lastFault)
	}()
	var swizzleDone chan struct{}
	active := false
	heal := func() {
		if swizzleDone != nil {
			<-swizzleDone
			swizzleDone = nil
		}
		if active {
			c.AllHealthyAndPowerOn(rng.pseudoRandBool())
			active = false
		}
	}
	defer heal()
	for range writes {
		// One Bernoulli trial per client command, including while faults are active.
		if rnd(3) == 0 {
			heal()
			faults++
			victim := int(rnd(int64(voters)))
			switch rnd(4) {
			case 0:
				plan := newSwizzleClog(rng, nodes, voters)
				lastFault = fmt.Sprintf("swizzle at write %d: %+v", acked+1, plan)
				swizzles++
				done := make(chan struct{})
				swizzleDone = done
				go func() {
					defer close(done)
					plan.run(c)
				}()
			default:
				leader = latestChainLeader(c, leader)
				active = true
				if victim == leader || rng.pseudoRandBool() {
					lastFault = fmt.Sprintf("isolate voter %d at write %d", victim, acked+1)
					c.IsolateNode(victim)
					if victim == leader {
						select {
						case name := <-c.LeaderNoop0committedCh:
							leader = c.Name2num[name]
							if leader == victim {
								t.Fatalf("isolated %s reported a new committed term", name)
							}
						case <-time.After(time.Minute):
							t.Fatalf("no new leader after isolating %s", c.Nodes[victim].name)
						}
					}
				} else {
					p := 1.0
					if rng.pseudoRandBool() {
						p = rng.float64prob()
					}
					if rng.pseudoRandBool() {
						lastFault = fmt.Sprintf("drop sends on voter %d p=%g at write %d", victim, p, acked+1)
						c.Nodes[victim].DropSends(p)
					} else {
						lastFault = fmt.Sprintf("deaf reads on voter %d p=%g at write %d", victim, p, acked+1)
						c.Nodes[victim].DeafToReads(p)
					}
				}
			}
		}
		leader = latestChainLeader(c, leader)
		payload := client.next()
		leader, lastIndex = submitChainBlock(t, c, leader, payload, &client)
		acked++
		if t.Failed() {
			return
		}
		if acked%10_000 == 0 {
			t.Logf("seed %d: %d writes acknowledged, %d faults, %d swizzles", seed, acked, faults, swizzles)
		}
	}
	heal()
	for _, node := range c.Nodes {
		checkChainHead(t, node, lastIndex, writes, client.digest)
	}
}

// Consume committed-term notifications, including elections during a swizzle.
func latestChainLeader(c *TubeCluster, leader int) int {
	for {
		select {
		case name := <-c.LeaderNoop0committedCh:
			leader = c.Name2num[name]
		default:
			return leader
		}
	}
}

func applyChainBlock(t *testing.T, node *TubeNode, cmd *Ticket, oracle *chainBlockStream, writes int64) {
	store := node.state.KVstore
	head, err := store.GetLeaf(chainTable, chainKey, false)
	switch err {
	case ErrKeyNotFound:
		head = leased.NewLeaf(chainKey, make([]byte, sha256.Size), "")
	case nil:
	default:
		t.Errorf("%s reading chain head at log index %d: %v", node.name, cmd.LogIndex, err)
		return
	}
	n := head.Version
	if n < oracle.count || n >= writes {
		t.Errorf("%s at log index %d: prior count %d outside [%d,%d)", node.name, cmd.LogIndex, n, oracle.count, writes)
		return
	}
	for oracle.count < n {
		oracle.next()
	}
	if !bytes.Equal(head.Value, oracle.digest[:]) {
		t.Errorf("%s at log index %d: prior hash after %d commands %x, want %x", node.name, cmd.LogIndex, n, head.Value, oracle.digest)
		return
	}
	head.Version++
	digest := sha256.Sum256(append(head.Value, cmd.Val...))
	head.Value = digest[:]
	if err := store.PutLeaf(chainTable, head); err != nil {
		t.Errorf("%s storing chain head at log index %d: %v", node.name, cmd.LogIndex, err)
		return
	}
	oracle.next()
	if digest != oracle.digest {
		t.Errorf("%s after %d commands at log index %d: hash %x, want %x", node.name, head.Version, cmd.LogIndex, digest, oracle.digest)
	}
}

func checkChainHead(t *testing.T, node *TubeNode, lastIndex, count int64, digest [sha256.Size]byte) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		look := node.Inspect()
		state := look.State
		if state.LastApplied >= lastIndex {
			head, err := state.KVstore.GetLeaf(chainTable, chainKey, false)
			state.KVstore.Close()
			if err != nil {
				t.Fatalf("%s at log index %d: reading final chain head: %v", node.name, state.LastApplied, err)
			}
			if head.Version != count || !bytes.Equal(head.Value, digest[:]) {
				t.Errorf("%s final chain (%d, %x), want (%d, %x)", node.name, head.Version, head.Value, count, digest)
			}
			return
		}
		state.KVstore.Close()
		if !time.Now().Before(deadline) {
			t.Fatalf("%s stopped at applied index %d, waiting for %d (role %s, term %d, leader %s, commit %d, last log %d)",
				node.name, state.LastApplied, lastIndex, look.Role, state.CurrentTerm, look.CurrentLeaderName, state.CommitIndex, look.LastLogIndex)
		}
		time.Sleep(node.cfg.HeartbeatDur)
	}
}

// A leader can lose its term after appending but before acknowledging. A new
// leader that has applied an entry of its own term has decided the fate of
// every entry from that old term. Inspect the chain there before resubmitting.
func resolveChainWrite(t *testing.T, ctx context.Context, c *TubeCluster, oldTerm int64, client *chainBlockStream) (leader int, appliedIndex int64) {
	t.Helper()
	for {
		for i, node := range c.Nodes {
			look := node.Inspect()
			s := look.State
			if look.Role != LEADER || s.CurrentTerm <= oldTerm || s.LastAppliedTerm != s.CurrentTerm {
				s.KVstore.Close()
				continue
			}
			head, err := s.KVstore.GetLeaf(chainTable, chainKey, false)
			s.KVstore.Close()
			if err == ErrKeyNotFound && client.count == 1 {
				return i, 0
			}
			if err != nil {
				t.Fatalf("%s: resolving write %d after term %d: %v", node.name, client.count, oldTerm, err)
			}
			switch head.Version {
			case client.count:
				if !bytes.Equal(head.Value, client.digest[:]) {
					t.Fatalf("%s after recovered write %d: hash %x, want %x", node.name, client.count, head.Value, client.digest)
				}
				return i, s.LastApplied
			case client.count - 1:
				return i, 0
			default:
				t.Fatalf("%s: resolving write %d found chain count %d", node.name, client.count, head.Version)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no committed leader after term %d for write %d: %v", oldTerm, client.count, ctx.Err())
		case <-time.After(c.Cfg.HeartbeatDur):
		}
	}
}

func submitChainBlock(t *testing.T, c *TubeCluster, leader int, payload []byte, client *chainBlockStream) (int, int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for {
		node := c.Nodes[leader]
		cmd := node.NewTicket("chain block", "", "", payload, node.PeerID, node.name, USER_DEFINED_FSM_OP, 0, ctx)
		select {
		case node.writeReqCh <- cmd:
		case <-ctx.Done():
			t.Fatalf("submitting write %d to %s: %v", client.count, node.name, ctx.Err())
		}
		select {
		case <-cmd.Done.Chan:
		case <-ctx.Done():
			t.Fatalf("waiting for %s to acknowledge write %d: %v", node.name, client.count, ctx.Err())
		}
		if cmd.Err == nil && cmd.Applied && cmd.AsOfLogIndex > 0 {
			return leader, cmd.AsOfLogIndex
		}
		if !errors.Is(cmd.Err, ErrLeaderStepDown) && !IsNoLeaderError(cmd.Err) {
			t.Fatalf("%s did not apply write %d: applied=%v index=%d err=%v", node.name, client.count, cmd.Applied, cmd.AsOfLogIndex, cmd.Err)
		}
		var index int64
		leader, index = resolveChainWrite(t, ctx, c, cmd.Term, client)
		if index != 0 {
			return leader, index
		}
	}
}
