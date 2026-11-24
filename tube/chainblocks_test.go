package tube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"runtime"
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

// Run a range with SCEN0=1001 SCEN_BATCH=100 go test -tags synctest -run '^Test103_ChainOfBlocks$' -count=1
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
			t.Fatalf("%s must be a nonnegative scenario number, got %q", name, s)
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
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	for offset := range batch {
		seed := first + offset
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			onlyBubbled(t, func(t *testing.T) { runChainBlocks(t, seed) })
		})
	}
}

func runChainBlocks(t *testing.T, seed int64) {
	const voters = 3
	seedBytes := intSeedToBytes(seed)
	rng := newPRNG(seedBytes)
	rnd := rng.pseudoRandNonNegInt64Range
	// PrepareForSimnet reserves zero for continuing the process-wide stream.
	rpcSeed := seed
	if rpcSeed == 0 {
		rpcSeed = -1
	}
	rpc.PrepareForSimnet(rpcSeed, true)

	// A serial client lets the test oracle know every expected prefix before
	// the replicas start. Only the raw command bytes go into the Raft log.
	commands := make([][]byte, 12+rnd(21))
	want := make([][sha256.Size]byte, len(commands)+1)
	for i := range commands {
		commands[i] = make([]byte, rnd(513))
		rng.cha8.Read(commands[i])
		want[i+1] = sha256.Sum256(append(want[i][:], commands[i]...))
	}
	cfg := NewTubeConfigTest(voters, t.Name(), true)
	cfg.InitialSimnetScenario = seed
	cfg.RpcCfg.InitialSimnetScenario = seed
	cfg.prng = newPRNG(seedBytes)
	if rnd(2) == 0 {
		cfg.CompactionThresholdBytes = 1
	}
	cfg.testApplyUserDefinedOp = func(node *TubeNode, cmd *Ticket) {
		applyChainBlock(t, node, cmd, want)
	}

	c, _, leader, _ := SetupTestClusterWithCustomConfig(cfg, t, voters, 0, 103)
	sim := cfg.RpcCfg.GetSimnet()
	defer func() {
		if t.Failed() {
			t.Log(c.SimnetSnapshot(true).ShortString())
		}
		c.Close()
		<-sim.Halt.Done.Chan
	}()

	// Leave a majority connected so each submitted command has a definite
	// outcome. Isolating the leader moves the client to the majority's leader.
	victim := int(rnd(voters))
	faultAt := 1 + int(rnd(3))
	healAt := faultAt + 3 + int(rnd(int64(len(commands)-faultAt-3)))
	t.Logf("seed %d: %d commands, fault on %s before command %d, heal before %d, compaction threshold %d",
		seed, len(commands), c.Nodes[victim].name, faultAt+1, healAt+1, cfg.CompactionThresholdBytes)
	var lastIndex int64
	for i, payload := range commands {
		if i == faultAt {
			if victim == leader {
				c.IsolateNode(victim)
				select {
				case name := <-c.LeaderNoop0committedCh:
					leader = c.Name2num[name]
					if leader == victim {
						t.Fatalf("isolated leader %s reported committing a new term", name)
					}
				case <-time.After(time.Minute):
					t.Fatalf("no new leader after isolating %s", c.Nodes[victim].name)
				}
			} else {
				switch rnd(3) {
				case 0:
					c.IsolateNode(victim)
				case 1:
					c.Nodes[victim].DropSends(rng.float64prob())
				case 2:
					c.Nodes[victim].DeafToReads(rng.float64prob())
				}
			}
		}
		if i == healAt {
			c.AllHealthyAndPowerOn(rng.pseudoRandBool())
		}
		lastIndex = submitChainBlock(t, c.Nodes[leader], payload)
	}

	// A lagging voter may install a snapshot and skip the intervening apply
	// callbacks. Check its materialized state once it passes the last command.
	for _, node := range c.Nodes {
		checkChainHead(t, node, lastIndex, int64(len(commands)), want[len(commands)])
	}
}

func applyChainBlock(t *testing.T, node *TubeNode, cmd *Ticket, want [][sha256.Size]byte) {
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
	head.Version++
	digest := sha256.Sum256(append(head.Value, cmd.Val...))
	head.Value = digest[:]
	if err := store.PutLeaf(chainTable, head); err != nil {
		t.Errorf("%s storing chain head at log index %d: %v", node.name, cmd.LogIndex, err)
		return
	}
	n := head.Version
	if n < 1 || n >= int64(len(want)) {
		t.Errorf("%s at log index %d applied %d commands, submitted at most %d", node.name, cmd.LogIndex, n, len(want)-1)
	} else if digest != want[n] {
		t.Errorf("%s after %d commands at log index %d: hash %x, want %x", node.name, n, cmd.LogIndex, digest, want[n])
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

// The client submits one byte slice and waits for its acknowledgement.
func submitChainBlock(t *testing.T, node *TubeNode, payload []byte) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := node.NewTicket("chain block", "", "", payload, node.PeerID, node.name, USER_DEFINED_FSM_OP, 0, ctx)
	select {
	case node.writeReqCh <- cmd:
	case <-ctx.Done():
		t.Fatalf("submitting chain block to %s: %v", node.name, ctx.Err())
	}
	select {
	case <-cmd.Done.Chan:
		if cmd.Err != nil || !cmd.Applied || cmd.AsOfLogIndex < 1 {
			t.Fatalf("%s did not apply chain block: applied=%v index=%d err=%v", node.name, cmd.Applied, cmd.AsOfLogIndex, cmd.Err)
		}
		return cmd.AsOfLogIndex
	case <-ctx.Done():
		t.Fatalf("waiting for %s to acknowledge chain block: %v", node.name, ctx.Err())
		return 0
	}
}
