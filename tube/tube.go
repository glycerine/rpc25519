package tube

// Copyright (C) 2025 Jason E. Aten, Ph.D. All rights reserved.
//
// A tube is a small raft, delightfully
// used for floating down rivers on a sunny
// summer's day in some parts of the world.
//
// Tube, this package, gives all of the core
// Raft algorithm in a deliberately small,
// compact form. All the core Raft logic
// is in this file, along with important
// and common optimizations like pre-voting
// and sticky-leader.
//
// Some externally oriented client facing
// APIs are in admin.go. The Raft write ahead log
// is in wal.go. The nodes save their persistent
// state using persistor.go. A simple key-value
// store finite-state-machine is implemented
// whose actions and defined in actions.go.
//
// We communicate log summaries within the cluster using
// a run-length-encoding of the term history
// called a TermsRLE in the rle.go file.
// There are just a handful of files, and
// nearly all of the implementation's behavior
// and optimizations can be readily
// discerned from this central tube.go file.
//
// This makes Tube great for understanding,
// using, and even extending the Raft algorithm.
//
// Log compaction and snapshots are implemented (chapter 5).
//
// The client session system from chapter 6 is
// implemented to preserve linearizability
// (aka linz for short).
//
// Clients can use CreateNewSession to establish
// a server side record of their activity, and thus obtain
// linearizability (exactly once semantics) within
// that session. The client must increment a
// SessionSerial number with each request. If the
// server detects a missing serial number, it informs
// the client by erroring out the Session. If
// the server sees a repeated (duplicate) serial number,
// it returns the cached reply rather than
// re-replicate the operation. See Chapter 6 of
// the Raft dissertation for details.
//
// Membership changes (chapter 4) are
// single-server-at-a-time (SSAAT)
// and utilize the Mongo-Raft-Reconfig algorithm
// from https://arxiv.org/abs/2102.11960
// See also [1][2][3].
//
// We prefer the Mongo-Raft-Reconfig algorithm over the
// dissertation SSAAT algorithm because, in
// addition to being model checked and used
// successfully in for several years in MongoDB operations:
//
// a) it has been formally proven. Neither of the
// dissertation membership change algorithms has been
// formally proven, and thus naturally bugs have
// been found subsequent to their publication[4].
// Despite this known concern, they have still not been
// formally proven.
//
// b) it keeps the MemberConfig (MC) separate from
// the raft log. This greatly simplifies membership
// management, especially when the raft log has to be truncated
// due to leadership change. As a "logless" algorithm
// the membership is kept instead in the persistent
// (state-machine) state which is separate
// from the Raft log.
//
// [1] https://conf.tlapl.us/2024/SiyuanZhou-HowWeDesignedAndModelCheckedMongoDBReconfigurationProtocol.pdf
// [2] https://will62794.github.io/distributed-systems/consensus/2025/08/25/logless-raft.html
// [3] https://github.com/will62794/logless-reconfig
// [4] https://groups.google.com/d/msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
// "bug in single-server membership changes" post by Diego Ongaro
// 2025 Jul 10
//
// ===========================
// On Pre-Voting
// ===========================
//
// Pre-voting gives a more stable cluster.
// The idea is that a singleton node (or any
// minority count of nodes) is never
// able to elect itself leader, by design,
// to prevent the zombie apocolypse.
//
// Just kidding? I am not. Zombie nodes
// are in a minority, cut-off from the rest of the
// cluster by partition (network partially
// goes down/network card fails).
// The have lived in a half-dead state "on the other side of
// the fence" for a while, and so
// in all likelihood have started elections
// trying to find a new leader. With lots
// of failed elections, each one incrementing
// their current term, when zombies
// "come back from the dead" (the parition
// heals/network comes back up), they
// disrupt the majority cluster with their
// local high "current" term number,
// which in core Raft forces delays and
// possible leader churn resulting
// in cluster instability.
//
// While the zombies won't win the election,
// since by Raft rules their logs are too
// far out of date, fighting them off
// takes resources and time away from serving
// client requests (all reads/writes are
// blocked in the meantime, since who should
// be leader is in doubt).
//
// The Pre-Vote mechanism is highly desirable,
// because it prevents all this.
//
// (Apparently Cloudflare endured six hour
// control plane outage in 2020 due to
// a network switch failure because they
// did not run Etcd with the pre-vote
// turned on).
// https://blog.cloudflare.com/a-byzantine-failure-in-the-real-world/
//
// So: the pre-vote mechanism is always
// used here in Tube.
//
// In addition to preventing leader churn,
// PreVote creates a leader read-lease, so
// that the leader can serve reads locally from
// memory without violating linearizability.
//
// This is much faster than waiting to
// hear from a quorum across the network.
// You really want PreVoting for this
// optimization alone. Tube will automatically
// check if all the conditions for serving
// reads locally are met (there are a bunch
// that I'm not going into here, but they
// are not that onerous to meet in normal
// operation), and will do
// so if it can maintain safety. It
// does this automatically, but this optimizatoin
// requires PreVoting to be used on
// all cluster nodes. If the leader has
// heard from a quorum recently, it knows
// it cannot have be deposed before
// another leader election timeout, since any
// other node will lose a pre-vote. The
// trade-off is enduring the same window of
// non-availability on leader crash:
// usually very much worth it.
//
// Protocol Aware Recovery (PAR) is half implemented;
// the wal.go and par.go files have some
// mechanism, but recovery and online
// checks have not been fully completed.
// See too the notes at the end of this file.

// My note on Fig 3.7: If there is a contiguous
// sequence of terms in the leader's log,
// extending back from
// the leaders current term, then it *should*
// I suspect be safe to commit those by
// counting repicas, but might as well
// get that first no-op done anyway.
// (An additional path, on top of the
// "all servers have the entry logic
// for when counting past-term replicas would
// allow them to be committed safely).
// Ongaro: "The way you decide [about
// prior term txn not being commited]
// is, is there any possibility that a server
// could become leader without having that
// entry".
// video lecture "Tech Talk - Raft, In
// Search of an Understandable Consensus
// Algorithm by Diego Ongaro" (38:45)
// -- https://youtu.be/LAqyTyNUYSY?t=2285
//
// "In any 5 server cluster, at least 3
// of them will always be elegible
// [for election]." -- https://youtu.be/LAqyTyNUYSY?t=2504

/* Notes on the required no-op commit before a
leader can do anything:

Q: To fix the bug in the single-node add/remove
protocol, Ongaro recommends having the leader
commit a no-op before it does anything else;
and I think this practice is recommended for
preventing another anomolie (what is it, by the way?).
Can you describe the reasoning and detailed
steps to accomplish this?

A: You're addressing an important safety issue
in Raft's cluster membership changes.
The practice of having a leader commit a no-op
entry at the start of its term addresses
multiple potential issues:

## The Bug and Its Fix

In Ongaro's dissertation, he identified that
when using the single-server membership
change approach (rather than joint consensus),
a specific sequence of events could lead
to committed entries being lost.

### The Problem

The issue arises from this sequence:
1. A cluster has configuration C_old
2. Leader adds a server, creating configuration C_new
3. Leader fails before fully replicating C_new to a majority
4. A new election occurs, and the new server becomes part of the voting quorum
5. The new leader might not have all previously committed entries

### The No-Op Solution

Having each leader commit a no-op entry at the start of its term fixes this by ensuring:

1. The leader must contact a majority of the cluster to commit its no-op
2. This majority must overlap with any previous majority that committed entries
3. Through the log matching property, the leader will discover and preserve any previously committed entries

## Detailed Implementation Steps

1. When a server becomes leader:
   - It immediately creates a log entry containing a no-op operation
   - This entry uses the leader's new term number
   - The leader replicates this entry using normal Raft AppendEntries

2. The leader doesn't process configuration changes or normal client requests until:
   - This no-op entry is committed (replicated to a majority)
   - This ensures the leader's log contains all previously committed entries

3. Only after the no-op is committed does the leader:
   - Begin accepting client requests
   - Process any pending configuration changes

## The Other Anomaly: Read-Only Operations

The no-op commit also helps with another subtle
issue - stale reads. Without this mechanism:

1. A newly elected leader might not know which entries are actually committed
2. If it serves read requests immediately, it might return stale data
3. The no-op commit ensures the leader has the most up-to-date information before serving reads

This is especially important because Raft allows
reads directly from the leader without going
through the log (for performance reasons), but
this optimization requires that the leader
is fully up-to-date.

The no-op approach gives us a clean, safe
starting point for each new leader term,
ensuring continuity of committed data and
preventing anomalies in both configuration
changes and read operations.
*/

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"runtime/debug"
	//"iter"
	//"io"
	//"os"
	//"math"
	//"sort"
	//"sync"
	//cryrand "crypto/rand"
	//"path/filepath"
	//"github.com/glycerine/greenpack/msgp"

	"github.com/glycerine/blake3"
	"github.com/glycerine/idem"
	//rb "github.com/glycerine/rbtree"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/tube/art"
)

//go:generate greenpack

func init() {
	/* try to avoid OOM like this, where we only need 1MB but Go allocates 30 GB per process

	[1085741.373606] Out of memory: Killed process 854413 (member) total-vm:65343076kB, anon-rss:28445568kB, file-rss:936kB, shmem-rss:0kB, UID:1000 pgtables:67976kB oom_score_adj:0
	[1085743.597277] oom_reaper: reaped process 854413 (member), now anon-rss:0kB, file-rss:936kB, shmem-rss:0kB
	[1085775.498923] member invoked oom-killer: gfp_mask=0x140cca(GFP_HIGHUSER_MOVABLE|__GFP_COMP), order=0, oom_score_adj=0
	[1085775.498957]  oom_kill_process+0x118/0x280
	[1085775.498962]  ? oom_evaluate_task+0x155/0x1e0
	[1085775.498968]  __alloc_pages_may_oom+0x10b/0x1d0
	[1085775.499239] [  pid  ]   uid  tgid total_vm      rss rss_anon rss_file rss_shmem pgtables_bytes swapents oom_score_adj name
	[1085775.499259] [   1387]   990  1387     4400     1867      288     1579         0    77824        0          -900 systemd-oomd
	[1085775.499478] oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=user.slice,mems_allowed=0,global_oom,task_memcg=/user.slice/user-1000.slice/session-83.scope,task=member,pid=854415,uid=1000
	[1085775.499595] Out of memory: Killed process 854415 (member) total-vm:1004092396kB, anon-rss:31270716kB, file-rss:296kB, shmem-rss:0kB, UID:1000 pgtables:334784kB oom_score_adj:0
	[1085778.017586] oom_reaper: reaped process 854415 (member), now anon-rss:0kB, file-rss:296kB,shmem-rss:0kB

	*/
	debug.SetMemoryLimit(10 << 30) // 10 GB. Was 2 GB but => grinding at 400% cpu sometimes.
}

// HLC is a hybrid logical/physical clock, based
// on the 2014 paper
//
// "Logical Physical Clocks and Consistent
// Snapshots in Globally Distributed Databases"
// by Sandeep Kulkarni, Murat Demirbas, Deepak
// Madeppa, Bharadwaj Avva, and Marcelo Leone.
//
// Its physical clock resolution (the upper
// 48 bits) is in ~ 0.1 msec or about 100 microseconds.
// The lower 16 bits of this int64
// keep a logical clock counter. The paper's
// experiments observed counter values up to 10,
// nowhere near the 2^16-1 == 65535 maximum.
//
// Indeed one would have to execute one clock
// query every nanosecond for 65536 times in
// a row -- in a single contiguous execution --
// to overflow the counter. This seems unlikely
// with current technology where a PhysicalTime48()
// call takes ~66 nanoseconds and there is rarely
// a requirement for 65K timestamps in a row.
//
// See also: the use of hybrid logical clocks in
// CockroachDB, and Demirbas and Kulkarni's
// paper on using them to solve the consistent
// snapshot problem in Spanner. The naming has evolved;
// in that paper the approach was called "Augmented Time".
// "Beyond TrueTime: Using AugmentedTime for Improving Spanner"
// https://cse.buffalo.edu/~demirbas/publications/augmentedTime.pdf
// https://github.com/AugmentedTimeProject/AugmentedTimeProject
//
// Currently there is no mutual exclusion / synchronization
// provided, and the user must arrange for that separately if
// required.
//
// defined in tube.go now for greenpack serz purposes,
// rather than in hlc.go
type HLC int64

// if we run out out test-coordination channel buffer
// size, our test will just hang, ugh. We try
// to detect and panic instead. Hopefully this
// is large enough... but if you get the
// panics that it is not, simply bump it up.
const TEST_CHAN_CAP = 1_000_000

const RFC3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

const waitForAckTrue = true
const waitForAckFalse = false

// sequentially issue Ticket.Serial for debugging.
var debugNextTSN int64

var debugSerialRaftState int64

type Key string
type Val []byte

var ErrShutDown = fmt.Errorf("error shutdown")
var ErrNotLeader = fmt.Errorf("error: I am not leader")
var ErrRetry = fmt.Errorf("error must retry")

const (
	TUBE_REPLICA string = "tube-replica"
	TUBE_CLIENT  string = "tube-client"

	// observe membership changes only. get pushes on changes.
	TUBE_OBS_MEMBERS string = "tube-obs-members"

	// Update: use TUBE_REPLICA for non-voting shadows
	// but have them marked differently.
	// shadow replicas observe all traffic but are non-voting,
	// so they ignore votes/pre-votes.
	// ack back on AE so as to stay current.
	// TUBE_SHADOW string = "tube-shadow"
)

type RaftRole int

const (
	// includes ShadowReplicas (always followers, they
	// never vote and never hold elections).
	FOLLOWER RaftRole = 1

	CANDIDATE RaftRole = 2
	LEADER    RaftRole = 3

	// anyone else just requesting info should
	// come in as a CLIENT.
	CLIENT RaftRole = 4
)

var alias = rpc.AliasDecode

func (role RaftRole) String() string {
	switch role {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	case CLIENT:
		return "CLIENT"
	}
	return "(no role yet)"
}

// RPC call/response FragOp in Tube
const (
	AppendEntriesMsg               int = 1
	AppendEntriesAckMsg            int = 2
	RequestVoteMsg                 int = 3
	VoteMsg                        int = 4
	RedirectTicketToLeaderMsg      int = 5
	LeaderToClientTicketAppliedMsg int = 6
	RequestPreVoteMsg              int = 7
	PreVoteMsg                     int = 8
	TellClientSteppedDown          int = 9
	PeerListReq                    int = 10
	PeerListReply                  int = 11
	ObserveMembershipChange        int = 12
	CircuitSetupHasBaseServerAddr  int = 13
	RequestStateSnapshot           int = 14
	StateSnapshotEnclosed          int = 15
	InstallEmptyMC                 int = 16
	NotifyClientNewLeader          int = 17
	ReliableMemberHeartBeatToCzar  int = 18
	PruneRedundantCircuit          int = 19
	// PAR means the leader must get log
	// information from followers, but actually
	// an empty AE and then AEack should suffice
	// as we have a fairly complete description,
	// with the run-length-encoding terms from
	// the log already.
)

func msgop(o int) string {
	switch o {
	case AppendEntriesMsg: // 1
		return "AppendEntries"
	case AppendEntriesAckMsg: // 2
		return "AppendEntriesAck"
	case RequestVoteMsg: // 3
		return "RequestVote"
	case VoteMsg: // 4
		return "Vote"
	case RedirectTicketToLeaderMsg: // 5
		return "RedirectTicketToLeader"
	case LeaderToClientTicketAppliedMsg: // 6
		return "LeaderToClientTicketApplied"
	case RequestPreVoteMsg: // 7
		return "RequestPreVoteMsg"
	case PreVoteMsg: // 8
		return "PreVoteMsg"
	case TellClientSteppedDown: // 9
		return "TellClientSteppedDown"
	case PeerListReq: // 10
		return "PeerListReq"
	case PeerListReply: // 11
		return "PeerListReply"
	case ObserveMembershipChange: // 12
		return "ObserveMembershipChange"

	case CircuitSetupHasBaseServerAddr: // 13
		return "CircuitSetupHasBaseServerAddr"
	case RequestStateSnapshot: // 14
		return "RequestStateSnapshot"
	case StateSnapshotEnclosed: // 15
		return "StateSnapshotEnclosed"
	case InstallEmptyMC: // 16
		return "InstallEmptyMC"
	case NotifyClientNewLeader: // 17
		return "NotifyClientNewLeader"
	case ReliableMemberHeartBeatToCzar: // 18
		return "ReliableMemberHeartBeatToCzar"
	case PruneRedundantCircuit: // 19
		return "PruneRedundantCircuit"
	}
	return fmt.Sprintf("unknown MsgOp: %v", o)
}

type TubeConfig struct {
	ConfigName string `zid:"0"`

	ClusterID string `zid:"1"`

	// where to read our config/log from.
	ConfigDir string `zid:"2"`

	// root dir of where to store the write-ahead RaftLog(s)
	DataDir string `zid:"3"`

	// NoFaultTolDur is used
	// to detect faulty configuration/setups.
	// It forces all nodes
	// must participate in quorum for
	// this much time after startup. Otherwise
	// the fault-tolerance of the algorithm can
	// mask manual configuration errors.
	// It is better to catch these early,
	// before seemless rolling into fault
	// tolerant mode after this much time.
	NoFaultTolDur time.Duration `zid:"4"`

	// Memory-only operation, to avoid wear and tear on SSD.
	// Just for testing. This is not safe (it will
	// trash your data, steal your money, run off
	// with your spouse, and drag you behind the horses
	// like a good old fashioned western...) and will give
	// safety violations if used in the real world.
	// YOU HAVE BEEN WARNED. TESTING ONLY. MUST BE false IN PROD.
	NoDisk bool `zid:"5"`

	// skip encryption? (used to simplify and speed up tests)
	TCPonly_no_TLS bool `zid:"6"`

	// HeartbeatDur gives the time between
	// leader sending heartbeats (empty AppendEntries)
	// messages to each followers, when no
	// other traffic has been received from them.
	// It must be 10x smaller than the ElectionDur.
	// The system check this at startup, and
	// will panic if this is not the case.
	// Defaults to 50 msec if unset.
	HeartbeatDur time.Duration `zid:"7"`

	// MinElectionDur is the minimum time T
	// before a new election the Raft
	// algorithm. Nodes will call for a
	// pre-vote (if PreVote is on) or vote
	// after a random timeout in [T, 2*T],
	// if they have not heard at least
	// a heartbeat from the current leader.
	// MinElectionDur must be at least 10x
	// the HeartbeatDur. The system will
	// check this at startup, and will
	// panic if this is not the case.
	// Default: 10 * the HeartbeatDur.
	// Hence if both HeartbeatDur and MinElectionDur
	// are unset, they will default to a
	// HeartbeatDur of 90 msec,
	// and an MinElectionDur of 1000 msec.
	MinElectionDur time.Duration `zid:"8"`

	// Cluster size is the number of
	// replica nodes in the cluster.
	// This is used to compute quorum.
	// It includes the leader and all followers.
	ClusterSize int `zid:"9"`

	// for tests: simulated network
	UseSimNet bool `zid:"10"` //

	SimnetGOMAXPROCS int `zid:"11"`

	ClockDriftBound time.Duration `zid:"12"` // time.Millisecond * 400 // even less chance of granting vote...

	// tuber --boot flag will set this to non-nil
	// get a fresh new cluster started.
	// NB: should have BootCount > 0.
	// Should be nil if not the designated leader
	// on new cluster start.

	// I think InitialLeaderName and ClusterSize is
	// sufficient, simpler, and we won't have any
	// PeerIDs until they are start up anyway, and
	// we really just need to know if "we" are the
	// the designated initial leader anyway.
	// If we are (i.e. our TubeNode.name matches
	// TubeNode.cfg.InitialLeaderName), then we can check our log, and
	// if it is empty we can bootstrap it up.
	InitialLeaderName string `zid:"13"`

	MyName string `zid:"14"`

	// "tube-replica" or "tube-client"; what
	// to register with the
	// peer/circuit/fragment framework as.
	PeerServiceName string `zid:"15"`

	// NodeAddr holds all initial nodes.
	// keys: (short) names, including MyName.
	// values: host:port to reach them on.
	Node2Addr map[string]string `zid:"16"`

	NoBackgroundConnect bool `zid:"17"`

	// for testing and evaluation, log compaction
	// can be shut off here. Logs will grow
	// without bound if NoLogCompaction is true.
	NoLogCompaction bool `zid:"18"`

	// the 'tube -zap' command line option to
	// clear the state.MC on startup.
	// It is unlikely that you will want
	// to actually use ZapMC in a config
	// file as a permanent or regular
	// thing -- it is meant for a rare rescue
	// situation to stand up again a
	// cluster that has gotten wedged because
	// it lost quorum. The 'tube -zap' command line
	// passes it ephemerally to the tube
	// process on startup using this setting.
	ZapMC bool `zid:"19"`

	// AccumateBatchDur says how long we wait
	// before putting a write through the consensus
	// pipeline, in order to try and accumulate
	// a batch of writes and amortize our fsync
	// and network roundtrip costs. The data limits
	// of rpc.UserMaxPayload will also, most
	// likely, need to apply as well.
	BatchAccumulateDur time.Duration `zid:"20"`

	TupDefaultTable string `zid:"21"`

	// for internal failure recovery testing,
	// e.g. to drop or ignore messages.
	// The int key hould correspond to the test number,
	// and the string value describes the condition.
	testScenario map[string]bool

	// testing mechanics, see linz_test.go
	deaf2a map[string]bool
	deaf2b map[string]bool

	// must be shared by all notes for simnet to work.
	// Init will set it up. NOT (greenpack) SERIALIZED at the moment.
	RpcCfg     *rpc.Config `msg:"-"`
	initCalled bool        // verify everyone is using cfg.Init() now.

	// for tests, of leader election, using LeaderElectedCh
	// for example. All test special behavior should
	// centralize through this one testCluster as much as possible.
	//
	// testCluster should be nil when not under test; in production.
	testCluster *TubeCluster
	isTest      bool // for 802 simae_test too
	testNum     int

	deadOnStartCount int // test 024 need 2 nodes, only have 1 started.
	// end TubeConfig
}

type cktAndError struct {
	ckt     *rpc.Circuit
	fragerr *rpc.Fragment
}

func (s *TubeNode) newMCfromAddr2Node() (mc *MemberConfig) {

	mc = s.NewMemberConfig("newMCfromAddr2Node")

	// include self
	detail := &PeerDetail{
		Name:                   s.name,
		URL:                    s.URL,
		PeerID:                 s.PeerID,
		Addr:                   s.MyPeer.BaseServerAddr,
		PeerServiceName:        s.MyPeer.PeerServiceName,
		PeerServiceNameVersion: s.MyPeer.PeerServiceNameVersion,
	}
	mc.setNameDetail(s.name, detail, s)

	for name, addr := range s.cfg.Node2Addr {
		// why don't we want ourselves in NewConfig.PeerName?
		// because we just added ourselves above.
		if name == "" {
			panic(fmt.Sprintf("cannot have empty cfg.Node2Addr names (keys): cfg='%v", s.cfg))
		}
		addr = FixAddrPrefix(addr)
		if name != s.name {
			_, ok := mc.PeerNames.Get2(name)
			if ok {
				panic(fmt.Sprintf("%v: double entry for peerName='%v' in the cfg Node2Addr: '%#v'", s.name, name, s.cfg.Node2Addr))
			}
			if addr == "" {
				addr = "boot.blank"
				alwaysPrintf("%v warning: s.cfg.Node2Addr had no address for name='%v'; using boot.blank", s.name, name)
				panic(fmt.Sprintf("%v: no empty addresses allowed. fix the config for name='%v'", s.name, name))
			}
			detail2 := &PeerDetail{
				Name: name,
				URL:  addr,
				Addr: addr,
			}
			mc.setNameDetail(name, detail2, s)
		}
	}
	return
}

func (s *TubeNode) Start(
	myPeer *rpc.LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *rpc.Circuit,

) (err0 error) {

	s.Ctx = myPeer.Ctx
	//vv("%v (PeerID: '%v') top of TubeNode.Start() ", s.name, myPeer.PeerID)
	//vv("%v (PeerID: '%v') top of TubeNode.Start() ; cfg = '%v'", s.name, myPeer.PeerID, s.cfg.ShortSexpString(nil))
	//cktHasBeenQuietTooLong := make(chan *rpc.Circuit)
	cktHasDied := make(chan *rpc.Circuit)
	cktHasError := make(chan *cktAndError)

	defer func() {
		r := recover()
		//vv("%v: (%v) end of TubeNode.Start() inside defer, about to return/finish; recover='%v'", s.me(), myPeer.ServiceName(), r)

		var reason error
		if r != nil {
			reason = fmt.Errorf("reason is panic on: '%v'", r)
		}
		s.MyPeer.Close()
		if s.cfg.UseSimNet {
			simnet := s.cfg.RpcCfg.GetSimnet()
			if simnet != nil {
				// CloseSimnode does SHUTDOWN automatically beforehand.
				//_, err := simnet.AlterHost(s.srvname, rpc.SHUTDOWN)
				//panicOn(err)
				err := simnet.CloseSimnode(s.srvname, reason)
				if err != nil {
					// can have problems finding... don't freak out.
					//panicOn(err)
				}
				//vv("good: simnet.AlterHost(s.srvname, rpc.SHUTDOWN)")
			}
		}
		// for _, cktP := range s.cktall {
		// 	if cktP.ckt != nil { // could be pending
		// 		cktP.ckt.Close(nil)
		// 		//vv("%v ckt closed: '%v'", s.name, cktP.ckt)
		// 	}
		// }

		s.Halt.Done.Close()
		if r != nil {
			panic(r)
		}
	}()

	//s.Srv.PeerAPI.StartLocalPeer() at :2112 sets the PeerName already.
	if s.name == "" {
		panic("must have s.name not empty")
	}
	if s.cfg.MyName != "" && s.cfg.MyName != s.name {
		panic(fmt.Sprintf("Arg. disagreement on our own name: s.cfg.MyName = '%v' but s.name = '%v'", s.cfg.MyName, s.name))
	}
	s.aliasSetup(myPeer)
	s.MyPeer = myPeer
	s.myid = rpc.AliasDecode(myPeer.PeerID)
	s.URL = myPeer.URL()
	s.PeerID = myPeer.PeerID

	//vv("%v: node.Start() top. myPeer.PeerID = '%v'", s.name, myPeer.PeerID)

	// If there is no prior state on disk, we
	// want these to be sanely set anyway.
	// (If there is a disk state, it will
	// overwrite them but still, we like
	// have the first state saved to
	// disk be informative.)
	s.state.PeerName = s.name
	s.state.PeerServiceName = s.PeerServiceName
	s.state.PeerServiceNameVersion = s.PeerServiceNameVersion
	s.state.PeerID = s.PeerID
	s.state.ClusterID = s.ClusterID

	//could add TubeNode.HomeDir string `zid:"1"`
	//s.HomeDir = filepath.Join(s.cfg.DataDir, "clusterID_"+cfg.ClusterID, s.name, "peerID_"+s.PeerID)
	//err = os.MkdirAll(homeDir)
	//panicOn(err)

	//vv("%v TubeNode.Start() started with url = '%v'; s.PeerID = '%v'; peerServiceName='%v'", s.name, s.URL, rpc.AliasDecode(s.PeerID), myPeer.ServiceName())

	now := time.Now()
	s.t0 = now
	switch {
	case s.cfg.NoFaultTolDur <= 0:
		s.cfg.NoFaultTolDur = 0
		s.noFaultTolExpiresTm = now
		s.haveCheckedAdvanceOnceAfterNoFaultExpired = true
	default:
		s.noFaultTolExpiresTm = now.Add(s.cfg.NoFaultTolDur)
		//vv("%v set s.noFaultTolExpiresTm = %v (in %v)", s.name, nice(s.noFaultTolExpiresTm), s.cfg.NoFaultTolDur)
	}

	// 3.4 "When servers start up, they begin as followers." p16
	// Set this before making method calls that might debug log,
	// to avoid seeing spurious "no role yet" in the logs.
	s.role = FOLLOWER
	//vv("%v TubeNode.Start(): set role to FOLLOWER")

	if s.cfg.PeerServiceName == TUBE_CLIENT {
		//vv("%v setting role to CLIENT b/c PeerServiceName is TUBE_CLIENT", s.me())
		s.role = CLIENT
	}

	// allow test setups like to have
	// given us a wal or memwal especailly;
	// in testSetupFirstRaftLogEntryBootstrapLog().
	err := s.initWalOnce()
	if err != nil {
		return
	}

	if false && s.isTest() {
		err = s.viewCurLog()
		panicOn(err)
		if err != nil {
			return
		}
	}

	// checks insaneConfig so even tests should always be sanely configured.
	// loads s.state.MC from disk if available.
	err = s.onRestartRecoverPersistentRaftStateFromDisk()
	panicOn(err)
	if err != nil {
		return
	}

	// make sure our wal and state are sycned at load time;
	// if we crashed before the state could be updated to match
	// the wal, for instance.
	s.state.CompactionDiscardedLast.Index = s.wal.logIndex.BaseC
	s.state.CompactionDiscardedLast.Term = s.wal.logIndex.CompactTerm
	s.assertCompactOK() // previously, out of sync even here: s.state.CompactionDiscardedLast.Index(1088) != s.wal.logIndex.BaseC(1187)

	if s.cfg.ZapMC {
		alwaysPrintf("%v -zap of MC requested, about to clear restored MC which is currently: '%v' and will clear this set of ShadowReplicas: '%v'", s.me(), s.state.MC, s.state.ShadowReplicas)
		s.state.MC = s.NewMemberConfig("ZapMC")
		s.state.ShadowReplicas = s.NewMemberConfig("ZapMC")
		s.saver.save(s.state)
	}
	//recoveredDiskMC := s.state.MC

	if !strings.HasPrefix(s.name, "tup") {
		// for tup we expect nil MC, do not bark.
		//alwaysPrintf("%v on start up, after onRestartRecoverPersistentRaftStateFromDisk(), our s.state.MC = %v", s.name, s.state.MC.Short())
	}

	if s.cachedMinElectionTimeoutDur == 0 {
		panic("why? cachedMinElectionTimeoutDur == 0")
	}

	// when we reboot, we should update our own address
	// in the config we recover from the log. So if we
	// become leader, we will broadcast our own location
	// correctly.
	if s.state.MC != nil {
		// modified s.state.MC if changed
		changed := s.updateSelfAddressInMemberConfig(s.state.MC)
		if changed {
			// ARG! incrementing ConfigVersion here has a very bad side effect:
			// by incrementing the version of our MC, we can no
			// longer participate in elections/elect a leader
			// among ourselves!?! since we are not _actually_
			// changing the membership config, just the connection details,
			// do NOT increment the version! (here in Start() at any rate;
			// tube.go: line 12188 in TubeNode.changeMembership() does increment it).
			//s.state.MC.ConfigVersion++

			//s.state.MC.IsCommitted = false
			//if s.state.MC.PeerNames.Len() == 1 {
			//	// it is just us (changed true means we are in it)
			//	s.state.MC.IsCommitted = true
			//}
		}
	}
	// used to do...
	//s.adjustCktReplicaForNewMembership()

	//vv("%v after recovery of persistent raft state in Start: s.state.LastApplied = '%v'; s.state.MC = '%v'", s.name, s.state.LastApplied, s.state.MC)

	// bootstrapping?
	// if nil then bootstraping AND we are not the
	// manually designated first leader; don't
	// run election timers
	//noClusterMembersFromDisk := (s.state.MC == nil)

	// "If a follower receives no communication over a period of time
	// called the election timeout, then it assumes there is
	// no viable leader, and begins an election to choose a
	// new leader."

	// Was under isTest, but InitAndStart seems
	// to always want it even if not under test.
	//
	// Tell cluster it is okay to read our URL and
	// PeerID without race now.
	if s.startupNodeUrlSafeCh != nil {
		s.startupNodeUrlSafeCh.Close()
	}

	var iAmReplicaInCurrentMC bool
	// this could be very old, of course.
	//iAmDesignatedLeader := (s.name == s.cfg.InitialLeaderName)

	// do we need to initialize a bootstrap first
	// log entry? for prod only.
	if !s.isTest() {
		// in prod...

		if s.state.MC != nil {
			_, iAmReplicaInCurrentMC = s.state.MC.PeerNames.Get2(s.name)

			// allow starting as client after being in the log before.
			if iAmReplicaInCurrentMC && s.PeerServiceName != TUBE_REPLICA {
				alwaysPrintf("%v: we are no longer a replica; (now: %v). deleting ourselves from the MC.PeerNames", s.name, s.PeerServiceName)
				iAmReplicaInCurrentMC = false
				s.state.MC.PeerNames.Delkey(s.name)
			}
		}

		//vv("%v iAmReplicaInCurrentMC = %v; iAmDesignatedLeader = %v", s.me(), iAmReplicaInCurrentMC, iAmDesignatedLeader)

	} // end if prod

	// should we start our election timer now?
	// If under test: allow tests to pause all
	// nodes and then start them
	// at about the same moment. Or, one by one.
	if s.isTest() {
		// === test path ===

		if s.state.MC == nil {
			//vv("%v: under test. s.state.MC==nil; thus no resetElectionTimeout; s.clusterSize()=%v; len(s.testBootstrapLogCh) = %v", s.me(), s.clusterSize(), len(s.testBootstrapLogCh)) // not seen 065
		} else if s.cfg.testCluster.NoInitialLeaderTimeout {
			// allow test to force a specific node to begin elections first
			//vv("%v NoInitialLeaderTimeout true: no resetElectionTimeout", s.me())
		} else {
			//vv("%v calling resetElectionTimeout", s.me())
			s.resetElectionTimeout("top of main loop, under test")
		}
	} else {
		// === production path ===

		if s.state.MC == nil {
			s.state.MC = s.NewMemberConfig("Start")
		}

		// we have to be sure election timeout is running.

		if s.PeerServiceName == TUBE_REPLICA {
			s.resetElectionTimeout("prod top of main loop")

			s.candidateReinitFollowerInfo() // init s.peers map.
			s.connectToMC("prod replica top of main loop")

			// on a previous shutdown sequence, we could
			// have been left last in Observers; clear ourselves from that.
			s.clearFromObservers(s.name)

		} else {
			// do not start election timeouts for
			// clients like tup and tubels, tuberm, etc.
		}
	}

	// if we still have boot.blank for our own
	// info, remedy that! so if we are leader then
	// fail, we will have propagated our actual
	// PeerDetails contact info in the MC
	// that goes out with each AE heartbeat.
	//
	// Note that s.state.MC will be
	// nil for fresh cluster followers, so check first.
	if s.state.MC != nil {
		detail, present := s.state.MC.PeerNames.Get2(s.name)
		_ = detail
		updatedDetail := &PeerDetail{
			Name:                   s.name,
			URL:                    s.URL,
			PeerID:                 s.PeerID,
			PeerServiceName:        s.PeerServiceName,
			PeerServiceNameVersion: s.PeerServiceNameVersion,
			Addr:                   myPeer.NetAddr,
		}
		if s.MyPeer.BaseServerAddr != "" {
			updatedDetail.Addr = s.MyPeer.BaseServerAddr
		}
		if present {
			s.state.MC.setNameDetail(s.name, updatedDetail, s)
		}
		s.state.Known.PeerNames.Set(s.name, updatedDetail)
	} else {
		if !strings.HasPrefix(s.name, "tup") {
			// for tup we expect it. do not bark.
			//alwaysPrintf("%v: we have nil s.state.MC here, ugh.", s.name)
		}
	}

	// try getting 059/402/403 to work without needing
	// to stall on leader coming in... nope. did not help; or
	// just not fast enough.
	if s.cfg.isTest && s.cfg.testNum != 802 {
		// just calls s.adjustCktReplicaForNewMembership() if viable.
		//vv("%v starting circuits to MC = '%v'", s.name, s.state.MC)
		s.connectToMC("test, top of main loop")
	}

	done0 := ctx0.Done()

	// centralize all peer incomming messages here,
	// to avoid locking in a million places.
	// (sent on in <-newCircuitCh handling below).
	arrivingNetworkFrag := make(chan *fragCkt)

	//vv("%v about to enter for/select loop. nextElection timeout '%v'; MC=%v ; cfg.Node2Addr = '%v'", s.me(), time.Until(s.nextElection), s.state.MC, s.cfg.Node2Addr)

	var loopPrevBeg, loopCurBeg time.Time
	for i := 0; ; i++ {

		// detect slow loop operations.
		// no, this is just measing time between events!
		// _not_ how long processing took!
		if false {
			loopPrevBeg = loopCurBeg
			loopCurBeg = time.Now()
			if i > 0 {
				elap := loopCurBeg.Sub(loopPrevBeg)
				if elap > 100*time.Millisecond {
					panic(fmt.Sprintf("%v slow loop took %v > 100ms, why i = %v??", s.me(), elap, i))
				}
			}
		}

		if false && !s.cfg.isTest && i%50 == 0 {
			// monitor liveness of prod processes
			if s.PeerServiceName == TUBE_REPLICA &&
				!strings.HasPrefix(s.name, "tup_") &&
				!strings.HasPrefix(s.name, "tubels_") &&
				!strings.HasPrefix(s.name, "tuberm_") &&
				!strings.HasPrefix(s.name, "tubeadd_") {
				alwaysPrintf("%v: top of select; i = %v", s.me(), i)
			}
		}

		// we should always have a timer going... or if leader,
		// have sent a heartbeat.
		now = time.Now()
		shouldHaveElectTO := now.Add(-s.maxElectionTimeoutDur())

		down := s.Halt.ReqStop.IsClosed()

		livelyLeader := s.role == LEADER && now.Before(s.leaderSendsHeartbeatsDue) &&
			s.leaderSendsHeartbeatsCh != nil

		if false && i > 5 && !down && !livelyLeader &&
			s.preVoteOkLeaderElecTimeoutCh == nil &&
			(s.electionTimeoutCh == nil || s.nextElection.Before(shouldHaveElectTO)) {
			panic(fmt.Sprintf(`i=%v; arg! can't wake up?
%v
 
s.preVoteOkLeaderElecTimeoutCh=%p
s.electionTimeoutCh=%p
s.lastElectionTimeOut=%v
s.lastElectionTimeCount=%v
s.nextElection='%v' < shouldHaveElectTO '%v'`,
				i,
				s.preVoteDiagString(),
				s.preVoteOkLeaderElecTimeoutCh,
				s.electionTimeoutCh,
				nice(s.lastElectionTimeOut),
				s.lastElectionTimeCount,
				nice(s.nextElection), shouldHaveElectTO))
		} // end debug no waking up.

		// must give WAL bootstrap first priority,
		// because this can establish one node as
		// having a known cluster membership (itself),
		// immediately, as so able to elect itself leader
		// and then take other joins.
		// Arg: We would like the noop0 to be first
		// in the log. But being able to discern
		// membership is kind of a pre-requisite to
		// being able to commit, well, anything.
		// So: decide we must allow a membership
		// change entry as the first thing in the
		// wal/raft log.

		select {
		case <-s.batchSubmitTimeCh:
			//vv("%v <-s.batchSubmitTimeCh fired", s.name)
			needSave, didSave := s.replicateBatch()
			if !didSave && needSave {
				s.saver.save(s.state)
			}

		case boot := <-s.testBootstrapLogCh: // 020, 040 election_test
			//vv("%v s.testBootstrapLogCh fired", s.me())
			err = s.setupFirstRaftLogEntryBootstrapLog(boot)
			//vv("%v <-s.testBootstrapLogCh: s.setupFirstRaftLogEntryBootstrapLog() done", s.me())
			if err != nil {
				return err
			}
			if boot.DoElection {
				s.resetElectionTimeout("test scenario, boot := <-s.testBootstrapLogCh; boot.DoElection true.")
			}
			s.startCircuitsToLatestMC() // added for 040 green again.
		default:
		}

		//vv("%v about wait at Start() select point; s.electionTimeoutCh=%p; s.nextElection in '%v'", s.name, s.electionTimeoutCh, time.Until(s.nextElection))
		select {

		case <-s.leaderSendsHeartbeatsCh:
			//if s.cfg.testNum != 52 && s.cfg.testNum != 51 {
			//	vv("%v <-s.leaderSendsHeartbeatsCh", s.name)
			//}

			s.countLeaderHeartbeat++
			if s.countLeaderHeartbeat%500 == 0 {
				//if s.cfg.testNum != 52 && s.cfg.testNum != 51 {
				//vv("%v <-s.leaderSendsHeartbeatsCh fired, term=%v: %v ; followers='%v'", s.me(), s.state.CurrentTerm, s.countLeaderHeartbeat, s.followers())
				//}
			}
			s.leaderSendsHeartbeats(false)
			continue

		case ckt := <-s.setLeaderCktChan:
			// clients only!
			if s.PeerServiceName == TUBE_REPLICA {
				panic("setLeaderCktChan is only for clients, not replica!")
			}
			//vv("%v <-s.setLeaderCktChan so setting s.leaderID='%v' and s.leaderName='%v'", s.name, s.leaderID, s.leaderName)
			s.leaderID = ckt.RemotePeerID
			s.leaderName = ckt.RemotePeerName
			s.leaderURL = ckt.RemoteServerURL("")

		case <-s.Halt.ReqStop.Chan:
			//s.ay("%v shutdown initiated, s.Halt.ReqStop seen", s.me())
			//vv("%v shutdown initiated, s.Halt.ReqStop seen", s.me())
			s.shutdown()
			return rpc.ErrHaltRequested
			//return ErrShutDown
		case <-s.testBeginElection:
			//s.ay("%v s.testBeginElection fired", s.me())
			//vv("%v s.testBeginElection fired", s.me())
			s.beginElection()
		case <-s.testBeginPreVote:
			//s.ay("%v s.testBeginPreVote fired", s.me())
			s.beginPreVote()

		case gonePeerID := <-s.peerLeftCh:
			//s.ay("%v gonePeerID", s.me())
			//vv("%v <-s.peerLeftCh, cktall gonePeerID: '%v'", s.name, alias(gonePeerID)) // not seen 031
			cktP, ok := s.cktall[gonePeerID]
			if ok {
				//vv("%v <-s.peerLeftCh, cktall gone name: '%v'", s.name, cktP.PeerName)
				s.deleteFromCktAll(cktP)
				if cktP.ckt != nil {
					cktP.ckt.Close(nil)
				}
			} else {
				delete(s.peers, gonePeerID)
			}
		case ins := <-s.requestInpsect:
			//s.ay("%v requestInspect", s.me())
			s.inspectHandler(ins)
			continue

		case <-s.nextWakeCh:
			//s.ay("%v nextWakeCh", s.me())
			s.countWakeCh++
			//vv("=================== <-s.nextWakeCh %v fired ================", s.countWakeCh)
			//s.checkCoordOrFollowerFailed()

		case tkt := <-s.writeReqCh:
			// WRITE, DELETE_TABLE, MAKE_TABLE, RENAME_TABLE,
			// CAS, ADD_SHADOW_NON_VOTING, REMOVE_SHADOW_NON_VOTING,
			// are all submitted here.
			s.countWriteCh++
			//s.ay("%v got ticket on %v <-s.writeReqCh: '%v'", s.me(), s.countWriteCh, tkt)
			//vv("%v got ticket on %v <-s.writeReqCh: '%v'", s.me(), s.countWriteCh, tkt)
			tkt.Stage += ":writeReqCh"
			//tkt.localHistIndex = len(s.tkthist)
			//s.tkthist = append(s.tkthist, tkt)
			//s.tkthistQ.add(tkt)

			if s.redirectToLeader(tkt) {
				if tkt.Err != nil {
					continue // bail out, error happened.
				}
				//vv("%v writeReq redirected to leader, adding to Waiting, writeReqCh: '%v'", s.me(), tkt)
				s.WaitingAtFollow.set(tkt.TicketID, tkt)
				tkt.Stage += ":writeReqCh_add_WaitingAtFollow"
				continue // follower does not replicate tickets.
			}
			// notice the continue just above means we are on the leader here.
			tkt.Stage += ":writeReqCh_replicateTicket"
			//s.replicateTicket(tkt)
			s.commandSpecificLocalActionsThenReplicateTicket(tkt, "<-s.writeReqCh")

		case tkt := <-s.newSessionRequestCh:
			s.handleNewSessionRequestTicket(tkt)

		case tkt := <-s.closeSessionRequestCh:
			s.handleCloseSessionRequestTicket(tkt)

		case disco := <-s.discoRequestCh:
			s.handleLocalCircuitDisconnectRequest(disco)

		case tkt := <-s.readReqCh:
			// SHOW_KEYS, READ_KEYRANGE, READ_PREFIX_RANGE, and READ use readReqCh.

			s.countReadCh++
			//vv("%v got ticket on %v <-s.readReqCh: '%v'", s.me(), s.countReadCh, tkt)
			tkt.Stage += ":readReqCh"
			//tkt.localHistIndex = len(s.tkthist)
			//s.tkthistQ.add(tkt)
			//s.tkthist = append(s.tkthist, tkt)

			if s.redirectToLeader(tkt) {
				if tkt.Err != nil {
					//vv("%v arg, tried to redirectToLeader but tkt.Err='%v'", s.me(), tkt.Err)
					continue // bail out, error happened.
				}
				//vv("%v adding to Waiting, s.leaderName='%v'; readReqCh saw tkt: '%v'", s.me(), s.leaderName, tkt)
				tkt.Stage += ":after_redirectToLeader_add_WaitingAtFollow"

				prior, already := s.WaitingAtFollow.get2(tkt.TicketID)
				if already {
					panic(fmt.Sprintf("WAF already has prior='%v'; versus tkt='%v'", prior, tkt))
				}
				s.WaitingAtFollow.set(tkt.TicketID, tkt)
				//vv("%v AFTER adding to WAF, at readReqCh: tkt='%v'", s.me(), tkt)
				continue
			}
			if !s.leaderServedLocalRead(tkt, false) {
				tkt.Stage += ":readReqCh_leaderServedLocalRead_false_calling_replicateTicket"
				s.replicateTicket(tkt)
			} else {
				tkt.Stage += ":readReqCh_leaderServedLocalRead_true"
			}
			//vv("%v what here? leader should have served local read", s.me())

		case tkt := <-s.deleteKeyReqCh:
			s.countDeleteKeyCh++
			//s.ay("%v got ticket on %v <-s.deleteKeyReqCh: '%v'", s.me(), s.countDeleteKeyCh, tkt)
			tkt.Stage += ":deleteKeyReqCh"
			//tkt.localHistIndex = len(s.tkthist)
			//s.tkthistQ.add(tkt)
			//s.tkthist = append(s.tkthist, tkt)

			if s.redirectToLeader(tkt) {
				if tkt.Err != nil {
					continue // bail out, error happened.
				}

				//vv("%v adding to Waiting, deleteKeyReqCh: '%v'", s.me(), tkt)
				tkt.Stage += ":after_redirectToLeader_add_WaitingAtFollow"

				prior, already := s.WaitingAtFollow.get2(tkt.TicketID)
				if already {
					panic(fmt.Sprintf("WAF already has prior='%v'; versus tkt='%v'", prior, tkt))
				}
				s.WaitingAtFollow.set(tkt.TicketID, tkt)
				//vv("%v AFTER adding to WAF, at deleteKeyReqCh: tkt='%v'", s.me(), tkt)
				continue
			}
			tkt.Stage += ":deleteKeyReqCh_on_leader_calling_replicateTicket"
			s.replicateTicket(tkt)

		case <-s.preVoteOkLeaderElecTimeoutCh:
			//vv("%v preVoteOkLeaderElecTimeoutCh fired s.preVoteTerm(%v); s.state.CurrentTerm(%v)", s.me(), s.preVoteTerm, s.state.CurrentTerm)

			// verify we are in phase 2 and this isn't a spurious or
			// racy wake up; also that the term we won the
			// pre-vote for is still viable to stand for.
			if s.role != FOLLOWER {
				// already a leader or candidate, probably
				// just a racy alarm. ignore.
				continue
			}
			if s.preVoteTerm > 0 {
				//vv("%v preVoteOkLeaderElecTimeoutCh in another phase one, can't do phase 2 yet.", s.me())
				continue
			}
			if s.preVoteOkLeaderElecDeadline.IsZero() {
				//vv("%v preVoteOkLeaderElecTimeoutCh not in phase two, hmm..", s.me())
				continue
			}
			if s.preVoteOkLeaderElecTerm != s.state.CurrentTerm+1 {
				//vv("%v preVoteOkLeaderElecTimeoutCh s.preVoteOkLeaderElecTerm(%v)"+
				//	" != s.state.CurrentTerm+1(%v): no longer the pre-vote "+
				//	"we won. probably had another leader come in.", s.me(),
				//	s.preVoteOkLeaderElecTerm, s.state.CurrentTerm+1)
				continue
			}

			//vv("%v pre-vote phase 2 complete for term %v; call beginElection", s.me(), s.preVoteOkLeaderElecTerm)

			s.preVotePhase2EndedAt = now
			s.beginElection()
			continue

		case <-s.electionTimeoutCh:
			//s.ay("%v electionTimeoutCh", s.me())
			if !s.cfg.isTest {
				//vv("%v electionTimeoutCh", s.me())
			}
			// so it is just never getting initialized(!)
			s.countElections++
			s.lastElectionTimeOut = time.Now()
			s.lastElectionTimeCount = s.countElections

			s.errorOutAwaitingLeaderTooLongTickets()

			if s.observerOnlyNow() {
				//vv("%v observerOnlyNow so exiting", s.me())
				// try to get a single node cluster to shut itself

				// down when removed from membership.
				return
			}

			// we were dropping these somewhere and thus
			// not electing a new leader after a leader
			// was removed. Be sure we always have one running now.
			s.resetElectionTimeout("top <-s.electionTimeoutCh: always have a leader election timout going.")

			// if we just woke from a pause like 055,
			// or a reboot... active connect to
			// our members.
			//vv("%v <-s.electionTimeoutCh about to call s.connectToMC()", s.me())
			if s.role != LEADER && s.leaderName != "" {
				// heartbeats should have suppressed this,
				// so if we are getting here it means
				// we have not gotten a heartbeat in too long.
				// Assume the leader failed... force a reconnection.
				cktP, ok := s.cktAllByName[s.leaderName]
				if ok {
					//vv("%v assuming leader is down and definitely deleting our ckt to it. leaderName='%v'", s.me(), s.leaderName)
					s.deleteFromCktAll(cktP)
					if cktP.ckt != nil {
						cktP.ckt.Close(nil)
					}
					// need to do this too, right?
					s.leaderName = ""
				}
			}
			// here we are in <-s.electionTimeoutCh
			s.connectToMC("mainline <-s.electionTimeoutCh")

			//vv("%v <-s.election timeout: %v", s.me(), s.countElections)

			if s.role == LEADER {

				// section 6.2 is a bit risky during bootstrap, when
				// one needs the leader to stay up and find its
				// followers as they bootstrap up the cluster, no
				// matter how slowly that goes.
				// We put off stepping down if no recent pong quorum
				// for quite a while (several leader election timeouts).

				leaderStepDownIfNoRecentPongQuorum := true // 052 partition_test green

				starting := time.Since(s.t0) < s.cfg.NoFaultTolDur
				bootstrapping := !s.wroteBootstrapFirstLogEntry.IsZero() &&
					time.Since(s.wroteBootstrapFirstLogEntry) < s.cfg.NoFaultTolDur

				if starting || bootstrapping {
					// do not step down prematurely
					leaderStepDownIfNoRecentPongQuorum = false
				}
				// side-effect: will set s.state.MC.IsCommitted if possible.
				notInMC := s.leaderShouldStepDownBecauseNotInCurCommittedMC()
				_ = notInMC
				wantPongStepDown := false
				if !notInMC && leaderStepDownIfNoRecentPongQuorum {

					quor := s.quorum() - 1 // -1 to count ourselves!

					// if quor > 0 allows single node cluster to function; e.g.
					// Test001_no_replicas_write_new_value
					sz := s.clusterSize()
					//vv("%v quor = %v; clusterSize = %v", s.name, quor, sz)
					if sz > 1 && quor > 0 {

						// leader stepping down at random times in tests
						// like 017; seems this is a bit too sensitive.
						// so give it 4T before stepping down.
						// And include our clock-drift-bound.
						//window := 4*s.cfg.MinElectionDur + s.cfg.ClockDriftBound
						// centalize to this func:
						window := s.followerDownDur()
						now := time.Now()
						if s.leaderElectedTm.IsZero() {
							panic("s.leaderElectedTm must not be zero tm!")
						}
						reliableTm := now.Add(-window)

						// want s.leaderElectedTm < reliableTm to know
						// that the leader has been up for long enough
						// to ping and hear back from followers.

						//if s.leaderElectedTm.After(reliableTm) {

						durSinceReliable := reliableTm.Sub(s.leaderElectedTm)
						if durSinceReliable < 0 {
							// not reliable yet
							//vv("%v we have not been leader long enough to have reliably pinged followers: durSinceReliable=%v is negative", s.me(), durSinceReliable)
						} else {
							// we have been leader at least two
							// election timeout cycles.
							// To be very conservative, we set redline
							// to also allow however much time it has
							// been since the last election timeout
							// fired plus the window, meaning we must
							// not have heard
							// from anyone for more than 4*T + last
							// election. Thus if
							// newestQuorumPongTm < redline then
							// we have not had a quorum of pongs
							// in a very long time (on average 5.5*T;
							// at minimum 4*T and at most 6*T).
							redline := reliableTm // or just 4*T

							newestQuorumPongTm, quorumPongOK, newestQuorumPong :=
								s.leaderFullPongPQ.quorumPongTm(quor)
							_ = newestQuorumPong
							// note newestQuorumPongTm will be zero if !quorumPongOK.
							been := redline.Sub(newestQuorumPongTm)
							_ = been
							wantPongStepDown = !quorumPongOK || newestQuorumPongTm.Before(redline)
							if wantPongStepDown {
								alwaysPrintf("%v wantPongStepDown=true; since redline its been=%v (step down if positive); newestQuorumPongTm = '%v'; quorumPongOK = %v; redline=%v; stepDown = %v; durSinceReliable=%v", s.me(), been, newestQuorumPongTm, quorumPongOK, nice(redline), wantPongStepDown, durSinceReliable)
							}
						}
					}
				} // end if leaderStepDownIfNoRecentPongQuorum

				// need to lead even if not in MC! for liveness; section 4.2.2
				//if notInMC || wantPongStepDown {
				if wantPongStepDown {
					// step down, per section 6.2.
					// Otherwise clients could be stuck
					// talking to us, a non-leader, which could
					// delay their getting a reply by alot.
					alwaysPrintf("%v wantPongStepDown=true", s.me()) // I am leader (since %v; for %v) but I did not get pong quorum in window=%v, quor=%v; \n newestQuorumPongTm='%v' (%v ago);\n newestQuorumPong='%#v'; redline='%v' since redline = %v", s.me(), nice9(s.leaderElectedTm), now.Sub(s.leaderElectedTm), window, quor, nice9(newestQuorumPongTm), time.Since(newestQuorumPongTm), newestQuorumPong, nice9(redline), now.Sub(redline))

					s.leaderName = ""
					s.leaderID = ""
					s.leaderURL = ""
					s.lastLeaderActiveStepDown = time.Now()
					s.becomeFollower(s.state.CurrentTerm, nil, SAVE_STATE)

					continue
				}
				// WHY? regularly check for step down? not sure but it is needed:
				//s.ay("%v as leader, reset election timeout", s.me())
				// if we comment out: makes red 052 partition_test!
				// Hence this _IS_ essential:
				// (We have to keep checking if we should step down, for one).
				//vv("%v calling resetElectionTimeout as leader", s.name)
				s.resetElectionTimeout("leader election timeout")

				// periodically (on election timeouts), we
				// want to cleanup un-refreshed sessions.
				s.garbageCollectOldSessions()

				continue
				// end if leader
			} else {
				// not leader.

				if s.observerOnlyNow() {
					alwaysPrintf("%v observerOnly() true so shutting down.", s.me())
					// try to get a single node cluster to shut itself
					// down when removed from membership.
					return
				}
			}

			// Partitioned and re-joining node can disrupt
			// a stable cluster by forcing an election with
			// a high term that they incremented on their own.
			// For stability, implement two-phase elections
			// with a pre-vote, remaining a FOLLOWER throughout.
			// If they can win a pre-vote without increment
			// their Term, then they can increment their Term
			// and conduct a real election as a CANDIDATE.

			// page 137 section 9.6
			// "In the Pre-Vote algorithm, a candidate only
			// increments its term if it first learns from
			// a majority of the cluster that they would be
			// willing to grant the candidate their votes
			// (if the candidate’s log is sufficiently up-to-date,
			// and the voters have not received heartbeats
			// from a valid leader for at least a baseline
			// election timeout)."

			// should always be on now.
			//vv("%v calling beginPreVote", s.me())
			s.beginPreVote()
			continue

		case tkt := <-s.singleUpdateMembershipReqCh:

			_, sentOnNewCkt, ckt2, err := s.handleLocalModifyMembership(tkt)
			if err != nil {
				return err
			}
			if ckt2 != nil && !sentOnNewCkt {
				err := s.handleNewCircuit(ckt2, done0, arrivingNetworkFrag, cktHasError, cktHasDied)
				if err != nil {
					return err
				}
			}

		case state2 := <-s.ApplyNewStateSnapshotCh:
			s.applyNewStateSnapshot(state2, "<-s.ApplyNewStateSnapshotCh")

		case itkt := <-s.requestRemoteInspectCh:
			prior, already := s.remoteInspectRegistryMap[itkt.fcid]
			if already {
				panic(fmt.Sprintf("conflict! cannot re-use fcid '%v'/must delete after use. prior='%#v'", itkt.fcid, prior))
			}
			s.remoteInspectRegistryMap[itkt.fcid] = itkt
			err = s.SendOneWay(itkt.ckt, itkt.reqFrag, -1, 0)
			_ = err // don't panic on halting.
			if err != nil {
				alwaysPrintf("%v non nil error '%v' on s.requestRemoteInspectCh -> SendOneWay to '%v'", s.me(), err, itkt.ckt.RemotePeerID)
				//return rpc.ErrHaltRequested
				continue
			}
			//vv("%v good: sent inspection request to itkt.ckt.RemotePeerName='%v'", s.me(), itkt.ckt.RemotePeerName)

		case fragCkt := <-arrivingNetworkFrag:
			frag := fragCkt.frag

			s.countFrag++
			//s.ay("%v <-net sees %v [%v] (seen: %v) total=%v", s.me(), msgop(frag.FragOp), frag.Typ, s.statString(frag), s.countFrag)

			//vv("%v <-net sees %v [%v] (seen: %v) total=%v; frag=%v", s.me(), msgop(frag.FragOp), frag.Typ, s.statString(frag), s.countFrag, frag)

			// centralize rejecting ancient/too far
			// in the future/wrong cluster garbage.
			if drop, reason := s.insaneTooOldOrNew(frag); drop {
				_ = reason
				alwaysPrintf("dropping frag outside our window: %v", reason)
				// can be from SESS_END for instance.
				continue // ignore it
			}
			if drop, reason := s.notOurClusterAndNotCircuitStart(frag); drop {
				_ = reason
				alwaysPrintf("dropping frag not in our cluster: %v", reason)
				panic("what? no cluster tests yet!")
				continue
			}
			s.peerJoin(frag, fragCkt.ckt)

			//now := time.Now()
			switch frag.FragOp {

			case PruneRedundantCircuit:
				s.handlePruneRedundantCircuit(frag)

			case NotifyClientNewLeader:
				// only for clients
				if s.role != CLIENT && s.cfg.PeerServiceName != TUBE_CLIENT {
					panicf("%v only send NotifyClientNewLeader to clients. PeerServiceName = '%v'", s.me(), s.cfg.PeerServiceName)
				}
				s.leaderName, _ = frag.GetUserArg("leaderName")
				s.leaderID, _ = frag.GetUserArg("leaderID")
				s.leaderURL, _ = frag.GetUserArg("leaderURL")
				//vv("%v got NotifyClientNewLeader: '%v'", s.name, s.leaderName)

				// also take the MC payload so we know all the tube nodes
				if len(frag.Payload) > 0 {
					mc := &MemberConfig{}
					_, err := mc.UnmarshalMsg(frag.Payload)
					panicOn(err)
					s.clientInstallNewTubeClusterMC(mc)
				}

			case InstallEmptyMC:
				s.handleInstallEmptyMC(frag, fragCkt.ckt)

			case RequestStateSnapshot:
				//vv("%v sees frag.FragOp RequestStateSnapshot", s.name)
				s.handleRequestStateSnapshot(frag, fragCkt.ckt, fmt.Sprintf("frag.FragOp=RequestStateSnapshot from '%v'", frag.FromPeerName))

			case StateSnapshotEnclosed:
				s.handleStateSnapshotEnclosed(frag, fragCkt.ckt, fmt.Sprintf("frag with FragOp==StateSnapshotEnclosed from '%v'", fragCkt.ckt.RemotePeerName))

			case CircuitSetupHasBaseServerAddr:
				// the first frag of a new circuit setup.
				baseServerHostPort, ok := frag.GetUserArg("baseServerHostPort")
				_ = baseServerHostPort
				if ok {
					//vv("%v new first frag from baseServerHostPort='%v'", s.me(), baseServerHostPort)
				}
			case PeerListReq:
				err = s.peerListRequestHandler(frag, fragCkt.ckt)
				if err != nil {
					panic(err)
					return err
				}
			case PeerListReply:
				err = s.peerListReplyHandler(frag)
				if err != nil {
					panic(err)
					return err
				}

			case RequestPreVoteMsg:
				//s.ay("%v RequestPreVoteMsg", s.me())

				reqPreVote := &RequestVote{}
				_, err := reqPreVote.UnmarshalMsg(frag.Payload)
				panicOn(err)

				if frag.FromPeerID == s.PeerID {
					panic("should not see frag from self")
				}
				if reqPreVote.ClusterID != s.ClusterID {
					//vv("%v wrong ClusterID reqPreVote='%v'", s.me(), reqPreVote)
					panic("wrong ClusterID")
					continue // drop
				}
				//vv("%v sees RequestPreVote '%v'", s.me(), reqPreVote)
				s.handleRequestPreVote(reqPreVote, fragCkt.ckt)

			case PreVoteMsg:
				//s.ay("%v PreVoteMsg", s.me())

				preVote := &Vote{}
				_, err := preVote.UnmarshalMsg(frag.Payload)
				panicOn(err)
				if preVote.ClusterID != s.ClusterID {
					//vv("%v wrong ClusterID preVote='%v'", s.me(), preVote)
					panic("wrong ClusterID")
					continue // drop it
				}
				//vv("%v sees RequestPreVote '%v'", s.me(), preVote)
				s.tallyPreVote(preVote)

			case LeaderToClientTicketAppliedMsg:
				//vv("%v LeaderToClientTicketAppliedMsg", s.me())

				answer := &Ticket{}
				_, err := answer.UnmarshalMsg(frag.Payload)
				panicOn(err)

				if answer.ClusterID != s.ClusterID {
					//vv("%v wrong ClusterID answer='%v'", s.me(), answer)
					alwaysPrintf("%v wrong ClusterID answer.ClusterID='%v' vs s.ClusterID='%v'", s.me(), answer.ClusterID, s.ClusterID)
					continue
				}

				s.hlc.ReceiveMessageWithHLC(answer.CreateHLC)

				// note now that when the "client" is also a
				// peer, it will have closed done and continued
				// the local Ticket as soon as it was applied.
				// This is the leader telling us again, in case
				// we were an external client (not participating
				// in Raft). So this is actually very common.
				question, ok := s.WaitingAtFollow.get2(answer.TicketID)
				//vv("%v sees LeaderToClientTicketAppliedMsg: ok=%v; answer='%v'\n question=%v'", s.me(), ok, answer, question)
				if ok {
					answer.Stage += ":LeaderToClientTicketApplied_answer_found"
				} else {
					//vv("%v drat! got answer, no question found. answer= %v;", s.me(), answer)
					answer.Stage += ":LeaderToClientTicketApplied_answer_no_question_found"
					q2, ok2 := s.WaitingAtLeader.get2(answer.TicketID)
					if ok2 {
						answer.Stage += ":LeaderToClientTicketApplied_q2_match_in_WaitingAL"
						question = q2
					} else {
						//vv("%v drat2! got answer, no q2 found in WaitAL either. answer= %v;", s.me(), answer)
						answer.Stage += ":LeaderToClientTicketApplied_drat2_no_q2_found_in_WaitAL"
						continue
					}
				}

				// the client (Read/Write) is waiting for
				// the question.Done channel, and then will
				// read question.Val, so we have copy to
				// observed-from-the-transaction value
				// over the the question where they expect get it.
				s.answerToQuestionTicket(answer, question)
				//vv("%v calling FinishTicket on question tkt='%v'", s.me(), question)
				s.FinishTicket(question, false)

			case RedirectTicketToLeaderMsg:
				//vv("%v RedirectTicketToLeaderMsg", s.me())

				tkt := &Ticket{}
				_, err := tkt.UnmarshalMsg(frag.Payload)
				panicOn(err)
				//vv("%v RedirectTicketToLeaderMsg; tkt=%v", s.me(), tkt)

				if tkt.ClusterID != s.ClusterID {
					//vv("%v wrong ClusterID in RedirectTicketToLeaderMsg tkt'%v'", s.me(), tkt)
					panic("wrong ClusterID")
					continue
				}
				s.hlc.ReceiveMessageWithHLC(tkt.CreateHLC)

				// in case RedirectTicketToLeaderMsg
				if s.bootstrappedOrForcedMembership(tkt) {
					//vv("%v continue after bootstrappedOrForcedMembership true", s.name)
					continue
				}

				// has leader changed in the meantime?
				// no need to add to Waiting, just forward it on;
				// leader will reply directly.
				if s.redirectToLeader(tkt) {
					vv("%v from was-leader redirect again to new leader '%v' for tkt=%v", s.me(), s.leaderName, tkt)
					continue // followers do not replicate tickets.
				}
				// INVAR: we are the leader.
				s.commandSpecificLocalActionsThenReplicateTicket(tkt, "RedirectTicketToLeaderMsg")

			case RequestVoteMsg:
				//s.ay("%v sees RequestVoteMsg", s.me())

				reqVote := &RequestVote{}
				_, err := reqVote.UnmarshalMsg(frag.Payload)
				panicOn(err)

				if frag.FromPeerID == s.PeerID {
					panic("should not see frag from self")
				}
				if reqVote.ClusterID != s.ClusterID {
					panic("wrong ClusterID")
					// drop or reply? probably drop.
				}
				s.handleRequestVote(reqVote, fragCkt.ckt)
				continue

			case VoteMsg:
				//s.ay("%v VoteMsg", s.me())

				vote := &Vote{}
				_, err := vote.UnmarshalMsg(frag.Payload)
				panicOn(err)
				if vote.ClusterID != s.ClusterID {
					//vv("%v wrong ClusterID VoteMsg='%v'", s.me(), vote)
					panic("wrong ClusterID")
					continue
				}
				s.tallyVote(vote)

			case AppendEntriesMsg:
				//s.ay("%v AppendEntriesMsg", s.me())

				ae := &AppendEntries{}
				_, err := ae.UnmarshalMsg(frag.Payload)
				panicOn(err)
				//zz("%v got AppendEntries '%v'", s.me(), ae)

				if ae.ClusterID != s.ClusterID {
					//vv("%v wrong ClusterID ae='%v'", s.me(), ae)
					panic("wrong ClusterID")
					continue // drop
				}
				s.handleAppendEntries(ae, fragCkt.ckt)

				// Ugh. checking isFollowerKaput causes
				// pre-mature shutdown of node_4
				// trying to joint the cluster for the first time.
				// some kind of logical race.
				//
				// is the current MC both committed and without us?
				// chicken and egg though: why would leader tell us,
				// if we are not in the membership?
				// we could move replica to observer and send observers
				// the latest MC.
				//if false { // off for 065 shadow replicas
				//	if s.isFollowerKaput() {
				//		alwaysPrintf("%v isFollowerKaput() true, returing from TubeNode.Start()", s.me())
				//		return
				//	}
				//}

			case AppendEntriesAckMsg:
				//s.ay("%v AppendEntriesAckMsg", s.me())

				aeAck := &AppendEntriesAck{}
				_, err := aeAck.UnmarshalMsg(frag.Payload)
				panicOn(err)
				if aeAck.ClusterID != s.ClusterID {
					//vv("%v wrong ClusterID aeAck='%v'", s.me(), aeAck)
					panic("wrong ClusterID")
					continue // drop
				}
				err0 = s.handleAppendEntriesAck(aeAck, fragCkt.ckt)
				if err0 != nil {
					return // try shutdown on commited MC without us.
				}

			case 0: // , FirstFragIsReplica:
				//vv("%v case 0 || FirstFragIsReplica; FragOp='%v'", s.me(), msgop(frag.FragOp))
				if frag.Typ == rpc.CallPeerStartCircuit {
					//zz("%v do stuff for new replica joining here. from = '%v'", s.me(), rpc.AliasDecode(frag.FromPeerID))
					s.peerJoin(frag, nil)
					continue
				}
			case TellClientSteppedDown:
				//s.ay("%v sees TellClientSteppedDown, redirect tickets to new leader?", s.me())
				leaderID, ok := frag.GetUserArg("leader")
				if ok && leaderID != "" {
					s.leaderID = leaderID
				}
				leaderName, ok := frag.GetUserArg("leaderName")
				if ok && leaderName != "" {
					s.leaderName = leaderName
				}
				leaderURL, ok := frag.GetUserArg("leaderURL")
				if ok && leaderURL != "" {
					s.leaderURL = leaderURL
				}
				//vv("%v TODO: submit tickets to new leader instead.", s.me())

			case ObserveMembershipChange:
				s.handleObserveMembershipChange(frag, fragCkt.ckt)

			//case ReliableMemberHeartBeatToCzar:
			//	s.handleReliableMemberHeartBeatToCzar(frag, fragCkt.ckt)

			default:
				panic(fmt.Sprintf("unknown raft message: not CallPeerStartCircuit frag.FragOp='%v'; frag='%v'", frag.FragOp, frag.String()))
			}
			//s.ay("%v after switch on frag.FragOp!", s.me())

		case ckt := <-cktHasDied:
			_ = ckt
			if false {
				//vv("%v circuit has died; ckt = '%v'", s.name, ckt)
				oldCktP, haveOld := s.cktall[ckt.RemotePeerID]
				if haveOld {
					s.deleteFromCktAll(oldCktP)
					if oldCktP.ckt != nil {
						oldCktP.ckt.Close(nil) // maybe?
					}
					delete(s.cktReplica, oldCktP.PeerID)
				}
			}
		case cktErr := <-cktHasError:
			if false {
				ckt := cktErr.ckt
				alwaysPrintf("%v cktErr reported frag err = '%v'", s.name, cktErr.fragerr)
				oldCktP, haveOld := s.cktall[ckt.RemotePeerID]
				if haveOld {
					s.deleteFromCktAll(oldCktP)
					if oldCktP.ckt != nil {
						oldCktP.ckt.Close(nil) // maybe?
					}
					delete(s.cktReplica, oldCktP.PeerID)
				}
			}
			// new Circuit connection arrives: a replica joins the cluster.
		case ckt := <-newCircuitCh:
			//s.ay("%v ckt := <-newCircuitCh, ckt = '%v'", s.me(), ckt)
			//vv("%v ckt := <-newCircuitCh, from ckt.RemotePeerName='%v'; ckt.RemotePeerID='%v'", s.me(), ckt.RemotePeerName, ckt.RemotePeerID) // not seen 059
			err := s.handleNewCircuit(ckt, done0, arrivingNetworkFrag, cktHasError, cktHasDied)
			if err != nil {
				return err
			}
		case <-done0:
			//s.ay("%v <-done0", s.me())

			//vv("%v: done0! myPeer closing up b/c context canceled", s.name)
			return rpc.ErrContextCancelled

			// already above:
			//case <-s.Halt.ReqStop.Chan:
			//	//	//zz("%v: halt.ReqStop seen", s.name)
			//	return rpc.ErrHaltRequested

		} // end select
	} // end for i
	return nil
}

// local request on node, no replication or Ticket needed.
func (s *TubeNode) handleLocalCircuitDisconnectRequest(disco *discoReq) {
	for name := range disco.who {
		cktP, ok := s.cktAllByName[name]
		if ok {
			if cktP.ckt != nil {
				cktP.ckt.Close(nil)
				s.deleteFromCktAll(cktP)
			}
		}
	}
	disco.done.Close()
}

func (s *TubeNode) clientInstallNewTubeClusterMC(mc *MemberConfig) {
	if s.state == nil || mc == nil {
		return
	}
	if s.state.MC != nil {
		if s.state.MC.ConfigVersion >= mc.ConfigVersion {
			return
		}
	}
	s.state.MC = mc
}

func (s *TubeNode) handleNewCircuit(
	ckt *rpc.Circuit,
	done0 <-chan struct{},
	arrivingNetworkFrag chan *fragCkt,
	cktHasError chan *cktAndError,
	cktHasDied chan *rpc.Circuit,

) (wantReturn error) {
	if ckt.RemotePeerID == "" {
		panic(fmt.Sprintf("cannot have ckt.RemotePeerID empty: ckt='%v'", ckt))
	}
	//vv("%v top handleNewCircuit ckt from '%v'", s.name, ckt.RemotePeerName)

	// below we work hard to distinguish
	// replicas (that participate as Raft nodes)
	// versus clients (that should _not_ be sent
	// AE or heartbeats). Of course, regular
	// replicas can act as clients too, but
	// we really don't want to send extra
	// traffic to clients that just want
	// to read/write the store. The PeerServiceName
	// TUBE_REPLICA is used now (instead of flag
	// cktPlus.isReplica)

	isRedund, earlierCkt := s.shouldPruneRedundantCkt(ckt)
	if isRedund {
		frag := s.newFrag()
		frag.FragOp = PruneRedundantCircuit
		frag.FragSubject = "PruneRedundantCircuit"
		frag.SetUserArg(pruneVictimKey, ckt.CircuitID)
		frag.SetUserArg(pruneKeeperKey, earlierCkt.CircuitID)
		s.SendOneWay(ckt, frag, -1, 1)
		s.SendOneWay(earlierCkt, frag, -1, 0)
		return // good: all tests green with this return (pruning fully on).
	}

	// this _was_ the only place where ckt are added to s.cktall.
	// in <-newCircuitCh here.
	//vv("%v in <-newCircuitCh here.", s.me())
	cktP, rejected := s.addToCktall(ckt)
	_ = rejected
	if rejected {
		alwaysPrintf("ouch 555555555 ckt rejected from cktall: '%v'", ckt.RemotePeerName)
		panic("when ckt rejected?")
	}

	// NB: myPeer.PeerID == ckt.LocalPeerID

	if s.cfg.isTest && s.verifyPeerReplicaOrNot != nil {
		panicAtCap(s.verifyPeerReplicaOrNot) <- cktP.thinclone()
	}
	if cktP.PeerServiceName == TUBE_REPLICA {
		// below only handles reads, so we
		// gotta verify connection
		// count here rather than using
		// the centralized read channel.
		// Followers don't contact other followers until they
		// want to hold elections, so if we are a follower we
		// might never get a read from another follower.
		// Used in clusterup_test.go and
		// waitForConnectedGrid().
		if s.cfg.isTest {
			s.verifyCluster(ckt.RemotePeerID)
		}

		// allow elections to not fail
		// on boot. eg 021 test.
		// We are inside handling <-newCircuitCh here
		//
		// I think this is redund with addToCktall now
		//s.setReplicaCktIfMember(cktP) // cktReplica-write (1)
	}
	//vv("%v adding to s.ckt a ckt from '%v' (isReplica=%v)", s.me(), rpc.AliasDecode(ckt.RemotePeerID), cktP.isReplica)

	//vv("%v: added to cktall ckt to RemotePeerName='%v'; now len(cktReplica)=%v; cktall='%v'", s.name, ckt.RemotePeerName, len(s.cktReplica), cktallToString(s.cktall))
	// and recompute the cktReplica so our clusterSize updates!

	// any tkt stalled waiting for leader might
	// be able to progress now. makes 402 green!
	// (solves the logical race of asking for leader before
	// its ckt gets saved in cktall).
	// Undispatchables will just be re-saved.
	// BUT: all we got was a new ckt... the
	// leader's first message has not come
	// through yet. Thus AE will also always
	// check and call dispatchAwaitingLeaderTickets()
	// if there are tickets waiting for a leader.
	s.dispatchAwaitingLeaderTickets()

	//vv("%v: got from <-newCircuitCh! I am peer '%v'; I see new peerURL: '%v'; s.cktall sz=%v; clusterSize()=%v\nconnection-established: ckt.LocalPeerID(%v) sees ckt.RemotePeerID(%v)", s.name, myPeer.URL(), ckt.RemoteCircuitURL(), len(s.cktall), s.clusterSize(), rpc.AliasDecode(ckt.LocalPeerID), rpc.AliasDecode(ckt.RemotePeerID))

	//vv("%v after adding %v, now cktall is: %v", s.me(), rpc.AliasDecode(ckt.RemotePeerID), s.showCktall())

	//s.Halt.AddChild(ckt.Halt) // error: child already has parent!

	debugGlobalCkt.Set(ckt.CircuitID, ckt)
	s.cktAuditByCID.Set(ckt.CircuitID, ckt)
	s.cktAuditByPeerID.Set(ckt.RemotePeerID, ckt)

	// listen to this peer on a separate goro
	go func(ckt *rpc.Circuit, cktP *cktPlus) (err0 error) {

		//var lastHeard time.Time // track liveness
		ctx := ckt.Context
		cktContextDone := ctx.Done()

		//if ckt.RemoteServiceName != TUBE_REPLICA {
		//vv("%v: (ckt '%v') 888888888888 got-incoming-ckt: from RemotePeerName:'%v' hostname '%v'; pid = %v; pointer-cktP=%p; ckt='%v'", s.name, ckt.Name, ckt.RemotePeerName, ckt.RpbTo.Hostname, ckt.RpbTo.PID, cktP, ckt)
		//}

		defer func() {
			//vv("%v: (ckt '%v') defer running! finishing RemotePeer goro.", s.name, ckt.RemotePeerName) // , stack())

			//vv("%v: (ckt '%v') defer running! finishing ckt for RemotePeer goro; from hostname '%v'; pid = %v", s.name, ckt.RemotePeerName, ckt.LpbFrom.Hostname, ckt.LpbFrom.PID)
			//}
			debugGlobalCkt.Del(ckt.CircuitID)
			s.cktAuditByCID.Del(ckt.CircuitID)
			// this is okay. Since there can only be one, we
			// must be deleting the one.
			s.cktAuditByPeerID.Del(ckt.RemotePeerID)

			ckt.Close(err0)
			// subtract from peers, avoid race by using peerLeftCh.
			//vv("%v reduce s.ckt peer set since peer went away: '%v'", s.me(), ckt.RemotePeerID)
			// try to not have a race between shutdowns keep us
			// from garbage collecting the ckt.
			t0gc := time.Now()
			_ = t0gc
			select {
			case s.peerLeftCh <- ckt.RemotePeerID:
				//vv("peerLeftCh after %v", time.Since(t0gc))
			case <-time.After(time.Millisecond * 10):
				//vv("arg! could not send on peerLeftCh for 10ms")
			}
			select {
			case s.peerLeftCh <- ckt.RemotePeerID:
			case <-done0:
				//zz("%v: (ckt '%v') done0!", s.name, ckt.Name)
			case <-s.Halt.ReqStop.Chan:
				//zz("%v: (ckt '%v') top func halt.ReqStop seen", s.name, ckt.Name)
			}
			//if ckt.RemoteServiceName != TUBE_REPLICA {
			//vv("%v: (ckt '%v') 999999999999 got-departing-ckt: from hostname '%v'; pid = %v; pointer-cktP=%p", s.name, ckt.Name, ckt.LpbFrom.Hostname, ckt.LpbFrom.PID, cktP)
		}()

		//zz("%v: (ckt '%v') <- got new incoming ckt", s.name, ckt.Name)
		//zz("incoming ckt has RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
		//zz("incoming ckt has LocalCircuitURL = '%v'", ckt.LocalCircuitURL())

		//tooLongDur := s.maxElectionTimeoutDur() // red 052 too early
		//tooLongDur := s.followerDownDur() // 707 client_test red interm
		//tooLongDur := 2 * s.followerDownDur()
		//tooLongCh := time.After(tooLongDur)
		for {
			select {

			case frag := <-ckt.Reads:
				//lastHeard = time.Now()
				//if frag.FragOp == 1 || frag.FragOp == 2 {
				// AE and AEack so much noise, quiet for debug
				//} else {
				//vv("%v: (ckt %v) ckt.Reads sees frag:'%s'; my PeerID='%v'", s.name, ckt.Name, frag, s.PeerID)
				//}
				// centralize to avoid locking in a bajillion places
				select {
				case arrivingNetworkFrag <- &fragCkt{frag: frag, ckt: ckt}:
				case <-s.Halt.ReqStop.Chan:
					//zz("%v: halt.ReqStop seen", s.name)
					return rpc.ErrHaltRequested
				}
			case fragerr := <-ckt.Errors:
				//zz("%v: (ckt '%v') fragerr = '%v'", s.name, ckt.Name, fragerr)
				_ = fragerr
				select {
				case cktHasError <- &cktAndError{ckt: ckt, fragerr: fragerr}:
				case <-s.Halt.ReqStop.Chan:
					//zz("%v: halt.ReqStop seen", s.name)
					return rpc.ErrHaltRequested
				}

			case <-ckt.Halt.ReqStop.Chan:
				//vv("%v: (ckt '%v') ckt halt requested (RemotePeerName: '%v').", s.name, ckt.Name, ckt.RemotePeerName) // NOT SEEN, SO WE used to LEAK THIS GORO! fixed by using LoopComm and having ckt.go:1197 tell the read loop which circuits they support, and in turn they notify those ckt when the socket is shut/closed.
				select {
				case cktHasDied <- ckt:
				case <-s.Halt.ReqStop.Chan:
					//zz("%v: halt.ReqStop seen", s.name)
					return rpc.ErrHaltRequested
				}
				return

			case <-cktContextDone:
				//vv("%v: cktContextDone (ckt '%v' to RemotePeerName:'%v') ", s.name, ckt.Name, ckt.RemotePeerName) // NOW SEEN, SO WE no longer leak this goro always, but have 8415 such open with only 200 members so hmm...
				select {
				case cktHasDied <- ckt:
				case <-s.Halt.ReqStop.Chan:
					//zz("%v: halt.ReqStop seen", s.name)
					return rpc.ErrHaltRequested
				}
				return // server finishing on done!
			case <-done0:
				//zz("%v: (ckt '%v') done0!", s.name, ckt.Name)
				return
			case <-s.Halt.ReqStop.Chan:
				//zz("%v: (ckt '%v') top func halt.ReqStop seen", s.name, ckt.Name)
				return
			}
		}

	}(ckt, cktP)
	return
}

// answers the question: should this ckt be pruned?
func (s *TubeNode) shouldPruneRedundantCkt(cktQuery *rpc.Circuit) (isRedund bool, earlierCkt *rpc.Circuit) {
	remoteQ := cktQuery.RemotePeerName
	if cktQuery.LocalPeerName != s.name {
		panicf("why are we trying to prune a ckt that does not terminate locally with ourselves? my name='%v'; the cktQuery='%v'", s.name, cktQuery)
	}

	var pruneLocal *rpc.Circuit

	// maintain the lock while we look through the many map entries
	s.cktAuditByPeerID.Update(func(m map[string]*rpc.Circuit) {
		ckt, ok := m[cktQuery.RemotePeerID]
		if !ok {
			return
		}
		if ckt.RemotePeerName == cktQuery.RemotePeerName &&
			ckt.LocalPeerName == s.name &&
			ckt.RemotePeerID == cktQuery.RemotePeerID {
			vv("%v found a redundant ckt to '%v'", s.name, remoteQ)

			// for stablity, keep the lesser (lexicographically)
			if ckt.CircuitID < cktQuery.CircuitID {
				vv("%v: shouldPruneRedundantCkt will prune cktQuery as redundant (to remote: '%v')", s.name, remoteQ)
				isRedund = true
				earlierCkt = ckt
				return
			}
			if ckt.CircuitID > cktQuery.CircuitID {
				// keep the new one, prune the old/existing
				pruneLocal = ckt

				// get rid of old value under this key... but technically
				// this is redundant since the next assignment would
				// blow it away; so skip:
				// delete(m, cktQuery.RemotePeerID)
				m[cktQuery.RemotePeerID] = cktQuery

				s.cktAuditByCID.Update(func(m2 map[string]*rpc.Circuit) {
					delete(m2, ckt.CircuitID)
					m2[cktQuery.CircuitID] = cktQuery
				})
			}
		}
	})
	if pruneLocal == nil {
		return
	}
	// INVAR: isRedund is false so caller will accept new cktQuery,
	// but pruneLocal is redundant with cktQuery...
	// we delete pruneLocal next.

	// swap in new, swap out old.
	// well, the new will added... by caller.
	// so just need to delete the old.

	vv("%v: shouldPruneRedundantCkt will prune old as redundant; caller should swap in new cktQuery (to remote: '%v')", s.name, remoteQ)
	oldCktPlus, ok := s.cktall[pruneLocal.RemotePeerID]
	if ok && oldCktPlus.ckt != nil {
		s.pruneRedundantCircuitMessageTo(pruneLocal, pruneLocal.CircuitID, cktQuery.CircuitID)
		s.deleteFromCktAll(oldCktPlus)
		oldCktPlus.ckt.Close(ErrPruned)
	}
	return
}

func (s *TubeNode) deleteFromCktAll(oldCktP *cktPlus) {
	//vv("%v deleteFromCktAll called on '%v'", s.me(), oldCktP)
	oldCktP.Close() // shut down watchdog
	delete(s.peers, oldCktP.PeerID)
	delete(s.cktall, oldCktP.PeerID)
	if !oldCktP.isGone {
		oldCktP.isGone = true
		oldCktP.goneTm = time.Now()
	}
	delete(s.cktAllByName, oldCktP.PeerName)

	// do we want this generally? nope! park instead
	//if oldCktP.ckt != nil {
	//	oldCktP.ckt.Close(nil)
	//}
	// do we want? think so.
	delete(s.cktReplica, oldCktP.PeerID)
}

func lte(a, b time.Time) bool {
	return !a.After(b)
}
func gte(a, b time.Time) bool {
	return !a.Before(b)
}

func NewTubeConfigTest(clusterSize int, clusterID string, useSimNet bool) (cfg *TubeConfig) {
	const yesIsTest = true
	cfg = newTubeConfig(clusterSize, clusterID, useSimNet, yesIsTest)
	return
}

func NewTubeConfigProd(clusterSize int, clusterID string) (cfg *TubeConfig) {
	cfg = newTubeConfig(clusterSize, clusterID, false, false)
	return
}

func newTubeConfig(clusterSize int, clusterID string, useSimNet, isTest bool) (cfg *TubeConfig) {

	cfg = &TubeConfig{
		PeerServiceName:    TUBE_REPLICA, // default
		ClusterID:          clusterID,
		ClusterSize:        clusterSize,
		TCPonly_no_TLS:     isTest,
		NoDisk:             isTest,
		HeartbeatDur:       time.Millisecond * 200,
		MinElectionDur:     time.Millisecond * 1200,
		BatchAccumulateDur: time.Millisecond * 100,
		UseSimNet:          useSimNet,
		ClockDriftBound:    time.Millisecond * 500,

		// if UseSimNet && faketime, do synctest.Wait? no effect if not UseSimNet
	}
	cfg.Init(false, isTest) // simae_test sets quiet=true, otherwise 800K prints, really.
	return
}

func (cfg *TubeConfig) Init(quiet, isTest bool) {
	if cfg.initCalled {
		panic("cannot call Init() twice")
	}
	cfg.initCalled = true
	cfg.isTest = isTest

	if faketime {
		if !isTest {
			panic("faketime only makes sense when testing, never in prod")
		}
		if !quiet {
			//alwaysPrintf("faketime=%v; cfg.UseSimNet=%v; cfg.SimnetGOMAXPROCS=%v", faketime, cfg.UseSimNet, cfg.SimnetGOMAXPROCS)
		}
	}

	if cfg.RpcCfg == nil {
		// must share the same rpc.Config to get the same simnet.
		rpcCfg := rpc.NewConfig()
		rpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
		rpcCfg.UseSimNet = cfg.UseSimNet
		rpcCfg.SimnetGOMAXPROCS = 1

		rpcCfg.ServerAddr = "127.0.0.1:0"
		rpcCfg.ServerAutoCreateClientsToDialOtherServers = true

		// this will turn off the reports of auto-cli creation.
		// Omitting those prints doesn't really speed up
		// the tests by much... and the progress report
		// might be useful for 030 cluster_up, but usually
		// getting rid of shutdown noise is worth it.
		rpcCfg.QuietTestMode = isTest

		// be sure we only start one replica peer per process.
		// rpc25519 will give us back an error if we try to
		// by accident (each server or client on its own gets one).
		rpcCfg.LimitedServiceNames = []string{string(TUBE_REPLICA)}
		rpcCfg.LimitedServiceMax = []int{1}

		cfg.RpcCfg = rpcCfg
	}
}

// succinct summary of past AppendEntries AE and AckAE
// calls/responses for RaftNodeInfo
type Ping struct {
	Sent time.Time `zid:"0"`
	Term int64     `zid:"1"`
	AEID string    `zid:"2"`
	//LogLastIndex int64     `zid:"3"`
	//LogLastTerm  int64     `zid:"4"`
}
type Pong struct {
	Sent            time.Time `zid:"0"`
	RecvTm          time.Time `zid:"1"`
	Term            int64     `zid:"2"`
	AEID            string    `zid:"3"`
	LogLastIndex    int64     `zid:"4"`
	LogLastTerm     int64     `zid:"5"`
	PeerID          string    `zid:"6"`
	PeerName        string    `zid:"7"`
	PeerServiceName string    `zid:"8"`
}

// RaftNodeInfo is kept by the leader for each
// server (peer) in the cluster.
// Raft calls this volatile state on leaders, which
// is reinitilized after election.
type RaftNodeInfo struct {
	PeerID                 string `zid:"0"`
	PeerName               string `zid:"1"`
	PeerServiceName        string `zid:"2"`
	PeerServiceNameVersion string `zid:"3"`

	// filled in by Inspect, but only just before
	// sending over wire; otherwise typically empty.
	PeerURL              string `zid:"4"`
	RemoteBaseServerAddr string `zid:"5"`
	IsInspectResponder   bool   `zid:"6"`

	// MatchIndex: index of highest log entry known
	// to be replicated on server. Initialized to 0,
	// increases monotonically.
	MatchIndex int64 `zid:"7"`

	// track heartbeat/AE history
	UnackedPing       Ping      `zid:"8"`  // move to LastFullPing when ackAE back
	LastFullPing      Ping      `zid:"9"`  // a complete round trip, begin
	LastFullPong      Pong      `zid:"10"` // a complete round trip, end
	LastHeardAnything time.Time `zid:"11"`

	LargestCommonRaftIndex int64 `zid:"12"` // becomes MatchIndex for quorum

	// guard against unique local configuration changes
	MinElectionTimeoutDur time.Duration `zid:"13"`

	MC *MemberConfig `zid:"14"`
}

type LogEntrySpan struct {
	Beg  int64 `zid:"0"`
	Endx int64 `zid:"1"`
}

func (s *LogEntrySpan) clone() (p *LogEntrySpan) {
	if s == nil {
		return nil
	}
	cp := *s
	p = &cp
	return
}

func (s *LogEntrySpan) IsEmpty() bool {
	if s == nil {
		return true
	}
	return s.Beg == s.Endx
}

// moved from rle.go for greenpack inlining

// TermsRLE conveys the structure of a log
// in compressed form, using run-length encoding.
type TermsRLE struct {
	// BaseC is our zero, first Index is at Base+1 if any.
	// so (BaseC, Endi] is our domain. We hold at most Endi
	// entries from BaseC+1, BaseC+2, ..., Endi. In fact
	// we hold exactly Endi-BaseC count of entries.
	BaseC int64      `zid:"0"` // "zero" Base after Compaction
	Endi  int64      `zid:"1"`
	Runs  []*TermRLE `zid:"2"`

	// replace with Base: CompactIndex int64 `zid:"3"`
	CompactTerm int64 `zid:"3"`
}

// TermRLE is an element of TermsRLE
type TermRLE struct {
	Term  int64 `zid:"0"`
	Count int64 `zid:"1"`
}

// TubeLogEntry: Raft log entries. Each entry contains
// command for state machine, and term
// when entry was received by leader (first index 1).
// (These are a write-ahead-log for applying
// state to the database; they store the state
// in pre-commit form, and nothing is commited
// until it is safely replicated to a majority.
type RaftLogEntry struct {
	// Term is the term when this entry was received by the leader.
	Term int64 `zid:"0"`

	// first Index is 1 (length of log after adding this entry)
	Index  int64   `zid:"1"`
	Ticket *Ticket `zid:"2"`

	// this servers CommitIndex, to allow us to know
	// for certain if some prefix of the log was actually
	// committed. This is my optimization, not in the
	// Raft descriptions.
	//
	// This CommitIndex will only ever know about the
	// commit of earlier entries than this one, since
	// we avoid re-writing the log and just append to it
	// (or overwrite if a new leader says to).
	CurrentCommitIndex int64 `zid:"3"`

	// when we restore/read raft log from disk, allow
	// setting the CompactTerm and BaseC representing
	// the previous prior compacted log. We verify
	// each link in the chain like a lego block snapping
	// onto the previous block in the chain. The
	// chain should maintain the invariant for
	// each entry e: e.PrevIndex == e.Index-1, which is
	// really there to make sure we simultaneously set
	// the correct PrevTerm at the same time, (since
	// its trivial to compute PrevIndex = Index -1).
	PrevIndex int64 `zid:"4"`
	PrevTerm  int64 `zid:"5"`

	Tm         time.Time `zid:"6"`
	LeaderName string    `zid:"7"`

	committed bool   // for safety assertions (but local and leader only!)
	msgbytes  []byte // to sustain the hash-chain
	node      *TubeNode

	testWrote bool
}

func (s *RaftLogEntry) clone() (c *RaftLogEntry) {
	cp := *s
	return &cp
}

// RaftState maintained for each server/node in the cluster.
type RaftState struct {
	name string // vprint convenience (node name)

	// jea added for debugging, not official part of the Raft algorithm.
	Serial int64 `zid:"0"`

	PeerName               string `zid:"1"`
	PeerServiceName        string `zid:"2"`
	PeerServiceNameVersion string `zid:"3"`

	// we expect PeerID to change on reboot of course
	PeerID    string `zid:"4"`
	ClusterID string `zid:"5"`

	// Raft: persistent state on all servers.

	// CurrentTerm is the lastest server has seen. Initialized
	// to zero of first boot. Increases monotonically.
	CurrentTerm int64 `zid:"6"` // like ballot number, a logical clock.

	// candidate that received vote in current term.
	// We have voted if either VotedFor is set.
	//
	// Update: no! voting against is not voting in an election!
	// Only voting FOR a candidate in a term is voting.
	//
	// This prevents double voting if we restart.
	// Notice that on restart we will have a different
	// PeerID, so we will look like a different node.
	// Hence the leader cannot help us, its vote dedup
	// mechanism won't prevent all kinds of double voting.
	// We must take it on ourselves to not vote twice.
	VotedFor     string `zid:"7"`
	VotedForName string `zid:"8"`
	// simpler flag if we have voted in CurrentTerm.
	HaveVoted     bool  `zid:"9"`
	HaveVotedTerm int64 `zid:"10"`

	// ====================================
	// Raft: volatile state on all servers. (Should actually be persisted!)
	// CommitIndex, LastApplied.
	// ====================================

	// CommitIndex is the index of the highest log entry
	// known to be committed (initialized to 0, increases
	// monotonically. Will converge again, but why not speed
	// that up?!)
	//
	// https://groups.google.com/g/raft-dev/c/oBkCbjRyqoA
	// "Yes. Persisting the commit index offers clear
	// benefits: upon startup, the server can immediately
	// restore to the latest committed state. This eliminates
	// the need to implement additional barriers that
	// would otherwise be required to prevent clients from
	// reading obsolete data from the state machine
	// during recovery."
	CommitIndex int64 `zid:"11"`

	// just in case there is skew, we track the term of
	// the entry specifically associated with the CommitIndex.
	// So we can precisely install a state snapshot
	// into the wall and recreate the logIndex so that
	// subsequent appends will work correctly.
	CommitIndexEntryTerm int64 `zid:"12"`

	// Raft: Last applied volatility should match the state machine.
	// "If the state machine is volatile, lastApplied should
	//  be volatile. If the state machine is persistent,
	//  lastApplied should be just as persistent."

	// LastApplied is the index of the highest log entry
	// applied to state machine (initialized to 0, increases
	// monotonically.
	LastApplied     int64 `zid:"13"`
	LastAppliedTerm int64 `zid:"14"` // the corresponding term

	// mini key-value store on disk
	// when writes are applied, they are
	// applied here. table -> key -> value
	KVstore *KVStore `zid:"15"`

	// ======== cluster config change support
	// (We implement the simpler one node at
	//  a time version -- not joint consensus --
	// and the Mongo Raft Reconfiguration version
	// which does not need rollback or separate
	// tracking of "committed"-ness of the MC

	MC *MemberConfig `zid:"16"`

	// known but not in current membership... so we
	// can find them again easlily if we restart
	// (and they are not in our config).
	Known *MemberConfig `zid:"17"`

	// put those removed from membership here
	// and send them all MC changes in AE heartbeat,
	// so a follower can learn of its own removal from MC.
	Observers *MemberConfig `zid:"18"`

	// ShadowReplicas are TUBE_REPLICA that see all
	// appendEntries but do not vote or add to quorum;
	// they are disjoint from s.state.MC, the current membership.
	//
	// ShadowReplicas be used to init/catch up/have at the
	// ready new replicas, or just to monitor all traffic.
	//
	// tubeadd -shadow will add to s.state.ShadowReplicas
	// rather than s.state.M using
	// tkt.Op = ADD_SHADOW_NON_VOTING leading to
	// a call to s.doAddShadow() on application after commit.
	ShadowReplicas *MemberConfig `zid:"19"`

	// raftStatePersistor saver.save() records its
	// invocation time here before saving to disk.
	LastSaveTimestamp time.Time `zid:"20"`

	// To "anchor" the state of log-compaction,
	// per Chapter 5, page 50 of the Raft dissertation,
	// we must save the last index and term that
	// the log-compaction discarded. Recall that the
	// the AppendEntries consistency check uses
	// these to anchor log replication in an
	// induction style step.
	CompactionDiscardedLast IndexTerm `zid:"21"`

	// ch6 client sessions for linz (linearizability).
	// key is SessionID
	SessTable map[string]*SessionTableEntry `zid:"22"`

	// ====================================
	// Raft: Volatile state on leaders (reinitilized after election)
	//   re-initialized after election (e.g. the
	//   []matchIndex for each server): See the RaftNodeInfo
	//   struct above.
	// ====================================
}

type IndexTerm struct {
	Index int64 `zid:"0"`
	Term  int64 `zid:"1"`
}

func (s *RaftState) votedStatus() string {
	return fmt.Sprintf(`
votedStatus{
      HaveVoted: %v,
  HaveVotedTerm: %v,
         PeerID: %q,
       PeerName: %q,
PeerServiceName: %q,
    CurrentTerm: %v,
       VotedFor: %v,
   VotedForName: %v,
}`,
		s.HaveVoted,
		s.HaveVotedTerm,
		rpc.AliasDecode(s.PeerID),
		s.PeerName,
		s.PeerServiceName,
		s.CurrentTerm,
		rpc.AliasDecode(s.VotedFor),
		s.VotedForName,
	)
}

func (s *RaftState) clone() (clon *RaftState) {
	cp := *s
	clon = &cp
	clon.KVstore = s.KVstore.clone()

	// ======== cluster config change support
	// alot of pointers, we must Clone to avoid sharing.
	clon.MC = s.MC.Clone()
	clon.SessTable = make(map[string]*SessionTableEntry)
	for k, v := range s.SessTable {
		clon.SessTable[k] = v.Clone()
	}
	clon.Known = s.Known.Clone()
	clon.Observers = s.Observers.Clone()
	clon.ShadowReplicas = s.ShadowReplicas.Clone()
	return
}

// TubeNode represents a single node in the Arb cluster
type TubeNode struct {
	cfg TubeConfig

	Ctx context.Context `msg:"-"` // from s.MyPeer.Ctx once available

	// shutdown might need to do different
	// stuff if node wasn't even started.
	started bool

	// allow handleAppendEntries() actual code
	// to be tested in the hosting harness.
	host hoster

	role                     RaftRole
	leaderID                 string
	leaderName               string
	leaderURL                string
	leaderElectedTm          time.Time
	electionTimeoutCh        <-chan time.Time
	electionTimeoutSetAt     time.Time
	electionTimeoutWasDur    time.Duration
	leaderFullPongPQ         *pongPQ
	lastLeaderActiveStepDown time.Time

	// batching raft writes to amortize fsyncs and network comms.

	// set to wakeup and submit a batch after a fixed interval.
	batchSubmitTimeCh <-chan time.Time
	// zero out after each batch; zero if no batch in progress
	batchStartedTm time.Time
	// when should this batch be sent off. roughly
	// the same as the deadline that batchSubmitTimeCh is armed for.
	batchSubmitTm   time.Time
	batchInProgress bool
	batchToSubmit   []*Ticket
	batchByteSize   int

	// update in handle AE when leader
	// changes, try to detect flapping
	currentLeaderFirstObservedTm time.Time

	nextElection           time.Time
	lastLegitAppendEntries time.Time

	leaderSendsHeartbeatsCh  <-chan time.Time
	leaderSendsHeartbeatsDue time.Time

	ClusterID string `zid:"0"`

	// comms
	URL                    string `zid:"1"`
	PeerID                 string `zid:"2"`
	PeerServiceName        string `zid:"3"`
	PeerServiceNameVersion string `zid:"4"`

	pushToPeerURL    chan string
	MyPeer           *rpc.LocalPeer `msg:"-"`
	Halt             *idem.Halter   `msg:"-"`
	peerLeftCh       chan string
	gotNewPeerListCh chan []string

	hostname string

	// sent by external getCircuitToLeader() for clients.
	setLeaderCktChan chan *rpc.Circuit

	requestRemoteInspectCh   chan *inspectionTicket
	remoteInspectRegistryMap map[string]*inspectionTicket

	singleUpdateMembershipReqCh       chan *Ticket
	singleUpdateMembershipRegistryMap map[string]*Ticket

	ApplyNewStateSnapshotCh chan *RaftState

	// cktall tracks all our peer replicas AND clients (and observers)
	// and does _not_ include ourself. In
	// contast, replicas only should be kept
	// in cktReplica and peers. The key is a PeerID.
	cktall map[string]*cktPlus

	// same as cktall but by PeerName for fast
	// lookup. also can be pending.
	cktAllByName map[string]*cktPlus

	// parked tracks all circuits for a given peerName,
	// so that multiple circuits to a given remote peerName
	// stay open. To keep them all up,
	// we maintain them all during suppressWatchdogs()
	// if we hear from the remote peer over any of the ckts.
	//
	// The first key is peerName, 2nd key is cktPlus.sn (so we don't loose any).
	parked map[string]map[int64]*cktPlus

	// cktReplica: the key is a PeerID.
	// Partitions ckt above into replica and
	// client ckt. Only replicas get AE/votes.
	//
	// As long as we have membership information,
	// being a member of the cluster is explicitly
	// required to get into cktReplica now.
	//
	// Certain tests, long established, such
	// as election_test.go tests like 020, 021,
	// etc do not setup explicit
	// membership. These are "grandfathered" in
	// and also allowed to join cktReplica.
	// This should never happen in production.
	// It allows simpler testing of elections
	// since we don't have to build up every
	// cluster size from 1 first.
	cktReplica map[string]*cktPlus

	// peers should hole only replica peers (so roles
	// FOLLOWER, CANDIDATE, or LEADER).
	// Do not include ourselves in this set.
	// The key is a PeerID. candidateReinitFollowerInfo()
	// resets this.
	peers map[string]*RaftNodeInfo

	Srv           *rpc.Server `msg:"-"`
	rpcServerAddr net.Addr
	name          string // "tube_node_2"; NOT srvServiceName which is always "tube-replica" or "tube-client"
	srvname       string // "srv_tube_node_2", how srv is named.
	writeReqCh    chan *Ticket

	// re-use the readReqCh for SHOW_KEYS, and READ_KEYRANGE
	// as well as basic READ, READ_PREFIX_RANGE,
	// since they all benefit from leader lease fast reads.
	readReqCh      chan *Ticket
	deleteKeyReqCh chan *Ticket

	newSessionRequestCh   chan *Ticket
	closeSessionRequestCh chan *Ticket
	discoRequestCh        chan *discoReq

	nextWake       time.Time
	nextWakeCh     <-chan time.Time
	nextWakeTicket *Ticket

	// convenience in debug printing
	myid string

	state *RaftState

	saver *raftStatePersistor

	t0                  time.Time
	noFaultTolExpiresTm time.Time
	requestInpsect      chan *Inspection

	haveCheckedAdvanceOnceAfterNoFaultExpired bool

	// Because we had an issue with a "lost" Ticket
	// being deleted prematurely from the Waiting maps,
	// now the tkthist is the authoritative
	// record (mostly append-only -- only trimmed
	// after enough time has elapsed) and the
	// Waiting maps become just fast indexes
	// into tkthist. Thus it is harder to lose
	// Tickets, as we can find them with a linear
	// search if need be, and figure our where
	// we were going wrong. Since clients may need
	// to reconnect and get recent transactions
	// anyway, this design serves that purpose too.
	// (in TubeNode here).
	WaitingAtLeader *imap `msg:"-"` // map[string]*Ticket
	WaitingAtFollow *imap `msg:"-"` // map[string]*Ticket

	// conjecture: ?actually not using these anymore?
	//and might be leaking memory?
	//tkthist  []*Ticket
	//tkthistQ *tkthistQ

	ticketsAwaitingLeader map[string]*Ticket

	debugMsgStat map[int]int

	// serial number assigned to ticket.LeaderStampSN
	// when the leader receives a ticket.
	prevTicketStampSN int64

	// new leader must get this ticket committed before
	// doing anything else...
	// but we don't want to persist it across restarts
	// since it is from an old term, and the point was
	// that we needed to get a commit in the _current leader's term_
	// to not loose configurations;older transactions.
	initialNoop0Tkt *Ticket
	// commitWhatWeCan sets this once initialNoop0Tkt commits.
	initialNoop0HasCommitted bool

	// candidate running an election, tally votes here.
	// initialize in beginElection()
	votes    map[string]bool
	yesVotes int
	noVotes  int

	// log entries. Each entry contains
	// command for state machine, and term
	// when entry was received by leader (first index 1).
	// Note: wal.RaftLog has our log (in memory version)
	wal *raftWriteAheadLog

	// section 6.4, p73, "Processing read-only queries more efficiently"
	readIndexOptim int64 // not really used at the moment.

	// pre-vote state (volatile)

	// My cleaner version on the pre-vote process does
	// two phase election: pre-vote then the same
	// random wait that the leader election does,
	// a second time. So from start of pre-vote
	// until leader elected would be twice as
	// long as normal Raft. This is why we cut
	// the leader election dur from 10x to 5x
	// of heartbeat. Same average election time.
	// Advantages: much less chance of live lock
	// with medium sized clusters on split votes.
	// See safety_test.go 050; especially the
	// 8 and 9 node cluster tests. There
	// is no cross talk between pre-vote and
	// regular voting, guaranteeing regular
	// Raft's correctness guarantees hold.
	// And responders can be stateless with
	// repsect to the pre-vote.

	// a) phase one of pre-vote
	//    note: preVoteOkLeaderElecDeadline is zero,
	//     and  preVoteTerm > 0 during phase 1.
	preVoteTerm int64 // 0 means invalid
	preVotes    map[string]bool
	yesPreVotes int
	noPreVotes  int

	// b) phase two of pre-vote, random wait of [T, 2T]
	//    note: preVoteTerm is zero, and
	//          preVoteOkLeaderElecDeadline is set in phase 2;
	//          preVoteOkLeaderElecTimeoutCh is armed.
	preVoteOkLeaderElecTimeoutCh <-chan time.Time
	preVoteOkLeaderElecDeadline  time.Time
	preVoteOkLeaderElecTerm      int64

	// cached election timeout param,
	// these don't change after startup,
	// so cache rather than recompute.
	cachedMinElectionTimeoutDur time.Duration
	cachedMaxElectionTimeoutDur time.Duration

	// how often our local read optimization
	localReadCount       int64
	totalReadCount       int64
	lastLocalReadLease   time.Duration
	lowestLocalReadLease time.Duration

	// channel stats
	countFrag            int
	countWakeCh          int
	countWriteCh         int
	countReadCh          int
	countDeleteKeyCh     int
	countShowKeysCh      int
	countElections       int
	countLeaderHeartbeat int

	// on recevied AE
	countProdAE int

	// diagnostics
	lastElectionTimeOut   time.Time
	lastElectionTimeCount int

	preVotePhase1Began    time.Time
	preVotePhase1Deadline time.Time
	preVotePhase1EndedAt  time.Time

	preVotePhase2Began   time.Time
	preVotePhase2EndsBy  time.Time
	preVotePhase2EndedAt time.Time

	// redudant cluster info for tube_test 030 basic startup verification
	// production/main code should not depend on these; might get deleted.
	// Actually it turns out not to be redundant, since
	// a follower might take a long time (if ever) to otherwise
	// get a read from another follower, since they are passive--
	// only the leader typically actively contacts peers.
	// Well okay, s.ckt has their circuits too. So yeah, this
	// is kinda redunant. This is the "never deleting" view thought.
	// While s.ckt will delete circuits that go down,
	// verifyPeersSeen does not; so this might be helpful
	// in diagnostics to know anyone we have ever seen. Hence
	// put a timestamp on when we saw them.
	verifyPeersSeen        *rpc.Mutexmap[string, time.Time]
	verifyPeersNeeded      int
	verifyPeersNeededSeen  *idem.IdemCloseChan
	verifyPeerReplicaOrNot chan *cktPlus

	// make Close idempotent
	mut         sync.Mutex
	closeCalled bool

	// dedup of tickets / session support
	ticketPQ *ticketPQ

	// ===== node specific test actions =====
	// election_test
	testBeginElection              chan bool
	testBootstrapLogCh             chan *FirstRaftLogEntryBootstrap
	testBeginPreVote               chan bool
	testGotGoodAE_lastLogIndexNow  chan int64
	testGotAeFromLeader            chan *AppendEntries
	testGotAeAckFromFollower       chan *AppendEntriesAck
	testAEchoices                  []string
	startupNodeUrlSafeCh           *idem.IdemCloseChan
	testWatchdogTimeoutReconnectCh chan time.Time
	// end TubeNode

	// membership config change requests that must
	// be stalled because a prior config change
	// has not yet committed.
	stalledMembershipConfigChangeTkt []*Ticket

	// set to true at the end of a call to
	// setupFirstRaftLogEntryBootstrapLog().
	wroteBootstrapFirstLogEntry time.Time

	// Since we want consensus on when
	// to delete a session, we have to update the
	// session table by SESS_REFRESH
	// and then we can purge those who
	// have not been refreshed or have expired.
	//
	// NOTE: it might seem strange, but
	// we must delete and re-add the
	// entry every time we update the Expiry in
	// order to get the tree to rebalance. Still
	// only O(log N) each time.
	sessByExpiry *sessTableByExpiry

	// try to avoid guesses about if who/I
	// am leader. If this is > 0 and
	// matches s.state.CurrentTerm then
	// we are pretty sure we are currently
	// the leader, since only s.becomeLeader()
	// sets it.
	lastBecomeLeaderTerm int64

	hlc HLC

	// multi-part state snapshot receives
	snapInProgressLastPart        int64
	snapInProgressB3checksumWhole string
	snapInProgressHasher          *blake3.Hasher
	snapInProgress                []byte

	// keep all circuits we spin up goroutine for,
	// to figure out where we are leaking/prune back.
	// This is the per TubeNode version of debugGlobalCkt.
	// key is RemotePeerID
	cktAuditByCID    *rpc.Mutexmap[string, *rpc.Circuit]
	cktAuditByPeerID *rpc.Mutexmap[string, *rpc.Circuit]
}

type discoReq struct {
	who  map[string]string
	done *idem.IdemCloseChan
	err  error
}

// tuber uses to read the DataDir in use.
func (s *TubeNode) GetConfig() TubeConfig {
	return s.cfg
}

func (s *TubeNode) GetPersistorPath() string {
	return s.cfg.DataDir + sep + "persistor.raftstate.tube.msgp"
}

func (s *TubeNode) preVoteDiagString() (r string) {
	r = "\n  preVoteDiagnostics:\n"
	r += fmt.Sprintf("    preVotePhase1Began = '%v'\n", nice(s.preVotePhase1Began))
	r += fmt.Sprintf("   preVotePhase1Deadline = '%v'\n", nice(s.preVotePhase1Deadline))
	r += fmt.Sprintf("  preVotePhase1EndedAt = '%v'\n", nice(s.preVotePhase1EndedAt))
	r += fmt.Sprintf("    preVotePhase2Began = '%v'\n", nice(s.preVotePhase2Began))
	r += fmt.Sprintf("   preVotePhase2EndsBy = '%v'\n", nice(s.preVotePhase2EndsBy))
	r += fmt.Sprintf("  preVotePhase2EndedAt = '%v'\n", nice(s.preVotePhase2EndedAt))
	r += "\n"
	return r
}

func (s *TubeNode) waitingSummary() (sum string, totalTkt int) {
	sum = s.waitingSummaryHelper(true, &totalTkt)
	sum += s.waitingSummaryHelper(false, &totalTkt)
	return
}

func (s *TubeNode) waitingSummaryHelper(atLeader bool, tot *int) (sum string) {
	var n int
	var waiting *imap
	if atLeader {
		waiting = s.WaitingAtLeader
		n = s.WaitingAtLeader.Len()
		if n == 0 {
			return "(empty WaitingAtLeader)"
		}
		sum = "WaitingAtLeader: "
	} else {
		waiting = s.WaitingAtFollow
		n = s.WaitingAtFollow.Len()
		if n == 0 {
			return "(empty WaitingAtFollow)"
		}
		sum = "WaitingAtFollow: "
	}
	*tot += n

	sum += fmt.Sprintf("(%v tkt waiting)\n", n)

	const maxShow = 2
	if true { // n < 2 {
		// we ony print the first 2 now...

		// sort into log order
		var orderedTickets []*Ticket
		for _, tkt := range waiting.all() {
			orderedTickets = append(orderedTickets, tkt)
		}
		sort.Sort(logOrder(orderedTickets))
		extra := ""

		var star string // "*" for commited, "_" for uncommited.
		for i, tkt := range orderedTickets {
			if i >= maxShow {
				break
			}
			if tkt.Committed {
				star = "*"
			} else {
				star = "_"
			}
			sum += fmt.Sprintf("___ %v[%v%v %v] '%v'\n", extra, tkt.LogIndex, star, tkt.TicketID[:4], tkt.Desc)
			if i == 0 {
				//extra = "; "
			}
		}
	} else {
		// not showing any ticket contents... slows down the process.
	}
	sum += ")"
	return
}

func (s *TubeNode) Name() string {
	return s.name
}

func (s *TubeNode) me() string {

	//if s.name != "node_0" { // filter for just node_0 for debugging
	//	return s.name
	//}

	lead := s.leaderName
	if lead == "" {
		lead = "no"
		if s.state.VotedFor != "" {
			lead = fmt.Sprintf("voted for %v", s.state.VotedForName)
		}
	}
	waiting, totalTkt := s.waitingSummary()
	waiting = fmt.Sprintf("[totalTkt %v] %v", totalTkt, waiting)

	awal := ""
	nawal := len(s.ticketsAwaitingLeader) // different from WaitingAtLeader!
	if nawal > 0 {
		awal = fmt.Sprintf(" ticketsAwaitingLeader[%v]{", nawal)
		for _, tkt := range s.ticketsAwaitingLeader {
			awal += fmt.Sprintf("%v,", tkt.Desc)
		}
		awal += "}"
	} else {
		awal = "(empty ticketsAwaitingLeader)"
	}
	var membr string
	var numMember int
	if s.state != nil && s.state.MC != nil {
		membr, numMember = s.memberNewestShortString()
	}
	var shadow string
	var numShadow int
	if s.state != nil && s.state.ShadowReplicas != nil {
		shadow, numShadow = s.shadowReplicasShortString()
	}

	var mcStall string
	nMCstall := len(s.stalledMembershipConfigChangeTkt)
	if nMCstall > 0 {
		mcStall = fmt.Sprintf("\ns.stalledMembershipConfigChangeTkt[%v]{\n", nMCstall)
		for _, mc := range s.stalledMembershipConfigChangeTkt {
			mcStall += fmt.Sprintf("%v\n", mc.Short())
		}
	}
	var mcVers string
	if s.state != nil && s.state.MC != nil {
		mcVers = s.state.MC.Vers()
	}
	return fmt.Sprintf("%v%v[term %v][ci %v][lli %v][%v lead] membr[%v]%v:{%v} shadow[%v]{%v} %v%v%v\n===\n==\n= ", s.myid, s.role.String(), s.state.CurrentTerm, s.state.CommitIndex, s.lastLogIndex(), lead, numMember, mcVers, membr, numShadow, shadow, waiting, awal, mcStall)
}

func (s *TubeNode) shadowReplicasShortString() (membr string, numMember int) {
	numMember = s.state.ShadowReplicas.PeerNames.Len()
	i := 0
	for peerName := range s.state.ShadowReplicas.PeerNames.All() {
		if i > 0 {
			membr += ", "
		}
		membr += peerName
		up := "-" // down by default
		if peerName == s.name {
			up = "*"
		} else {
			cktP, ok := s.cktAllByName[peerName]
			if ok {
				if cktP.isPending() {
					up = "p"
				} else {
					up = "+"
					if s.role == LEADER {
						if cktP.up() {
							up = "*"
						}
					}
				}
			}
		}
		membr += up
		i++
	}
	return
}

func (s *TubeNode) memberNewestShortString() (membr string, numMember int) {
	numMember = s.state.MC.PeerNames.Len()
	i := 0
	//down := s.followerDownDur()
	for peerName := range s.state.MC.PeerNames.All() {
		if i > 0 {
			membr += ", "
		}
		membr += peerName
		up := "-" // down by default
		if peerName == s.name {
			up = "*"
		} else {
			cktP, ok := s.cktAllByName[peerName]
			if ok {
				if cktP.isPending() {
					up = "p"
				} else {
					up = "+"
					if s.role == LEADER {
						if cktP.up() {
							up = "*"
						}
					}
				}
			}
		}
		membr += up
		i++
	}
	return
}

// AppendEntries
// Raft: invoked by leader to replicate log
// entries (section 3.5), also for heartbeats (section 3.4).
type AppendEntries struct {
	ClusterID                  string `zid:"0"`
	FromPeerID                 string `zid:"1"`
	FromPeerName               string `zid:"2"`
	FromPeerServiceName        string `zid:"3"`
	FromPeerServiceNameVersion string `zid:"4"`

	// leader's term
	LeaderTerm int64 `zid:"5"`

	// so follower can redirect clients
	LeaderID   string `zid:"6"`
	LeaderName string `zid:"7"`
	LeaderURL  string `zid:"8"`

	// index of log entry immediately preceeding new ones
	PrevLogIndex int64 `zid:"9"`

	// term of prevLogIndex entry
	PrevLogTerm int64 `zid:"10"`

	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency).
	Entries []*RaftLogEntry `zid:"11"`

	// Leader's commitIndex
	LeaderCommitIndex          int64 `zid:"12"`
	LeaderCommitIndexEntryTerm int64 `zid:"13"`

	LogTermsRLE *TermsRLE `zid:"14"`

	AEID string `zid:"15"`

	// provide these so even heartbeats to new
	// members who have no log yet can immediately
	// enable the new joiners to discover the cluster:

	// MC is the sender's newest MemberConfig. In
	// fact it used to be called NewestMemberConfig
	// but that was alot.
	MC *MemberConfig `zid:"16"`

	// Send shadows too to try and keep them in sync with MC.
	ShadowReplicas *MemberConfig `zid:"17"`

	PeerID2LastHeard map[string]time.Time `zid:"18"`

	LeaderLLI int64 `zid:"19"` // leader last log index.

	// what is on disk/in the wal might be different
	// from the leader's current Term; if noop0 has
	// not committed yet.
	LeaderLLT int64 `zid:"20"` // leader last log index term.

	LeaderCompactIndex int64 `zid:"21"`
	LeaderCompactTerm  int64 `zid:"22"`

	LeaderHLC HLC `zid:"23"`
}

func (ae *AppendEntries) clone() (p *AppendEntries) {
	if ae == nil {
		return nil
	}
	cp := *ae
	p = &cp
	p.Entries = cloneRaftLogEntries(ae.Entries)
	p.LogTermsRLE = ae.LogTermsRLE.clone()

	p.MC = ae.MC.Clone()

	p.PeerID2LastHeard = make(map[string]time.Time)
	for k, v := range ae.PeerID2LastHeard {
		p.PeerID2LastHeard[k] = v
	}
	return
}

func cloneRaftLogEntries(orig []*RaftLogEntry) (c []*RaftLogEntry) {
	for _, o := range orig {
		c = append(c, o.clone())
	}
	return
}

// AppendEntriesAck
type AppendEntriesAck struct {
	ClusterID                  string `zid:"0"`
	FromPeerID                 string `zid:"1"`
	FromPeerName               string `zid:"2"`
	FromPeerServiceName        string `zid:"3"`
	FromPeerServiceNameVersion string `zid:"4"`

	// Current term, for leader to update itself.
	Term int64 `zid:"5"`

	// false if follower contained entry matching
	// prevLogIndex and prevLogTerm. else successfully applied.
	Rejected bool `zid:"6"`

	LogsMatchExactly bool `zid:"7"`

	// LargestCommonRaftIndex
	// enables matching logs with minimal network traffic.
	// this is the longest common prefix of the
	// leader and this follower's log.
	// Mind the gap, though. (See rle.go)
	LargestCommonRaftIndex int64 `zid:"8"`

	// if Rejected, details per page 21:
	// "For example, when rejecting an AppendEntries request,
	// the follower can include the term of the conflicting
	// entry and the first index it stores for that term.
	ConflictTerm         int64 `zid:"9"`
	ConflictTerm1stIndex int64 `zid:"10"`
	// With Rejected, we return -1 if our
	// log is too short; this gives the leader our log length,
	// and first index.
	PeerLogCompactIndex int64 `zid:"11"`
	PeerLogCompactTerm  int64 `zid:"12"`
	PeerLogFirstIndex   int64 `zid:"13"`
	PeerLogFirstTerm    int64 `zid:"14"`
	PeerLogLastIndex    int64 `zid:"15"`
	PeerLogLastTerm     int64 `zid:"16"`

	// reproduce part of the AppendEntries
	// request, to re-establish context
	// since server state might have
	// changed in the meantime, as we are
	// not RPC based.
	SuppliedPrevLogIndex      int64     `zid:"17"`
	SuppliedPrevLogTerm       int64     `zid:"18"`
	SuppliedEntriesIndexBeg   int64     `zid:"19"`
	SuppliedEntriesIndexEnd   int64     `zid:"20"`
	SuppliedLeaderCommitIndex int64     `zid:"21"`
	SuppliedLeader            string    `zid:"22"`
	SuppliedLeaderName        string    `zid:"23"`
	SuppliedLeaderTermsRLE    *TermsRLE `zid:"24"`
	SuppliedLeaderLastTerm    int64     `zid:"25"`
	SuppliedLeaderLLI         int64     `zid:"26"`
	SuppliedLeaderLLT         int64     `zid:"27"`

	PeerLogTermsRLE *TermsRLE `zid:"28"`
	RejectReason    string    `zid:"29"`
	AEID            string    `zid:"30"`

	MinElectionTimeoutDur time.Duration `zid:"31"`
	SpanMatching          *LogEntrySpan `zid:"32"`

	PeerMC *MemberConfig `zid:"33"`

	PeerCompactionDiscardedLastIndex   int64 `zid:"34"`
	PeerCompactionDiscardedLastTerm    int64 `zid:"35"`
	NeedSnapshotGap                    bool  `zid:"36"`
	SuppliedLeaderCommitIndexEntryTerm int64 `zid:"37"`
	SuppliedCompactIndex               int64 `zid:"38"`
	SuppliedCompactTerm                int64 `zid:"39"`
	FollowerHLC                        HLC   `zid:"40"`
}

func (ack *AppendEntriesAck) clone() (p *AppendEntriesAck) {
	if ack == nil {
		return nil
	}
	cp := *ack
	p = &cp
	p.SuppliedLeaderTermsRLE = ack.SuppliedLeaderTermsRLE.clone()
	p.PeerLogTermsRLE = ack.PeerLogTermsRLE.clone()
	p.SpanMatching = ack.SpanMatching.clone()
	p.PeerMC = ack.PeerMC.Clone()
	return
}

// RequestVote
// Raft: invoked by candidates to gather votes.
type RequestVote struct {
	ClusterID                  string `zid:"0"`
	FromPeerID                 string `zid:"1"`
	FromPeerName               string `zid:"2"`
	FromPeerServiceName        string `zid:"3"`
	FromPeerServiceNameVersion string `zid:"4"`

	// candidate's term
	CandidatesTerm int64 `zid:"5"`

	// candidate requesting vote
	CandidateID string `zid:"6"`

	// index of candidates last log entry
	LastLogIndex int64 `zid:"7"`

	// term of candidates last log entry
	LastLogTerm int64 `zid:"8"`

	// Whether this is a pre-vote request
	IsPreVote bool `zid:"9"`

	// Over-ride leader stickiness on
	// leadership transfer, section 3.10
	// and section 4.2.3 page 42
	LeadershipTransferFrom string `zid:"10"`

	Weight float64 `zid:"11"` // TODO: implement vote weighting

	MC *MemberConfig `zid:"12"`

	SenderHLC HLC `zid:"13"`
}

// Vote is the response to RequestVote.
type Vote struct {
	ClusterID                  string `zid:"0"`
	FromPeerID                 string `zid:"1"`
	FromPeerName               string `zid:"2"`
	FromPeerServiceName        string `zid:"3"`
	FromPeerServiceNameVersion string `zid:"4"`
	FromPeerCurrentTerm        int64  `zid:"5"`
	FromPeerCurrentLLI         int64  `zid:"6"`
	FromPeerCurrentLLT         int64  `zid:"7"`

	CandidateID string `zid:"8"`

	// currentTerm, for candidate to update itself
	CandidatesTerm int64 `zid:"9"`

	// true means candidate received vote
	VoteGranted bool `zid:"10"`

	// Whether this is a pre-vote response
	IsPreVote bool `zid:"11"`

	// why not send during vote so they
	// already know what we need.
	PeerLogTermsRLE *TermsRLE `zid:"12"`

	Reason string `zid:"13"`

	MC *MemberConfig `zid:"14"`

	SenderHLC HLC `zid:"15"`
}

func (s *RequestVote) String() (r string) {

	r = "&RequestVote{\n"
	r += fmt.Sprintf("             ClusterID: \"%v\",\n", rpc.AliasDecode(s.ClusterID))
	r += fmt.Sprintf("            FromPeerID: \"%v\",\n", rpc.AliasDecode(s.FromPeerID))
	r += fmt.Sprintf("          FromPeerName: \"%v\",\n", s.FromPeerName)
	r += fmt.Sprintf("   FromPeerServiceName: \"%v\",\n", s.FromPeerServiceName)
	r += fmt.Sprintf("        CandidatesTerm: %v,\n", s.CandidatesTerm)
	r += fmt.Sprintf("           CandidateID: \"%v\",\n", rpc.AliasDecode(s.CandidateID))
	r += fmt.Sprintf("          LastLogIndex: %v,\n", s.LastLogIndex)
	r += fmt.Sprintf("           LastLogTerm: %v,\n", s.LastLogTerm)
	r += fmt.Sprintf("             IsPreVote: %v,\n", s.IsPreVote)
	r += fmt.Sprintf("LeadershipTransferFrom: \"%v\",\n", rpc.AliasDecode(s.LeadershipTransferFrom))
	r += fmt.Sprintf("  MC: \"%v\",\n", s.MC.Short())
	r += "}"
	return
}

// A simple wrapper header on all msgpack messages;
// has the length and the bytes.
// Allows us length delimited messages; with
// length knowledge up front.
type ByteSlice []byte

type TubeCluster struct {
	TestName               string
	Cfg                    *TubeConfig
	Nodes                  []*TubeNode
	LeaderElectedCh        chan string
	LeaderNoop0committedCh chan string
	// election_test
	NoInitialLeaderTimeout bool
	Halt                   *idem.Halter `msg:"-"`
	Name2num               map[string]int
	Snap                   *rpc.SimnetSnapshotter `msg:"-"`
	termChanges            chan *testTermChange

	BootMC *MemberConfig
}

// make a test cluster
func NewCluster(testName string, cfg *TubeConfig) (cluster *TubeCluster) {

	// NB: moved rpcCfg setup to NewTubeConfig() ending cfg.Init() call.
	//
	// The nodes are made below in the NewTubeNode() calls.

	cluster = &TubeCluster{
		Halt:     idem.NewHalter(),
		TestName: testName,
		Cfg:      cfg,
		// important to make this buffered to max
		// possible elections checked for in one test,
		// so we don't miss one.
		LeaderElectedCh:        make(chan string, TEST_CHAN_CAP),
		LeaderNoop0committedCh: make(chan string, TEST_CHAN_CAP),
		Name2num:               make(map[string]int),
		termChanges:            make(chan *testTermChange, TEST_CHAN_CAP),
	}

	// let nodes report things like leader election.
	cfg.testCluster = cluster

	if cfg.Node2Addr == nil {
		cfg.Node2Addr = make(map[string]string)
	}
	for i := range cfg.ClusterSize { // in NewCluster; make new test cluster
		name := fmt.Sprintf("node_%v", i)
		cluster.Name2num[name] = i
		node := NewTubeNode(name, cfg)
		cluster.Nodes = append(cluster.Nodes, node)
		cluster.Halt.AddChild(node.Halt)

		// try to get an initial MC for 059/402/403?
		cfg.Node2Addr[name] = name
	}

	return
}

func (s *TubeCluster) AllHealthyAndPowerOn(deliverDroppedSends bool) {
	s.AllHealthy(true, deliverDroppedSends)
}

func (s *TubeCluster) SimnetSnapshot(skipTraffic bool) *rpc.SimnetSnapshot {
	if !s.Cfg.UseSimNet {
		panic("simnet not in use!")
		return nil
	}
	if s.Snap == nil {
		s.Snap = s.Cfg.RpcCfg.GetSimnetSnapshotter()
	}
	return s.Snap.GetSimnetSnapshot(skipTraffic)
}

func (s *TubeCluster) NoisyNothing(oldval, newval bool) (swapped bool) {
	simnet := s.Cfg.RpcCfg.GetSimnet()
	return simnet.NoisyNothing(oldval, newval)
}

func (s *TubeCluster) AllHealthy(powerOnAnyOff, deliverDroppedSends bool) {
	if !s.Cfg.UseSimNet {
		panic("simnet not in use!")
		return
	}
	simnet := s.Cfg.RpcCfg.GetSimnet()
	if simnet == nil {
		panic("AllHealthy is a no-op without simnet")
	}
	err := simnet.AllHealthy(powerOnAnyOff, deliverDroppedSends)
	panicOn(err)
}

func (s *TubeCluster) IsolateNode(i int) {
	if !s.Cfg.UseSimNet {
		panic("simnet not in use!")
		return
	}
	simnet := s.Cfg.RpcCfg.GetSimnet()
	if simnet == nil {
		panic("IsolateNode is a no-op without simnet")
	}

	simnet.AlterHost(s.Nodes[i].srvname, rpc.ISOLATE)
}

// deaf reads in the deaf set, node number:probability of deaf read.
// dropped sends in the drop set, node number:probability of dropped send.
func (s *TubeCluster) DeafDrop(deaf, drop map[int]float64) {
	if !s.Cfg.UseSimNet {
		panic("simnet not in use!")
		return
	}
	for w, v := range deaf {
		node := s.Nodes[w]
		node.DeafToReads(v)
	}
	for w, v := range drop {
		node := s.Nodes[w]
		node.DropSends(v)
	}
}
func (s *TubeNode) DeafToReads(probDeaf float64) {
	if !s.cfg.UseSimNet {
		panic("simnet not in use!")
		return
	}

	simnet := s.cfg.RpcCfg.GetSimnet()
	if simnet == nil {
		panic("DeafToReads is a no-op without simnet")
	}
	dd := rpc.DropDeafSpec{
		UpdateDeafReads:  true,
		UpdateDropSends:  false,
		DeafReadsNewProb: probDeaf,
		DropSendsNewProb: 0,
	}
	const deliverDroppedSends = false
	err := simnet.FaultHost(s.srvname, dd, deliverDroppedSends)
	panicOn(err)
}

func (s *TubeNode) DropSends(probDrop float64) {
	if !s.cfg.UseSimNet {
		panic("simnet not in use!")
		return
	}
	simnet := s.cfg.RpcCfg.GetSimnet()
	if simnet == nil {
		panic("DropSends is a no-op without simnet")
	}
	dd := rpc.DropDeafSpec{
		UpdateDeafReads:  false,
		UpdateDropSends:  true,
		DeafReadsNewProb: 0,
		DropSendsNewProb: probDrop,
	}
	const deliverDroppedSends = false
	err := simnet.FaultHost(s.srvname, dd, deliverDroppedSends)
	panicOn(err)
}

func (s *TubeNode) SimCrash() {
	if !s.cfg.UseSimNet {
		panic("simnet not in use!")
		return
	}

	simnet := s.cfg.RpcCfg.GetSimnet()
	if simnet == nil {
		panic("SimCrash is a no-op without simnet")
	}

	// we made this remove the node from the simnet, but
	// then we cannot see its data. but more
	// realistic testing of restart, so lets prefer it.
	// call TubeNode.Close():
	s.CloseWithReason(fmt.Errorf("SimCrash"))
	// does: s.Srv.Close()

}

func (cluster *TubeCluster) SimBoot(i int) {
	if !cluster.Cfg.UseSimNet {
		panic("simnet not in use!")
		return
	}

	old := cluster.Nodes[i]
	simnet := old.cfg.RpcCfg.GetSimnet()
	if simnet == nil {
		panic("SimBoot is a no-op without simnet")
	}

	// we just replace the cluster slot and
	// let the GC collect any parts it can.

	s2 := NewTubeNode(old.name, &old.cfg)
	cluster.Nodes[i] = s2
	cluster.Halt.AddChild(s2.Halt)

	// verify simnet is intact
	simnet2 := s2.cfg.RpcCfg.GetSimnet()
	if simnet2 != simnet {
		panic(fmt.Sprintf("problem: simnet not preserved! simnet2=%p simnet=%p", simnet2, simnet))
	}

	//vv("old.MC = %v", old.state.MC)
	//vv("old.ShadowReplicas = %v", old.state.ShadowReplicas)
	//vv("len old.wal.raftLog = %v", len(old.wal.raftLog))
	// give s2 the same memwal log from old:
	s2.wal = old.wal
	s2.state = old.state

	alwaysPrintf("SimBoot is about to start node i=%v name='%v'; MC='%v'", i, s2.name, s2.state.MC)
	// starts server and local peer
	err := s2.InitAndStart()
	panicOn(err)

}

func (s *TubeCluster) Start() {

	// If we need to call newFrag, call it on a live node, not
	// dead on start; else 024 test will seg fault.
	var live *TubeNode
	for i, n := range s.Nodes {
		if i < s.Cfg.deadOnStartCount {
			// leave this node off, 024 election test.
			n.shutdown()
			continue
		}
		err := n.InitAndStart()
		panicOn(err)
		live = n
	}
	sz := len(s.Nodes)
	if sz == 0 {
		return
	}
	_ = live
	//vv("cluster has %v nodes", sz)

	var ckts []*rpc.Circuit

	// now that they are all started, form a complete mesh
	// by connecting each to all the others
	bkgCtx := context.Background()
	for i, n0 := range s.Nodes {
		if i < s.Cfg.deadOnStartCount {
			// leave this node off, 024 election test.
			continue
		}
		for j, n1 := range s.Nodes {
			if j < s.Cfg.deadOnStartCount {
				// leave this node off, 024 election test.
				continue
			}
			if j <= i || i == sz-1 {
				// avoids making redundant connections.
				continue
			}

			// tell a fresh client to connect to server and then pass the
			// conn to the existing server.

			//vv("about to connect i=%v to j=%v", i, j)
			firstFrag := n0.newFrag()
			// firstFrag.FragOp = 0 // leave 0
			firstFrag.ToPeerName = n1.name
			// client might also, don't assume replica here
			//firstFrag.SetUserArg("isTubeReplica", "true")

			firstFrag.SetUserArg("fromPeerName", n0.name)
			firstFrag.SetUserArg("toPeerName", n1.name)

			//vv("n1.URL = '%v'", n1.URL)
			//ckt, _, err := n0.MyPeer.NewCircuitToPeerURL("tube-ckt", n1.URL, firstFrag, 0)
			var peerServiceNameVersion string
			// here we are in TubeCluster.Start()
			ckt, ackMsg, madeNewAutoCli, onlyPossibleAddr, err := n0.MyPeer.PreferExtantRemotePeerGetCircuit(bkgCtx, "tube-ckt", firstFrag, string(TUBE_REPLICA), peerServiceNameVersion, n1.URL, 0, nil, waitForAckTrue)
			_ = onlyPossibleAddr
			if err != nil {
				// don't freak out on shutdown
				alwaysPrintf("%v shutdown on err='%v'", n0.name, err)
				return
			}
			_ = ackMsg
			_ = madeNewAutoCli

			//vv("ckt.RpbTo.NetAddr = '%v'", ckt.RpbTo.NetAddr)

			ckts = append(ckts, ckt)

			// must manually tell the service goro about the new ckt in this case.
			n0.MyPeer.NewCircuitCh <- ckt
			//vv("created ckt between n0 '%v' and n1 '%v': '%v'", n0.name, n1.name, ckt.String())
		}
	}

}

func (s *TubeCluster) Close() {
	//vv("TubeCluser.Close()")
	// lots of redundancy but meh. getter done.
	net := s.Cfg.RpcCfg.GetSimnet()
	if net != nil {
		//vv("calling simnet.Close()")
		net.Close()
		//vv("back from calling simnet.Close()")
	}
	s.Halt.ReqStop.Close()
	for _, n := range s.Nodes {
		n.Close()
	}
	s.Halt.RootReqStopClose(ErrShutDown)
	if net != nil {
		// try to prevent the 500msec halter timeouts from clashing with
		// the synctest bubble exit... otherwise we get:
		// panic: deadlock: main bubble goroutine has exited but blocked goroutines remain
		time.Sleep(10 * time.Second)
		vv("cluster.Close slept 10 sec")
	}
}

func (s *TubeNode) Close() {
	//vv("TubeNode.Close() '%v'/'%v'", s.name, s.PeerID)
	s.Halt.ReqStop.Close()
	<-s.Halt.Done.Chan
}

func (s *TubeNode) CloseWithReason(reason error) {
	//vv("TubeNode.Close() '%v'/'%v'", s.name, s.PeerID)
	s.Halt.ReqStop.CloseWithReason(reason)
	<-s.Halt.Done.Chan
}

func (s *TubeNode) shutdown() {
	//vv("%v shutdown; started=%v; cluster: '%v'", s.name, s.started, s.cfg.ClusterID)

	for _, ckt := range s.cktall {
		ckt.ckt.LpbFrom.Close()
	}
	s.Halt.ReqStop.Close()
	s.Halt.Done.Close() // essential so s.srv's StopTree can finish.

	// if we never got started, we might not have s.srv,
	// as in election_test 024.
	if s.Srv != nil {
		s.Srv.Close() // already does StopTree, and we are child.
	}
}

func (s *RaftNodeInfo) clone() (p *RaftNodeInfo) {
	if s == nil {
		return nil // can happen for peers we have no info from yet.
	}
	cp := *s
	p = &cp

	p.MC = s.MC.Clone()
	return
}

type ClusterMembership struct {
	Old     map[string]*RaftNodeInfo // C_old servers
	New     map[string]*RaftNodeInfo // C_new servers
	IsJoint string                   // "stable" or "joint"
}

func newClusterMembership() *ClusterMembership {
	return &ClusterMembership{
		Old: make(map[string]*RaftNodeInfo),
		New: make(map[string]*RaftNodeInfo),
	}
}

// A Blake3sum checksum follows each DiskState record,
// provide a cryptographic hash checksum of the
// serialized DiskState to catch corruption.
type Blake3sum struct {
	// prefix: "blake3.33B-"
	// generated with blake3ToString33B() below.
	Sum33B      string `zid:"0"`
	Sum33Bchain string `zid:"1"`
}

// avoid serializing the full CurState 2x, since
// the messages about to be sent have the full state on them.
func (d *RaftLogEntry) PreSaveHook() {
}

func (d *RaftLogEntry) PostLoadHook() {
}

func (t *Ticket) clone() *Ticket {
	cp := *t
	//cp.Key = append(Key([]byte{}), t.Val...)
	cp.Val = append(Val([]byte{}), t.Val...)
	cp.OldVal = append(Val([]byte{}), t.OldVal...)
	cp.CASRejectedBecauseCurVal = append(Val([]byte{}), t.CASRejectedBecauseCurVal...)
	// cp.Done?
	return &cp
}

func (t *Ticket) PreSaveHook() {
	if t.Err != nil {
		t.Errs = t.Err.Error()
	}
}

func (t *Ticket) PostLoadHook() {
	if t.Errs != "" {
		t.Err = errors.New(t.Errs)
	}
}

// NewTubeNode creates a new Tube node
// name = "node_0", "node_1", "node_2", ...
// cfg.PeerServiceName = "tube-replica" (typically); or "tube-client"
func NewTubeNode(name string, cfg *TubeConfig) *TubeNode {

	hostname, err := os.Hostname()
	panicOn(err)

	if cfg.PeerServiceName == "" {
		panic(`cfg.PeerServiceName must be set; usually to "tube-replica" but could also be "tube-client" to just send read/write requests.`)
	}

	if !cfg.initCalled {
		panic("must call cfg.Init() before using a TubeConfig.")
	}
	if hasWhiteSpace(name) {
		panic(fmt.Sprintf("name '%v' cannot have whitespace, as it will be in the log path", name))
	}

	if !cfg.RpcCfg.CompressionOff {
		cfg.RpcCfg.CompressionOff = true
		if !cfg.isTest {
			//alwaysPrintf("we set cfg.RpcCfg.CompressionOff = true to keep memory use low.")
		}
	}

	if cfg.ClusterID == "" {
		panic("must have clusterID")
		// client wants us to pick one.
		// clusterID = rpc.NewCallID("")
		// for recovery, should we be trying to
		// read this from the config dir? at the
		// moment that would make a mess of
		// parallel tests, so omit for now.
		//
		// but yes, we want the ClusterID to
		// help guarantee that ballot numbers
		// are not re-used.
	}

	s := &TubeNode{
		cfg:             *cfg, // must make our own copy to avoid cross talk with other nodes
		hostname:        hostname,
		Ctx:             context.Background(), // placeholder until MyPeer.Ctx
		name:            name,
		PeerServiceName: cfg.PeerServiceName,
		ClusterID:       cfg.ClusterID,

		// client Tickets that are waiting for logs to replicate.
		WaitingAtLeader: newImap(),
		WaitingAtFollow: newImap(),
		//tkthistQ:        newTkthistQ(),

		cktall:       make(map[string]*cktPlus), // by PeerID
		cktAllByName: make(map[string]*cktPlus),

		// explicit BootstrapRegisterAsReplica now
		// required to go from ckt to cktReplica now.
		cktReplica: make(map[string]*cktPlus),

		ticketPQ: newTicketPQ(name),

		// comms
		pushToPeerURL:  make(chan string),
		Halt:           idem.NewHalter(),
		writeReqCh:     make(chan *Ticket),
		readReqCh:      make(chan *Ticket),
		deleteKeyReqCh: make(chan *Ticket),

		newSessionRequestCh:   make(chan *Ticket),
		closeSessionRequestCh: make(chan *Ticket),
		discoRequestCh:        make(chan *discoReq),
		requestInpsect:        make(chan *Inspection),
		peerLeftCh:            make(chan string),

		// bad if buffer=> 707 red no leader
		setLeaderCktChan: make(chan *rpc.Circuit),

		peers:                 make(map[string]*RaftNodeInfo),
		ticketsAwaitingLeader: make(map[string]*Ticket),
		debugMsgStat:          make(map[int]int),

		requestRemoteInspectCh:   make(chan *inspectionTicket),
		remoteInspectRegistryMap: make(map[string]*inspectionTicket),

		singleUpdateMembershipReqCh:       make(chan *Ticket),
		singleUpdateMembershipRegistryMap: make(map[string]*Ticket),
		ApplyNewStateSnapshotCh:           make(chan *RaftState),

		leaderFullPongPQ: newPongPQ(name),
		parked:           newParkedTree(),
		sessByExpiry:     newSessTableByExpiry(),
		cktAuditByCID:    rpc.NewMutexmap[string, *rpc.Circuit](),
		cktAuditByPeerID: rpc.NewMutexmap[string, *rpc.Circuit](),
	}
	s.hlc.CreateSendOrLocalEvent()
	s.cfg.MyName = name
	// non-testing default is talk to ourselves.
	// simae_test will switch this to its TubeSim instance.
	s.host = s

	if cfg.isTest {
		// avoid allocating all this for the 800K simae runs
		// that don't use it. Makes them very slow.
		if cfg.testNum != 802 {
			// clusterup_test 030 stuff. here in NewTubeNode test stuff
			s.verifyPeersNeeded = cfg.ClusterSize - 1
			s.verifyPeersNeededSeen = idem.NewIdemCloseChan()
			s.verifyPeersSeen = rpc.NewMutexmap[string, time.Time]()
			n2 := 100 + cfg.ClusterSize*cfg.ClusterSize*10 // *10 b/c full grid instead of half-upper-triangle does more e.g. 401_add_node membership_test. ugh. when 401 hits this now I think it indicates a real problem.
			s.verifyPeerReplicaOrNot = make(chan *cktPlus, n2)
			// election_test stuff.
			s.testBootstrapLogCh = make(chan *FirstRaftLogEntryBootstrap, 1)
			s.testBeginElection = make(chan bool, TEST_CHAN_CAP)
			s.testBeginPreVote = make(chan bool, TEST_CHAN_CAP)
			s.testGotGoodAE_lastLogIndexNow = make(chan int64, TEST_CHAN_CAP)
			s.testGotAeFromLeader = make(chan *AppendEntries, TEST_CHAN_CAP)
			s.testGotAeAckFromFollower = make(chan *AppendEntriesAck, TEST_CHAN_CAP)
			s.testWatchdogTimeoutReconnectCh = make(chan time.Time, TEST_CHAN_CAP)
		}
	}
	// InitAndStart seems to always want this
	//s.startupNodeUrlSafeCh = make(chan struct{})
	s.startupNodeUrlSafeCh = idem.NewIdemCloseChan()

	s.state = s.newRaftState()

	if s.cfg.DataDir == "" {
		var err error
		s.cfg.DataDir, err = GetServerDataDir()
		panicOn(err)
		// bad: separate log for every new process!? ugh.
		//s.cfg.DataDir += sep + cfg.ClusterID + sep + nodeID
		//
		// instead, have a rebooted "node_3" use the prior
		// wal of that the previous "node_3" wrote:
		s.cfg.DataDir += sep + cfg.ClusterID + sep + s.name
	}

	// config dir applies options to all instances
	// of tube.
	if s.cfg.ConfigDir == "" {
		s.cfg.ConfigDir = GetConfigDir()
	}

	//zz("%v NewTubeNode: config dir ='%v'; data dir = '%v'", s.name, s.cfg.ConfigDir, s.cfg.DataDir)

	return s
}

func (e *RaftLogEntry) Short() string {
	if e == nil {
		return "(nil RaftLogEntry)"
	}
	return fmt.Sprintf("RaftLogEntry{Term:%02d, Index:%02d, Ticket: %v, CurrentCommitIndex:%v, Ticket.Op=%v tkt4=%v Ticket.Desc: %v}", e.Term, e.Index, e.Ticket.TicketID, e.CurrentCommitIndex, e.Ticket.Op, e.Ticket.TicketID[:4], e.Ticket.Desc)
}

func (s *TubeNode) viewCurLog() (err error) {
	alwaysPrintf("%v viewCurLog is happening; s.cfg.NoDisk = %v; s.wal.LogicalLen = %v", s.name, s.cfg.NoDisk, s.wal.LogicalLen())
	if s.cfg.NoDisk {
		// nothing touches disk, at the moment,
		// and its nice to see the RaftLog print below.
	}

	//s.wal.RaftLog = s.wal.entry
	//vv("%v loaded %v entries from the disk log", s.name, len(s.wal.RaftLog))

	var i int
	var e *RaftLogEntry
	for i, e = range s.wal.raftLog {
		// count from 1 to match the Raft log Index.
		fmt.Printf("i=%03d  %v\n", i+1, e.Short())
	}
	if len(s.wal.raftLog) == 0 {
		fmt.Printf("(empty Raft wal log)\n")
	}

	return
}

func (s *TubeNode) newRaftState() *RaftState {

	sn := atomic.AddInt64(&debugSerialRaftState, 1)

	a := &RaftState{
		name:            s.name,
		Serial:          sn,
		PeerID:          s.PeerID,
		PeerName:        s.name,
		PeerServiceName: s.PeerServiceName,
		ClusterID:       s.ClusterID,
		KVstore:         newKVStore(),
		SessTable:       make(map[string]*SessionTableEntry),

		// try to prevent seg fault in 401
		// where Vote.MC was nil. Ugh, then ClusterSize() returns 0, so
		// wait to init MC: s.NewMemberConfig("newRaftState", false),
		Known:          s.NewMemberConfig("newRaftState"),
		Observers:      s.NewMemberConfig("newRaftState"),
		ShadowReplicas: s.NewMemberConfig("newRaftState"),
	}
	return a
}

func copyHashMap(m map[Key]Val) (r map[Key]Val) {
	r = make(map[Key]Val)
	for k, v := range m {
		r[k] = append(Val{}, v...)

	}
	return
}

// StartClientOnly is used by the tube command to issue
// remove peer commands, updating the membership.
// Since a server isn't needed, we skip starting one.
func (s *TubeNode) StartClientOnly(ctx context.Context, dialto string) (cli *rpc.Client, err error) {

	s.cfg.RpcCfg.ClientDialToHostPort = dialto
	cli, err = rpc.NewClient(s.name, s.cfg.RpcCfg)
	panicOn(err)
	err = cli.Start()
	if err != nil {
		return
	}

	err = cli.PeerAPI.RegisterPeerServiceFunc(string(s.PeerServiceName), s.Start)
	panicOn(err)

	var peerServiceNameVersion string
	_, err = cli.PeerAPI.StartLocalPeer(ctx, string(s.PeerServiceName), peerServiceNameVersion, nil, s.name, true)
	panicOn(err)

	select {
	case <-s.startupNodeUrlSafeCh.Chan:
		//vv("TubeNode.InitAndStart end: node '%v' has started, at url '%v'", s.name, s.URL)
	case <-s.Halt.ReqStop.Chan:
		return nil, ErrShutDown
	}

	return
}

// InitAndStart sets everything up and Start()s the node.
// registeredServiceName should be either "tube-replica"
// or "tube-client" depending on which role you
// want your node to play.
func (s *TubeNode) InitAndStart() error {

	s.started = true
	s.srvname = "srv_" + s.name
	//s.srvname = s.name
	s.Srv = rpc.NewServer(s.srvname, s.cfg.RpcCfg)

	serverAddr, err := s.Srv.Start()
	panicOn(err)
	s.rpcServerAddr = serverAddr
	//vv("%v started srv at '%v'", s.name, serverAddr)
	// ??? usually just before rpc.NewClient()
	//cfg.ClientDialToHostPort = serverAddr.String()

	err = s.Srv.PeerAPI.RegisterPeerServiceFunc(string(s.PeerServiceName), s.Start)
	panicOn(err)

	// coordinated shutdown
	// try having Cluster own instead?
	//s.Srv.GetHostHalter().AddChild(s.Halt) // already has parent if cluster owns it

	// racy so try not to touch 's' now, the Start() goro has it!
	var peerServiceNameVersion string
	_, err = s.Srv.PeerAPI.StartLocalPeer(context.Background(), string(s.PeerServiceName), peerServiceNameVersion, nil, s.name, true)
	panicOn(err)

	// to avoid racing on reading s.URL and PeerID to wire
	// up the grid, wait for startupNodeUrlSafeCh to be closed.
	select {
	case <-s.startupNodeUrlSafeCh.Chan:
		//vv("TubeNode.InitAndStart end: node '%v' has started, at url '%v'", s.name, s.URL)
	case <-s.Halt.ReqStop.Chan:
		return ErrShutDown
	}
	return nil
}

func (s *TubeNode) isRegularTest() bool {
	return s.cfg.isTest && s.cfg.testNum != 802
	// simae and Test802 does not set up a cluster, this misses it:
	//return s.cfg.testCluster != nil
}
func (s *TubeNode) isSimAeTest() bool {
	return s.cfg.isTest && s.cfg.testNum == 802
	// simae and Test802 does not set up a cluster, this misses it:
	//return s.cfg.testCluster != nil
}

func (s *TubeNode) isTest() bool {
	return s.cfg.isTest
	// simae and Test802 does not set up a cluster, this misses it:
	//return s.cfg.testCluster != nil
}

func (s *TubeNode) statString(frag *rpc.Fragment) string {
	s.debugMsgStat[frag.FragOp]++
	return fmt.Sprintf("%v", s.debugMsgStat[frag.FragOp])
}

func (s *TubeNode) aliasSetup(myPeer *rpc.LocalPeer) {
	// full PeerID:
	rpc.AliasRegister(myPeer.PeerID, "("+s.name+") "+myPeer.PeerID+" ")
	// or nickname only:
	//rpc.AliasRegister(myPeer.PeerID, "("+s.name+")")
}

func panicAtCap[T any](ch chan T) chan T {
	n := len(ch)
	if n >= cap(ch) {
		panic(fmt.Sprintf("arg! test channel cap reached: %v; raise TEST_CHAN_CAP", n))
	}
	return ch
}

func (s *TubeNode) addInspectionToTicket(tkt *Ticket) {
	//vv("%v addInspectionToTicket called by '%v'", s.name, fileLine(3)) // FinishTicket typically, or changeMembership earlier.
	insp := newInspection()
	insp.Minimal = true
	insp.done = nil
	s.inspectHandler(insp)
	tkt.Insp = insp
}

func (s *TubeNode) inspectHandler(ins *Inspection) {
	//vv("%v got <-s.requestInspect, in inspectHandler()", s.me())

	// minimize true
	// tries to avoid tubels failing to get reply
	// when the serz gets too large (hdr.go drops
	// on exceeding UserMaxPayload/maxMessage.
	minimize := ins.Minimal

	ins.Cfg = s.cfg
	ins.LastLogIndex = s.lastLogIndex()
	ins.LastLogTerm = s.lastLogTerm()
	// (compaction means there is no correspondence between len(s.wal.raftLog)
	// and LastLogIndex anymore).
	n := len(s.wal.raftLog)
	if ins.LastLogIndex > 0 && n > 0 {
		rle := s.wal.raftLog[n-1]
		ins.LastLogLeaderName = rle.LeaderName
		ins.LastLogTicketOp = rle.Ticket.Op
	}
	ins.LogIndexBaseC = s.wal.logIndex.BaseC

	ins.ResponderPeerID = s.PeerID
	ins.ResponderPeerURL = s.URL
	ins.ResponderName = s.name
	ins.Role = s.role
	ins.Hostname = s.hostname
	ins.PID = fmt.Sprintf("%v", os.Getpid())

	// don't report someone else as leader if we are.
	if s.role == LEADER {
		if s.leaderName != s.name {
			alwaysPrintf("warning: how is this possible? s.role==LEADER but s.name='%v' while s.leaderName='%v'... becomeLeader() would have set this... ", s.name, s.leaderName)

			// what becomeLeader does:
			//s.leaderID = s.PeerID
			//s.leaderName = s.name
			//s.leaderURL = s.URL
		}
	}

	ins.CurrentLeaderName = s.leaderName
	ins.CurrentLeaderURL = s.leaderURL
	ins.CurrentLeaderID = s.leaderID
	ins.CurrentLeaderFirstObservedTm = s.currentLeaderFirstObservedTm

	// don't report ourselves as leader unless
	// we really are.
	if s.leaderName == s.name {
		if s.role != LEADER ||
			s.lastBecomeLeaderTerm == 0 ||
			s.lastBecomeLeaderTerm != s.state.CurrentTerm {
			//vv("%v invalidating self-report of leadership", s.me())

			ins.CurrentLeaderName = ""
			ins.CurrentLeaderURL = ""
			ins.CurrentLeaderID = ""
		}
	}

	ins.ElectionCount = s.countElections
	ins.LastLeaderActiveStepDown = s.lastLeaderActiveStepDown
	if !minimize {
		ins.State = s.getStateSnapshot()
	}

	if !minimize {
		//for _, tkt := range s.tkthist {
		//	ins.Tkthist = append(ins.Tkthist, tkt.clone())
		//}

		for id, info := range s.peers {
			if minimize && info.PeerServiceName == TUBE_CLIENT {
				continue
			}
			infoClone := info.clone()
			ins.Peers[id] = infoClone
			cktP, ok := s.cktall[id]
			if !ok {
				// our ckt died after 055 node_0 reboot, we
				// could have a pending reconnect? but why
				// in peers if it died?
				// maybe they should not be in peers yet? just ckt?
				// not sure why 055 was hitting this.
				//panic(fmt.Sprintf("%v in peers but not in cktall?? id='%v'; info='%#v'", s.name, id, info))
				alwaysPrintf("%v warning: in peers but not in cktall?? id='%v'; info='%#v'", s.name, id, info)
			} else {
				ckt := cktP.ckt
				infoClone.PeerURL = ckt.RemoteCircuitURL()
				infoClone.RemoteBaseServerAddr = cktP.PeerBaseServerAddr
			}
			//vv("we set infoClone.PeerURL = '%v'", infoClone.PeerURL)
		}
		// since peers does not include ourselves, add ourselves too.
		ins.Peers[s.PeerID] = &RaftNodeInfo{
			PeerName:             s.name,
			PeerURL:              s.URL,
			IsInspectResponder:   true,
			MC:                   s.state.MC.Clone(),
			RemoteBaseServerAddr: s.MyPeer.BaseServerAddr,
		}
	}
	for id, cktP := range s.cktReplica {
		ckt := cktP.ckt
		if minimize && cktP.PeerServiceName == TUBE_CLIENT {
			continue
		}
		ins.CktReplica[id] = ckt.RemoteCircuitURL() // inspection copy
		//vv("%v inspectHandler(): setting CktReplica['%v'] = '%v'", s.name, id, ckt.RemoteCircuitURL())
	}
	// add ourselves too. but only if we are in MC.
	var inMC bool
	if s.state.MC != nil {
		_, inMC = s.state.MC.PeerNames.Get2(s.name)
		if inMC {
			ins.CktReplica[s.PeerID] = s.URL // inspection copy
		}
	}
	// for inspection ease, make a pseudo ins.CktReplicaByName.
	// it does not really exist on the node. but also cover the
	// full current MC
	if s.state.MC != nil {
		for name, det := range s.state.MC.PeerNames.All() {
			//for _, cktP := range s.cktReplica {
			cktP, ok := s.cktReplica[det.PeerID]
			if ok {
				ins.CktReplicaByName[name] = cktP.ckt.RemoteCircuitURL() // inspection copy
				//ins.CktReplicaByName[ckt.RemotePeerName] = ckt.RemoteBaseServerNoCktURL("")
			} else {
				ins.CktReplicaByName[name] = fmt.Sprintf("down (%v)", det.URL)
				// verify that it is not in cktall
				cktP2, ok2 := s.cktall[det.PeerID]
				if ok2 {
					ins.CktReplicaByName[name] = "< in cktall but NOT cktReplica >: " + cktP2.ckt.RemoteCircuitURL()
				}
			}
		}
	}
	// add ourselves too.
	if inMC { // avoid making it look like we are in MC if we are not.
		ins.CktReplicaByName[s.name] = s.URL // inspection copy
	}

	ins.MC = s.state.MC.Clone() // in inspectHandler
	ins.ShadowReplicas = s.state.ShadowReplicas.Clone()

	// tubels wants cktAll now
	for _, cktP := range s.cktall {
		ckt := cktP.ckt
		url := ckt.RemoteCircuitURL()
		ins.CktAll[url] = ckt.RemotePeerName
	}
	// add ourselves too.
	ins.CktAll[s.URL] = s.name

	if !minimize {
		for name, cktP := range s.cktAllByName {
			ckt := cktP.ckt

			var url string
			switch {
			case cktP.isPending():
				url = "pending"
			case ckt == nil:
				// racy
				//panic(fmt.Sprintf("why is ckt nil if not pending? cktP='%#v'", cktP))
			default:
				url = ckt.RemoteCircuitURL()
			}
			ins.CktAllByName[name] = url
		}
		// add ourselves too.
		ins.CktAllByName[s.name] = s.URL

		ins.WaitingAtLeader = s.cloneWaitingAtLeaderToMap()
		ins.WaitingAtFollow = s.cloneWaitingAtFollowToMap()

		// same format as CktAllByName, for helper.go ease.
		ins.CktAllByName[s.name] = s.URL
		for name, det := range s.state.Known.PeerNames.All() {
			ins.Known[name] = det.URL
		}
	} // end if minimize

	if ins.done != nil {
		close(ins.done)
	}
}

func sortedCktname(ckts map[string]*cktPlus) (names []string) {
	for nm := range ckts {
		names = append(names, nm)
	}
	sort.Strings(names)
	return
}

func (s *TubeNode) followers() (r string) {
	r = fmt.Sprintf("\n followers(ckt=%v, peers=%v):\n", len(s.cktReplica), len(s.peers))
	i := 0

	replicaNames := sortedCktname(s.cktReplica)

	for _, peer := range replicaNames {
		foll, ok := s.peers[peer]
		if ok {
			r += fmt.Sprintf("[%v] %v foll.LastHeardAnything = %v ; since last full pong: %v ; lcp = %v ; MatchIndex==LastApplied = %v\nMC = '%v'", i, rpc.AliasDecode(foll.PeerID), nice(foll.LastHeardAnything), time.Since(foll.LastFullPong.RecvTm), foll.LargestCommonRaftIndex, foll.MatchIndex == s.state.LastApplied, foll.MC.Short())
		} else {
			r += fmt.Sprintf("[%v] %v  arg. nothing in s.peers[peer] for them.\n", i, rpc.AliasDecode(peer))

		}
		i++
	}
	r += "\n"
	return
}

// does _not_ add to s.Waiting, caller should if we return true.
// we send the redirect back to the caller internally.
func (s *TubeNode) redirectToLeader(tkt *Ticket) (redirected bool) {
	//vv("%v redirectToLeader() called by '%v'; s.clusterSize()=%v; s.state.MC=%v; tkt=%v", s.me(), fileLine(2), s.clusterSize(), s.state.MC, tkt)

	// Arg! This makes 402 grow a cluster from 1 node
	// very difficult, since each new node on its own
	// wants to become leader!!??? add this check for testNum < 400:
	if s.clusterSize() == 1 && s.cfg.isTest && s.cfg.testNum < 400 {
		// Test001_no_replicas_write_new_value tube_test.go.
		if s.role != LEADER {
			//vv("%v cluster size 1, about to becomeLeader", s.me())
			s.becomeLeader()
		}
		tkt.Stage += ":redirectToLeader(false,clusterSize=1)"
		return false
	}
	if s.role == 0 {
		panic("not initialized yet, role=0")
	}
	if s.role == LEADER {
		//vv("%v we are leader, returning false from redirectToLeader()", s.name)
		tkt.Stage += ":redirectToLeader(false,role_is_leader)"
		return
	}
	//if s.leaderID == s.PeerID {
	// probably just a logical race?
	//panic(fmt.Sprintf("%v I am not leader, so how did my PeerID get set as s.leaderID? s.leaderID = '%v' ", s.me(), s.leaderID))
	//}

	//if s.leaderID == s.PeerID {
	// stale; or we left
	// var chacha8rand *mathrand2.ChaCha8 = newZeroSeededChaCha8()
	// instead of
	// var chacha8rand *mathrand2.ChaCha8 = newCryrandSeededChaCha8()
	// in hdr.go, ha! clear on leaderName instead:
	if s.leaderName == s.name {
		s.leaderID = ""
		s.leaderName = ""
		s.leaderURL = ""
	}

	// stashForLeader = false is easier
	// to reason about, since tup hangs plus ctrl-c do not result
	// in later addition of members once a leader is found.
	// That can be surprising(!) It is a result of
	// the awaiting leader ticket queue, which prevents
	// a bunch of logical races when trying to
	// build up a cluster, for instance in the 402/403 tests.
	//
	// To accomodate 402 and 403 membership_tests, we use
	// the errWriteDur in the AddPeer call to decide how
	// long to wait for a leader. If 0 then deadline will
	// be zero, and we won't stash away the ticket
	// waiting for a leader but rather eagerly error out.
	//
	// The 059 compact_test and 402/403
	// membership_test.go use 2 seconds, so
	// they do stash, as traditionally was done.
	// The command line clients tubels/rm/add use zero,
	// so they eagerly error.
	//   ... the critical ones that needed fixing under
	//       stashForLeader = false (they now stash with writeErrDur > 0)
	// red 059 compact_test.go
	// red Test402_build_up_a_cluster_from_one_node membership_test.go
	// read Test403_reduce_a_cluster_down_to_one_node

	stashForLeader := !tkt.WaitLeaderDeadline.IsZero()

	//vv("%v stashForLeader is %v; tkt.WaitLeaderDeadline='%v' (in %v); tkt4=%v", s.name, stashForLeader, nice9(tkt.WaitLeaderDeadline), time.Until(tkt.WaitLeaderDeadline), tkt.TicketID[:4])

	if s.leaderID == "" {
		if stashForLeader {
			// save it until we do get a leader?
			s.ticketsAwaitingLeader[tkt.TicketID] = tkt
			//s.Waiting[tkt.TicketID] = tkt
			//vv("%v no leader yet, saving ticket until then: '%v'", s.me(), tkt.Short())
			tkt.Stage += fmt.Sprintf(":redirectToLeader(true,not_leader)_from_%v", fileLine(2))
			return true
		}
		//vv("%v stashForLeader is false, and s.leaderID is empty", s.me())
		// stashing for later leader made for a weird
		// command line tubeadd experience/hang. better to eagerly error.

		//addOther := false
		var xtra string
		if tkt.Op == MEMBERSHIP_SET_UPDATE &&
			tkt.AddPeerName != "" &&
			tkt.AddPeerName != s.name {
			//addOther = true
			xtra = fmt.Sprintf(" MEMBERSHIP_SET_UPDATE tkt.AddPeerName='%v'. To prevent a node from adding a dead neighbor (the drowned sailor scenario, page 22, The Part-Time Parliament) by mistake, we require additions to leaderless clusters to come from self-add only. See also bootstrappedOrForcedMembership() circa tube.go:15131. This error from redirectToLeader() circa %v. Update: the tubeadd -f forcedNodeAddition can be used as a last resort, but risks membership corruption...", tkt.AddPeerName, fileLine(1))
		}
		tkt.Err = fmt.Errorf("ahem. no leader known to me (node '%v'). stashForLeader is false.%v", s.name, xtra)

		// page 22 of Lamport 1998, "The Part-Time Parliament".
		// "Changing the composition of Parliament in this
		// way [me: choosing new members based on a prior,
		// n-3, log entry] was dangerous and had to be done with care.
		// The consistency and progress conditions would always hold.
		// However, the progress condition guaranteed progress
		// only if a majority set was in the Chamber; it did
		// not guarantee that a majority set would ever be there. In fact,
		// the mechanism for choosing legislators led to the
		// downfall of the Parliamentary system in Paxos. Because
		// of a scribe’s error, a decree that was supposed to honor
		// sailors who had drowned in a shipwreck instead
		// declared them to be the only members of Parliament.
		// Its passage prevented any new decrees from being passed—
		// including the decrees proposed to correct the mistake.
		// Government in Paxos came to a halt. A general named
		// Λαµπσων [me: "Lampson", referring to Butler Lampson]
		// took advantage of the confusion to stage
		// a coup, establishinga military dictatorship that
		// ended centuries of progressive government. Paxos grew
		// weak under a series of corrupt dictators, and was unable
		// to repel an invasion from the east that led to the
		// destruction of its civilization."

		//s.respondToClientTicketApplied(tkt)
		s.replyToForwardedTicketWithError(tkt)
		s.FinishTicket(tkt, false)
		return true
	}
	if tkt.FromID == s.PeerID {
		//vv("%v ticket from ourself, we are not leader (%v), tkt: '%v'", s.me(), alias(s.leaderID), tkt)
	}
	//vv("%v in redirectToLeader (we are %v): will send to: s.leaderName = '%v' the tkt='%v'", s.name, s.role, s.leaderName, tkt.Short())

	// In redirectToLeader() bool here.
	// Per Ch 4 on config changes, the leader might not
	// actually officially be in the cluster during
	// transitions. So use s.cktall, not s.cktReplica here.
	cktP, ok := s.cktall[s.leaderID]
	if !ok {
		alwaysPrintf("%v don't know how to contact '%v' (because not in cktall its s.leaderID='%v') to redirect to leader; for tkt '%v'. Assuming they died. s.cktall = '%#v'", s.me(), s.leaderName, s.leaderID, tkt.Short(), s.cktall)

		// return true so that we do not assume we are
		// the leader, even if we cannot contact one.
		// stash it like above where we had no leader.
		if stashForLeader {
			s.ticketsAwaitingLeader[tkt.TicketID] = tkt
			//s.Waiting[tkt.TicketID] = tkt ?

			//vv("%v no leader yet, saving ticket until then: '%v'", s.me(), tkt)
			tkt.Stage += ":redirectToLeader(true,no_leader_yet)" // was (true,cannot_redirect_to_leader)
			return true
		}
		// stashing for later leader made for a weird
		// command line tubeadd experience/hang.
		//vv("%v stashForLeader is false, and no cktall for leader '%v'", s.me(), s.leaderName)
		tkt.Err = fmt.Errorf("hmm. no leader known to me (node '%v')", s.name)
		//s.respondToClientTicketApplied(tkt)
		s.replyToForwardedTicketWithError(tkt)
		s.FinishTicket(tkt, false)
		return true
	}
	ckt := cktP.ckt

	// client -> follower -> lead means we want to rewrite the from
	// so the leader can respond to the follower? Yuck. it really would
	// be simpler to reject if we were not the originator, and
	// for client -> leader directly. Otherwise we could have
	// a whole stack of forwarding chains to push/pop.

	//vv("%v: redirectToLeader sending newFrag.FragOp=RedirectTicketToLeaderMsg over ckt '%v'", s.name, ckt)
	frag := s.newFrag()
	frag.FragOp = RedirectTicketToLeaderMsg
	frag.FragSubject = "RedirectTicketToLeader"
	bts, err := tkt.MarshalMsg(nil)
	panicOn(err)
	frag.Payload = bts

	frag.SetUserArg("redirectedByName", s.name)

	frag.SetUserArg("leader", s.leaderID)
	frag.SetUserArg("leaderName", s.leaderName)
	//err = s.SendOneWay(ckt, frag, -1, 0)
	//vv("tkt.waitForValid = %v; s.leaderName = '%v'; ckt='%v'", tkt.waitForValid, s.leaderName, ckt)
	err = s.SendOneWay(ckt, frag, tkt.waitForValid, 0)
	_ = err // don't panic on halting.
	if err != nil {
		alwaysPrintf("%v non nil error '%v' on Redirect to '%v'", s.me(), err, ckt.RemotePeerID)
		tkt.Stage += ":redirectToLeader_error_on_SendOneWay"
		tkt.Err = fmt.Errorf("%v non nil error '%v' on Redirect to '%v'", s.me(), err, ckt.RemotePeerID)
		return true
	}
	if tkt.Op != WRITE {
		//vv("%v redirected %v ticket to leader '%v' ticket: '%v'", s.me(), tkt.Op, rpc.AliasDecode(s.leaderID), tkt)
	}
	tkt.Stage += ":redirectToLeader(true,sent_RedirectTicketToLeaderMsg)"
	//vv("%v fragSend; err='%v'", s.name, err)
	return true
}

type Inspection struct {
	CktReplica       map[string]string        `zid:"0"`
	CktReplicaByName map[string]string        `zid:"1"`
	CktAll           map[string]string        `zid:"2"`
	CktAllByName     map[string]string        `zid:"3"`
	Peers            map[string]*RaftNodeInfo `zid:"4"`

	WaitingAtLeader map[string]*Ticket `zid:"5"`
	WaitingAtFollow map[string]*Ticket `zid:"6"`

	Role                     RaftRole   `zid:"7"`
	State                    *RaftState `zid:"8"`
	CurrentLeaderName        string     `zid:"9"`
	CurrentLeaderID          string     `zid:"10"`
	CurrentLeaderURL         string     `zid:"11"`
	ElectionCount            int        `zid:"12"`
	LastLeaderActiveStepDown time.Time  `zid:"13"`

	// TubeConfig has ClusterSize, which determines quorum.
	Cfg TubeConfig `zid:"14"`

	MC               *MemberConfig `zid:"15"`
	ResponderPeerID  string        `zid:"16"`
	ResponderPeerURL string        `zid:"17"`
	ResponderName    string        `zid:"18"`

	LastLogIndex      int64    `zid:"19"`
	LastLogTerm       int64    `zid:"20"`
	LastLogLeaderName string   `zid:"21"`
	LastLogTicketOp   TicketOp `zid:"22"`
	LogIndexBaseC     int64    `zid:"23"`

	ShadowReplicas *MemberConfig     `zid:"24"`
	Known          map[string]string `zid:"25"`

	CurrentLeaderFirstObservedTm time.Time `zid:"26"`

	Tkthist []*Ticket `msg:"-"`

	Minimal bool `zid:"27"`

	Hostname string `zid:"28"`
	PID      string `zid:"29"`

	// internal use only. tests/users call TubeNode.Inpsect()
	done chan struct{}
}

func (s *Inspection) String() (r string) {
	var peers string
	for id, info := range s.Peers {
		peers += fmt.Sprintf(`Peers ['%v'] "%v": "%v" (IsInspectResponder: %v; RemoteBaseServerAddr: %v)
`, info.PeerName, rpc.AliasDecode(id), info.PeerURL, info.IsInspectResponder, info.RemoteBaseServerAddr)
	}

	var cktreplica string
	for id, url := range s.CktReplica {
		cktreplica += fmt.Sprintf(`CktReplica "%v" -> "%v"
`, rpc.AliasDecode(id), url)
	}

	var cktall string
	for id, url := range s.CktAll {
		cktall += fmt.Sprintf(`CktAll "%v": -> "%v"
`, rpc.AliasDecode(id), url)
	}
	var cktAllByName string
	for name, cktp := range s.CktAllByName {
		cktAllByName += fmt.Sprintf(`CktAllByName "%v": -> "%v"
`, name, cktp)
	}

	var known string
	for name, url := range s.Known {
		known += fmt.Sprintf(`Known "%v": -> "%v"
`, name, url)
	}

	r = fmt.Sprintf(`Inspection{
      ResponderName: %v,
  CurrentLeaderName: %v,
    CurrentLeaderID: %v,
   ---------- CktAll: ------------
%v
   ---------- Peers: -------------
%v
   ---------- CktReplica: --------
%v
   ---------- CktAllByName: ------
%v
   ---------- Known: --------
%v

`, s.ResponderName,
		s.CurrentLeaderName,
		s.CurrentLeaderID,
		cktall, peers, cktreplica, cktAllByName, known)

	r += fmt.Sprintf("   --- WaitingAtLeader: %v\n\n", s.WaitingAtLeader)
	r += fmt.Sprintf("   --- WaitingAtFollow: %v\n\n", s.WaitingAtFollow)
	r += fmt.Sprintf("   --- MC: %v\n\n", s.MC.Short())

	return r + "}"
}

func clonePeers(peers map[string]*RaftNodeInfo) (r map[string]*RaftNodeInfo) {
	r = make(map[string]*RaftNodeInfo)
	for id, info := range peers {
		r[id] = info.clone()
	}
	return
}

func newInspection() *Inspection {
	return &Inspection{
		Peers:            make(map[string]*RaftNodeInfo),
		CktReplica:       make(map[string]string),
		CktReplicaByName: make(map[string]string),
		CktAll:           make(map[string]string),
		CktAllByName:     make(map[string]string),
		Known:            make(map[string]string),
		done:             make(chan struct{}),
		WaitingAtLeader:  make(map[string]*Ticket),
		WaitingAtFollow:  make(map[string]*Ticket),
	}
}
func (s *TubeNode) Inspect() (look *Inspection) {
	look = newInspection()
	look.Minimal = false
	select {
	case s.requestInpsect <- look:
	case <-s.Halt.ReqStop.Chan:
		return
	}
	select {
	case <-look.done:
		return
	case <-s.Halt.ReqStop.Chan:
		return
	}
}

// Ticket is how Read/Write operations are submitted
// to raft in a goroutine safe way such that the
// caller can simply wait until the Done channel is closed
// to resume. It is exported for serialization purposes.
type Ticket struct {
	ctx context.Context

	TSN int64     `zid:"0"`
	T0  time.Time `zid:"1"`

	// the HLC when NewTicket was called locally.
	CreateHLC HLC `zid:"2"`

	Key                      Key    `zid:"3"`
	Val                      Val    `zid:"4"` // Read/Write. For CAS: new value.
	Table                    Key    `zid:"5"` // like a database table, a namespace.
	OldVal                   Val    `zid:"6"` // For CAS: old value (tested for).
	CASwapped                bool   `zid:"7"`
	CASRejectedBecauseCurVal Val    `zid:"8"`
	NewTableName             Key    `zid:"9"`
	Vtype                    string `zid:"10"`

	FromID    string `zid:"11"`
	FromName  string `zid:"12"` // tell old leader (or client) from new
	ClusterID string `zid:"13"`

	// regular code should use this, not Errs.
	Err error `msg:"-"`
	// version of Err for transport only
	// (since the error interface does not serialize well).
	Errs string `zid:"14"`

	// TicketID is a unique identifier for each Ticket.
	TicketID string `zid:"15"`

	Op TicketOp `zid:"16"`

	// with client retries this might be needed?
	Done *idem.IdemCloseChan `msg:"-"`

	// notice: the LogIndex for a committed ticket
	// can act as a fencing token for external
	// services to avoid consistency issues due to
	// client processes being paused.
	LogIndex int64 `zid:"17"` // where we are logged to.
	Term     int64 `zid:"18"` // where we are logged to.

	// short description of transaction to make
	// debugging easier. Optional but recommended.
	Desc string `zid:"19"`

	// easy to display tick status in debug prints
	Committed   bool `zid:"20"`
	Applied     bool `zid:"21"`
	ClientAcked bool `zid:"22"`

	// when we committed, in commitWhatWeCan(),
	// what was LastApplied? Not sure if on leader for sure(?)
	// (not written to raft log, obviously).
	AsOfLogIndex int64 `zid:"23"`

	// who got our ticket and
	// assigned the LogIndex.
	// These are more for diagnostics than
	// any necessary part of the protocol.
	// They are assigned in replicateTicket().
	LeaderID      string `zid:"24"`
	LeaderName    string `zid:"25"`
	LeaderURL     string `zid:"26"`
	LeaderStampSN int64  `zid:"27"`

	SessionID             string `zid:"28"`
	SessionSerial         int64  `zid:"29"`
	SessionLastKnownIndex int64  `zid:"30"`

	Stage string `zid:"31"`

	DoneClosedOnPeerID string `zid:"32"`

	// =======  MEMBERSHIP_SET_UPDATE  ===========
	// IN: only one Add or Remove can be set:
	AddPeerName               string `zid:"33"`
	AddPeerID                 string `zid:"34"`
	AddPeerServiceName        string `zid:"35"`
	AddPeerServiceNameVersion string `zid:"36"`
	AddPeerBaseServerHostPort string `zid:"37"`

	RemovePeerName               string `zid:"38"`
	RemovePeerID                 string `zid:"39"`
	RemovePeerServiceName        string `zid:"40"`
	RemovePeerServiceNameVersion string `zid:"41"`
	RemovePeerBaseServerHostPort string `zid:"42"`

	// provided by SingleUpdateClusterMemberConfig to let
	// the node find the cluster.
	GuessLeaderURL string `zid:"43"`

	// OUT:
	// Insp=optional inspection. MEMBERSHIP_SET_UPDATE
	// always returns it, other Ops not atm,
	// but can in the future if desired.
	Insp *Inspection `zid:"44"`

	// internal use: caller need not/should not set this.
	// note that the tickets in the wal show very
	// out of date MC and we don't pull MC from the wal
	// only the state on disk now.
	MC *MemberConfig `zid:"45"`

	// =======  end MEMBERSHIP_SET_UPDATE  ===========

	// =======  begin CLIENT LINEARIZABILITY HELP (Chapter 6)

	LeaderGotTicketTm        time.Time `zid:"46"`
	LeaderLocalReadGoodUntil time.Time `zid:"47"`
	LeaderLocalReadAtTm      time.Time `zid:"48"`
	LeaderLocalReadHLC       HLC       `zid:"49"`

	ClientLocalSubmitTm   time.Time `zid:"50"`
	ClientLocalResponseTm time.Time `zid:"51"`

	// leader should reject if they have not seen this log index.
	ClientHighestLogIndexSeen int64 `zid:"52"`
	// -- see above for
	// FromName (above)
	// SessionID (above)
	// SessionSerial (above)
	// -- which are also essential for client linz.

	NewSessReq                  *Session `zid:"53"`
	NewSessReply                *Session `zid:"54"`
	DupDetected                 bool     `zid:"55"`
	MinSessSerialWaiting        int64    `zid:"56"`
	EndSessReq_SessionID        string   `zid:"57"`
	HighestSerialSeenFromClient int64    `zid:"58"`

	// =======  end CLIENT LINEARIZABILITY HELP  ===========

	// state snapshot transfer for new joiners
	StateSnapshot *RaftState `zid:"59"`

	// key range scan input
	KeyEndx     Key  `zid:"60"`
	ScanDescend bool `zid:"61"` // default ascending
	// key range scan output
	KeyValRangeScan *art.Tree `zid:"62"`

	UserDefinedOpCode int64 `zid:"63"`

	// let clients/tests 402/403 tell
	// AddPeerIDToCluster how long to
	// await leader.
	WaitLeaderDeadline time.Time `zid:"64"`

	ForceChangeMC bool `zid:"65"`

	// lease on writing to table:key
	LeaseRequestDur        time.Duration `zid:"66"` // optional on WRITE
	Leasor                 string        `zid:"67"` // optional on WRITE
	LeaseUntilTm           time.Time     `zid:"68"`
	LeaseEpoch             int64         `zid:"69"` // filled on response
	LeaseWriteRaftLogIndex int64         `zid:"70"` // filled on response
	LeaseAutoDel           bool          `zid:"71"`

	// when actually submitted to raft log in replicateTicket
	RaftLogEntryTm time.Time `zid:"72"`

	OldVersionCAS int64 `zid:"73"` // For CAS: old version (tested for).
	VersionRead   int64 `zid:"74"` // version of key that we read.

	// to improve load bearing / latency, we want to
	// batch up consensus operations and store a bunch of them
	// durably to disk under a sync set of fsyncs.
	// This recursion should only ever be one level deep.
	// The tickets in this Batch may only have their own Batch = nil.
	Batch []*Ticket `zid:"75"`

	// where in tkthist we were entered locally.
	localHistIndex int

	// should we return early and stop waiting
	// after a preset duration? 0 means wait forever.
	waitForValid time.Duration

	// when a Ticket is forwarded to leader, this is the
	// corresponding reply from leader.
	answer *Ticket

	finishTicketCalled bool
}

// ticket operations, aka "commands" to the state machine.
type TicketOp int64

const (
	UNSET TicketOp = 0
	NOOP  TicketOp = 1
	READ  TicketOp = 2
	WRITE TicketOp = 3

	// We only support one-at-a-time raft cluster
	// membership changes; not JointConsensus.
	MEMBERSHIP_SET_UPDATE TicketOp = 4
	MEMBERSHIP_BOOTSTRAP  TicketOp = 5

	SESS_NEW          TicketOp = 6
	SESS_END          TicketOp = 7
	SESS_REFRESH      TicketOp = 8
	DELETE_KEY        TicketOp = 9
	SHOW_KEYS         TicketOp = 10
	CAS               TicketOp = 11 // Compare-And-Swap
	MAKE_TABLE        TicketOp = 12
	DELETE_TABLE      TicketOp = 13
	RENAME_TABLE      TicketOp = 14
	READ_KEYRANGE     TicketOp = 15
	READ_PREFIX_RANGE TicketOp = 16

	ADD_SHADOW_NON_VOTING    TicketOp = 17 // through writeReqCh
	REMOVE_SHADOW_NON_VOTING TicketOp = 18

	USER_DEFINED_FSM_OP TicketOp = 19

	// when a ticket acts as a container for a
	// batch of other tickets.
	//TKT_BATCH TicketOp = 20
)

func (t TicketOp) String() (r string) {
	switch t {
	case UNSET:
		return "UNSET"
	case NOOP:
		return "NOOP"
	case READ:
		return "READ"
	case WRITE:
		return "WRITE"
	case MEMBERSHIP_SET_UPDATE:
		return "MEMBERSHIP_SET_UPDATE"
	case MEMBERSHIP_BOOTSTRAP:
		return "MEMBERSHIP_BOOTSTRAP"
	case SESS_NEW:
		return "SESS_NEW"
	case SESS_END:
		return "SESS_END"
	case SESS_REFRESH:
		return "SESS_REFRESH"
	case DELETE_KEY:
		return "DELETE_KEY"
	case SHOW_KEYS:
		return "SHOW_KEYS"
	case CAS:
		return "CAS"
	case MAKE_TABLE:
		return "MAKE_TABLE"
	case DELETE_TABLE:
		return "DELETE_TABLE"
	case RENAME_TABLE:
		return "RENAME_TABLE"
	case READ_KEYRANGE:
		return "READ_KEYRANGE"
	case READ_PREFIX_RANGE:
		return "READ_PREFIX_RANGE"
	case ADD_SHADOW_NON_VOTING:
		return "ADD_SHADOW_NON_VOTING"
	case REMOVE_SHADOW_NON_VOTING:
		return "REMOVE_SHADOW_NON_VOTING"
	case USER_DEFINED_FSM_OP:
		return "USER_DEFINED_FSM_OP"
		//case TKT_BATCH:
		//	return "TKT_BATCH"
	}
	r = fmt.Sprintf("(unknown TicketOp: %v", int64(t))
	panic(r)
	return
}

func (s *TubeNode) FinishTicket(tkt *Ticket, calledOnLeader bool) {

	// idempotent to let CommitWhatWeCan possibly
	// call us 2x to make sure all membership changes
	// get at least 1x call here and cleared from WaitingAtLeader.
	if tkt.finishTicketCalled {
		return
	}
	tkt.finishTicketCalled = true

	//vv("%v FinishTicket for LogIndex:%v, Serial: %v; tkt4=%v", s.me(), tkt.LogIndex, tkt.TSN, tkt.TicketID[:4]) // , stack())
	//zz("caller = %v", fileLine(2))

	if tkt.Op == MEMBERSHIP_SET_UPDATE ||
		tkt.Op == MEMBERSHIP_BOOTSTRAP ||
		tkt.Op == ADD_SHADOW_NON_VOTING ||
		tkt.Op == REMOVE_SHADOW_NON_VOTING ||
		tkt.Op == USER_DEFINED_FSM_OP {

		if tkt.Insp == nil {
			//panic("why wasn't Insp already set?")
			//vv("%v is filling in tkt.Insp in FinishTicket; tkt='%v'", s.me(), tkt.Desc)
			s.addInspectionToTicket(tkt)
		}
	}

	//vv("%v FinishTicket deleting from Waiting tkt='%v'", s.me(), tkt)
	var waiting *imap
	if calledOnLeader {
		waiting = s.WaitingAtLeader

		//vv("delete from WaitingAtLeader tkt.Serial='%v'", tkt.Serial)
		tkt.Stage += ":FinishTicket_calledOnLeader_delete_from_WALeader"
	} else {
		waiting = s.WaitingAtFollow
		//vv("delete from WAF tkt.Serial='%v'", tkt.Serial)
		//vv("delete from WAF tkt.Serial='%v' stack=\n%v", tkt.Serial, stack())
		tkt.Stage += ":FinishTicket_calledOnFollow_delete_from_WAF"
	}

	prior, already := waiting.get2(tkt.TicketID)
	if already && prior != tkt {

		prior.Stage += ":FinishTicket_am_prior_copy_from_tkt"

		// tell waiting client the value
		// equiavlent to question-answer in
		// answerToQuestionTicket()
		//vv("%v FinishTicket, setting prior.NewSessReply(%v) = tkt.NewSessReq(%v)", s.name, prior, tkt)
		prior.NewSessReply = tkt.NewSessReq
		prior.DupDetected = tkt.DupDetected

		prior.Val = tkt.Val // critical to get read value back
		prior.Vtype = tkt.Vtype
		prior.RaftLogEntryTm = tkt.RaftLogEntryTm
		prior.KeyValRangeScan = tkt.KeyValRangeScan
		prior.Err = tkt.Err
		prior.Insp = tkt.Insp // membership change/query wants.
		prior.Committed = tkt.Committed
		prior.Applied = tkt.Applied
		prior.LogIndex = tkt.LogIndex
		prior.Term = tkt.Term
		prior.LeaderName = tkt.LeaderName
		prior.LeaderURL = tkt.LeaderURL
		prior.LeaderStampSN = tkt.LeaderStampSN
		prior.LeaderID = tkt.LeaderID
		prior.LeaseRequestDur = tkt.LeaseRequestDur
		prior.Leasor = tkt.Leasor
		prior.LeaseUntilTm = tkt.LeaseUntilTm
		prior.LeaseEpoch = tkt.LeaseEpoch
		prior.LeaseAutoDel = tkt.LeaseAutoDel
		prior.VersionRead = tkt.VersionRead
		prior.LeaseWriteRaftLogIndex = tkt.LeaseWriteRaftLogIndex
		prior.MC = tkt.MC

		prior.AsOfLogIndex = tkt.AsOfLogIndex // LastApplied when we committed.
		prior.LeaderLocalReadGoodUntil = tkt.LeaderLocalReadGoodUntil
		prior.LeaderLocalReadAtTm = tkt.LeaderLocalReadAtTm
		prior.LeaderLocalReadHLC = tkt.LeaderLocalReadHLC

		prior.Stage += ":FinishTicket_prior_Val_written"
		if prior.Done != nil {
			if prior.DoneClosedOnPeerID != "" {
				// seen. close on same node. does not seem worth crashing over now.
				alwaysPrintf("should be the first time-only close! s.PeerID='%v' but prior.DoneClosedOnPeerID='%v'", s.PeerID, prior.DoneClosedOnPeerID)
			}
			prior.DoneClosedOnPeerID = s.PeerID
			prior.Stage += ":FinishTicket_prior_Done_Closed"
			prior.Done.Close()
		} else {
			prior.Stage += ":FinishTicket_prior_Done_was_nil"
		}
	}
	waiting.del(tkt)

	// responses to client actions, when received on
	// the client, want to update their update view of MC.
	if s.role == CLIENT && tkt.MC != nil {
		s.clientInstallNewTubeClusterMC(tkt.MC)
	}

	// I understand the double close now! (old sporadic, should now be fixed).
	// Once on the apply at the peer who is both cluster member and client; and
	// Once when master replies to client that apply has been done.
	// The same TicketID bounces to peer possibly 3 times.
	//
	// leader <- client starts ticket for read/write (originated)
	// leader -> peer (client) to replicate, (might be seen 2nd time)
	// leader "this ticket is done" -> peer, committed, applied (close 1)
	// leader <- peer (yep, committed)
	// leader -> client (peer) your ticket is done. 3rd seen. (close 2)
	//
	// this is because we allow a peer to also be a client, which
	// is the most common case. We also want to be able to
	// support external clients too, so we need to gracefully
	// not freak out when same ticketID circulates or not.
	if tkt.Done != nil {
		if tkt.DoneClosedOnPeerID == "" {
			tkt.DoneClosedOnPeerID = s.PeerID
			tkt.Stage += ":FinishTicket_tkt_DoneClosed"
			tkt.Done.Close()
		}
	} else {
		tkt.Stage += ":FinishTicket_tkt_Done_was_nil"
	}
}

func (t *Ticket) String() string {
	if t == nil {
		return "(nil Ticket)"
	}
	lifetime := time.Since(t.T0)
	isClosed := "unk"
	if t.Done != nil {
		isClosed = fmt.Sprintf("%v", t.Done.IsClosed())
	}
	var extra string
	if t.Op == MEMBERSHIP_SET_UPDATE ||
		t.Op == MEMBERSHIP_BOOTSTRAP {
		extra = fmt.Sprintf(`
                MC: %v
       AddPeerName: "%v",
         AddPeerID: "%v",
AddPeerServiceName: "%v",
AddPeerBaseServerHostPort: "%v",
    RemovePeerName: "%v",
      RemovePeerID: "%v",
RemovePeerServiceName: "%v",
RemovePeerBaseServerHostPort: "%v"`,
			t.MC,
			t.AddPeerName,
			t.AddPeerID,
			t.AddPeerServiceName,
			t.AddPeerBaseServerHostPort,
			t.RemovePeerName,
			t.RemovePeerID,
			t.RemovePeerServiceName,
			t.RemovePeerBaseServerHostPort,
		)
	}
	var leaseUntilTmStr string
	if !t.LeaseUntilTm.IsZero() {
		leaseUntilTmStr = t.LeaseUntilTm.Format(RFC3339NanoNumericTZ0pad)
	}
	return fmt.Sprintf(`Ticket{
   --------  Ticket basics  ---------
finishTicketCalled: %v,
          TicketID: %v,
               TSN: %v,
              Desc: %v (since T0: %v),
                T0: %v,
                Op: %v,
               Key: "%v",
               Val: "%v",
             Vtype: "%v",
               Err: %v,%v
   --------  Leasing  ---------
    LeaseRequestDur: %v
             Leasor: "%v"
       LeaseUntilTm: %v
         LeaseEpoch: %v
       LeaseAutoDel: %v
LeaseWriteRaftLogIndex: %v
   --------  Ticket membership updates  ---------
       AddPeerName: %v
    RemovePeerName: %v
   --------  Ticket status  ---------
         Committed: %v,
           Applied: %v,
       ClientAcked: %v,
              Insp: %p,
   --------  Ticket client details  ---------
            FromID: %v,
          FromName: %v,
         SessionID: %v,
     SessionSerial: %v,
SessionLastKnownIndex: %v,
MinSessSerialWaiting: %v,
      waitForValid: %v,
   -------- Ticket RaftLogEntry in WAL details ---------
          LogIndex: %v,
              Term: %v,
      AsOfLogIndex: %v,
        LeaderName: %v,
     LeaderStampSN: %v,
          LeaderID: %v,
         ClusterID: %v,
          IsClosed: %v,
DoneClosedOnPeerID: "%v",
             Stage: %v,
}`, t.finishTicketCalled,
		rpc.AliasDecode(t.TicketID), t.TSN, t.Desc, lifetime,
		t.T0.Format(RFC3339NanoNumericTZ0pad),
		t.Op,
		string(t.Key),

		// just returns string(t.Val) if not ExternalCluster
		showExternalCluster(t.Val),
		t.Vtype,
		t.Err,
		extra,
		t.LeaseRequestDur,
		t.Leasor,
		leaseUntilTmStr,
		t.LeaseEpoch,
		t.LeaseAutoDel,
		t.LeaseWriteRaftLogIndex,
		t.AddPeerName,
		t.RemovePeerName,
		t.Committed,
		t.Applied,
		t.ClientAcked,
		t.Insp,
		rpc.AliasDecode(t.FromID),
		t.FromName,
		rpc.AliasDecode(t.SessionID), t.SessionSerial,
		t.SessionLastKnownIndex,
		t.MinSessSerialWaiting,
		t.waitForValid,
		t.LogIndex, t.Term, t.AsOfLogIndex,
		t.LeaderName, t.LeaderStampSN,
		t.LeaderID,
		rpc.AliasDecode(t.ClusterID),
		isClosed,
		t.DoneClosedOnPeerID,
		t.Stage,
	)
}

func (t *Ticket) Short() string {
	if t == nil {
		return "(nil Ticket)"
	}
	lifetime := time.Since(t.T0)
	return fmt.Sprintf(`Ticket{TSN: %v, Desc: %v (%v)}`, t.TSN, t.Desc, lifetime)
}

func (s *TubeNode) NewTicket(
	desc string,
	table Key,
	key Key,
	val Val,
	fromID string,
	fromName string,
	op TicketOp,
	waitForDur time.Duration,
	ctx context.Context,

) (tkt *Ticket) {

	hlc, t0 := s.hlc.CreateAndNow()
	tkt = &Ticket{
		Desc:         desc,
		TSN:          atomic.AddInt64(&debugNextTSN, 1),
		T0:           t0, // for gc of abandonded tickets.
		CreateHLC:    hlc,
		Op:           op,
		Table:        table,
		Key:          key,
		Val:          val,
		Done:         idem.NewIdemCloseChan(),
		FromID:       fromID,
		FromName:     fromName,
		ClusterID:    s.ClusterID,
		TicketID:     rpc.NewCallID(""),
		waitForValid: waitForDur,
		ctx:          ctx,
	}
	return
}

type SessionTableEntry struct {
	SessionID string `zid:"0"`

	Serial2Ticket *Omap[int64, *Ticket] `msg:"-"`

	HighestSerialSeenFromClient int64 `zid:"1"` // any op
	MaxAppliedSerial            int64 `zid:"2"` // writes or non-cached reads

	// re-created from Serial2Ticket so no need to serz.
	ticketID2tkt map[string]*Ticket

	// Serz is just for greenpack serialization
	// of Serial2Ticket. Serz will be nil during
	// normal operations.
	Serz map[int64]*Ticket `zid:"3"`

	// maintain/update here, initially a copy
	// of the original sess.SessionIndexEndxTm,
	// but refresh here too in case we lack the sess
	// and go to recover from follower-> leader.
	SessionEndxTm           time.Time `zid:"4"`
	SessionReplicatedEndxTm time.Time `zid:"5"`

	SessRequestedInitialDur time.Duration `zid:"6"`
	ClientName              string        `zid:"7"`
	ClientPeerID            string        `zid:"8"`
	ClientURL               string        `zid:"9"`
}

func (z *SessionTableEntry) String() (r string) {
	r = "&SessionTableEntry{\n"
	r += fmt.Sprintf("                  SessionID: \"%v\",\n", z.SessionID)
	r += fmt.Sprintf("HighestSerialSeenFromClient: %v,\n", z.HighestSerialSeenFromClient)
	r += fmt.Sprintf("           MaxAppliedSerial: %v,\n", z.MaxAppliedSerial)
	r += fmt.Sprintf("    SessRequestedInitialDur: %v,\n", z.SessRequestedInitialDur)
	r += fmt.Sprintf("              SessionEndxTm: %v,\n", z.SessionEndxTm)
	r += fmt.Sprintf("     (len %v) Serial2Ticket: \n", z.Serial2Ticket.Len())
	r += fmt.Sprintf("                 ClientName: %v,\n", z.ClientName)
	r += fmt.Sprintf("               ClientPeerID: %v,\n", z.ClientPeerID)
	r += fmt.Sprintf("                  ClientURL: %v,\n", z.ClientURL)
	for ser, tkt := range z.Serial2Ticket.All() {
		r += fmt.Sprintf("             %v: %v\n", ser, tkt.Short())
	}
	r += "}\n"
	return
}

func (s *SessionTableEntry) Clone() (r *SessionTableEntry) {

	r = &SessionTableEntry{
		SessionID:                   s.SessionID,
		HighestSerialSeenFromClient: s.HighestSerialSeenFromClient,
		MaxAppliedSerial:            s.MaxAppliedSerial,
		Serial2Ticket:               NewOmap[int64, *Ticket](),
		ticketID2tkt:                make(map[string]*Ticket),
		SessRequestedInitialDur:     s.SessRequestedInitialDur,
		SessionReplicatedEndxTm:     s.SessionReplicatedEndxTm,
		ClientName:                  s.ClientName,
		ClientPeerID:                s.ClientPeerID,
		ClientURL:                   s.ClientURL,
	}
	for _, kv := range s.Serial2Ticket.Cached() {
		r.Serial2Ticket.Set(kv.key, kv.val)
	}
	for k, v := range s.ticketID2tkt {
		r.ticketID2tkt[k] = v
	}
	return
}

func (s *SessionTableEntry) PreSaveHook() {
	if s == nil || s.Serial2Ticket == nil {
		// empty set; happens in auto gen_test
		return
	}
	s.Serz = make(map[int64]*Ticket)
	for _, kv := range s.Serial2Ticket.Cached() {
		s.Serz[kv.key] = kv.val
	}
}

func (s *SessionTableEntry) PostLoadHook() {
	s.Serial2Ticket = NewOmap[int64, *Ticket]()
	s.ticketID2tkt = make(map[string]*Ticket)
	for k, v := range s.Serz {
		s.Serial2Ticket.Set(k, v)
		s.ticketID2tkt[v.TicketID] = v
	}
	s.Serz = nil
}

func (s *SessionTableEntry) delTicketID(ticketID string) {
	tkt, ok := s.ticketID2tkt[ticketID]
	if !ok {
		return
	}
	delete(s.ticketID2tkt, ticketID)
	if tkt.SessionSerial == 0 {
		panic(fmt.Sprintf("cannot have 0 for SessionSerial now. tkt='%v'", tkt))
	}
	s.Serial2Ticket.Delkey(tkt.SessionSerial)
}

func (s *SessionTableEntry) delSessionSerial(sessSerial int64) {
	if sessSerial == 0 {
		panic(fmt.Sprintf("cannot have 0 for sessSerial now."))
	}
	tkt, ok := s.Serial2Ticket.Get2(sessSerial)
	if !ok {
		return
	}
	delete(s.ticketID2tkt, tkt.TicketID)
	s.Serial2Ticket.Delkey(sessSerial)
}

func newSessionTableEntry(sess *Session) *SessionTableEntry {
	return &SessionTableEntry{

		// initial--updates should happen here on the outer
		// so we get saved with state.
		SessionEndxTm:           sess.SessionIndexEndxTm,
		SessionReplicatedEndxTm: sess.SessionIndexEndxTm,
		SessRequestedInitialDur: sess.SessRequestedInitialDur,
		SessionID:               sess.SessionID,
		Serial2Ticket:           NewOmap[int64, *Ticket](),
		ticketID2tkt:            make(map[string]*Ticket),
		ClientName:              sess.CliName,
		ClientPeerID:            sess.CliPeerID,
		ClientURL:               sess.CliURL,
	}
}

// release any waiting-to-start LogIndex == 0 transactions.
// sort/submit by leader stamped order to try and
// give clients first-come first-served.
func (s *TubeNode) resubmitStalledTickets() {
	//vv("%v top of resubmitStalledTickets", s.me())

	// since the stall went into WaitingAtLeader at tube.go:2975,
	// we want to read out of WaitingAtLeader here.
	// _And_ from s.stalledMembershipConfigChangeTkt.
	var resub []*Ticket
	var both []*Ticket
	for _, tkt := range s.WaitingAtLeader.all() {
		//vv("appending to both tkt='%v'", tkt.Short())
		both = append(both, tkt)
	}
	if len(s.stalledMembershipConfigChangeTkt) > 0 {
		//both = append(both, s.stalledMembershipConfigChangeTkt...)
		//s.stalledMembershipConfigChangeTkt = nil
		// only 1?(below) or all? above. Well, we argue
		// the 2nd one will have to stall again, waiting
		// on the first one, so don't bother.
		both = append(both, s.stalledMembershipConfigChangeTkt[0])
		s.stalledMembershipConfigChangeTkt = s.stalledMembershipConfigChangeTkt[1:]
	}
	for _, tkt := range both {
		if tkt.LogIndex == 0 {
			//vv("%v doing resub for: '%v'", s.me(), tkt)
			resub = append(resub, tkt)
			tkt.Stage += ":resubmitStalledTickets_resub_LogIndex_0"
		} else {
			//vv("%v resub skipped for: '%v'", s.me(), tkt)
			tkt.Stage += ":resubmitStalledTickets_resub_skipped"
		}
	}
	//sort.Sort(ticketsInSerialOrder(resub))
	//sort.Sort(chronoOrder(resub))
	sort.Sort(stampOrder(resub))

	for _, tkt := range resub {
		//vv("%v resubmiting tkt='%v'", s.me(), tkt)
		tkt.Stage += ":resubmitStalledTickets_commandSpecificLocalActionsThenReplicateTicket"
		s.commandSpecificLocalActionsThenReplicateTicket(tkt, "resubmitStalledTickets")
	}
}

// replicateTicket is the start of a transaction
// on a leader. replicateTicket steps:
//
// (1) assign a LogIndex to the ticket;
// (2) save the ticket in a RaftLogEntry to the raft log on disk;
// (3) add the ticket to the Waiting set;
// (4) calls bcastAppendEntries() to broadcast it,
// out which sends an AppendEntries request fo
// followers.
//
// Tickets that are received before the leader's first
// no-op commits will be added to the Waiting set
// without a LogIndex assigned, since they should
// come after it. (We can identify them easily
// their with Ticket.LogIndex == 0).
//
// Once noop0 commits, we can sort throught the
// pending tickets and submit them here again.
//
// What order are tickets re-submitted in?
// 1st attempt:
// To try and preserve linearizability, we'll try and
// process them in Serial (submitted) order,
// but since Serial is assigned by a client
// process locally... we should use the leader
// to also label in timestamp order. This doesn't
// guarantee this order will be consistent between
// leader changes, of course, but before a
// log index is assigned and replicated and committed,
// there is no true order. We hope it provides
// some fairness between clients.
//
// 2nd attempt: we realized that the Serial
// order would punish old clients and reward
// new client process, so we order by the
// chronological timestamp now. This is still
// created by the client. We don't want clients
// trying to game the system for preference,
// so maybe we want something server assigned
// and FIFO... so maybe we want a separate
// ticketCreated for Serial and a
// ticketReceivedSeq for leader ordering.
//
// Details:
//
// The logic is spread out over the various message
// handlers, as distributed systems are want to do, but
// the big picture flow of the noop0 and
// any tickets it stalls is:
//
// Method becomeLeader() initiates the noop0 (setting s.initialNoop0Tkt),
// then calls us. When the noop reaches
// commitWhatWeCan(), it looks out for the noop0 in
// s.initialNoop0Tkt. Upon seeing noop0, commitWhatWeCan()
// sets s.initialNoop0HasCommitted to true and calls
// resubmitStalledTickets() who in turn will
// send us the stalled ticket in (chrono) order.
//
// Meanwhile read and write requests could hit
// the leader at any point by calling us here
// in replicateTicket(). We, the leader, must
// stall any that come before noop0 is commited, so
// that we "have a commit in our term". In addition
// to fixing the reconfig bug described by Ongaro
// on the raft-dev list in 2015, this gives
// eager/non-lazy responses to clients who may have
// been waiting for a dead leader to get their
// transaction done.
//
// PRE: If tkt.Op == MEMBERSHIP_* then the tkt.MC must
// be already set on submission. Otherwise it
// must be nil and we will copy s.state.MC for it.
func (s *TubeNode) replicateTicket(tkt *Ticket) {
	//vv("%v top of replicateTicket. Op=%v, key='%v'; val='%v'; FromID='%v'; tkt='%v'", s.me(), tkt.Op, string(tkt.Key), string(tkt.Val), rpc.AliasDecode(tkt.FromID), tkt)
	if s.role != LEADER {
		panic("replicateTicket is only for leader")
	}
	tkt.Stage += ":replicateTicket"

	doneEarly, needSave := s.leaderDoneEarlyOnSessionStuff(tkt)
	defer func() {
		if needSave {
			s.saver.save(s.state)
		}
	}()
	if doneEarly {
		return
	}

	// if this ticket gets resubmitted, the LeaderStampSN
	// will already be set, and we preserve it
	// to try and keep first come first served.
	if tkt.LeaderStampSN == 0 {
		tkt.LeaderStampSN = atomic.AddInt64(&s.prevTicketStampSN, 1)
		tkt.LeaderID = s.PeerID
		tkt.LeaderName = s.name
		tkt.LeaderURL = s.URL
	}

	var isNoop0 bool
	if !s.initialNoop0HasCommitted {
		// let the noop0 itself proceed, of course.
		if tkt == s.initialNoop0Tkt {
			//vv("%v noop0 tkt recognized: '%v'", s.me(), tkt.Desc)
			isNoop0 = true
		} else {
			// enforce safety that the leader's first noop0
			// effects: make others wait until it is commited.
			// keep the queue used in sync with tube.go:2865 in
			// resubmitStalledTickets().
			s.WaitingAtLeader.set(tkt.TicketID, tkt)
			//vv("%v stalled tkt '%v' until noop0 is done", s.me(), tkt.Desc)
			tkt.Stage += "_stalled_into_WaitAtLeader_until_noop0"
			return
		}
	}

	if !isNoop0 {
		sz := tkt.Msgsize()
		now := time.Now()
		s.batchToSubmit = append(s.batchToSubmit, tkt)

		if s.batchInProgress {
			s.batchByteSize += sz
			submit := false
			if s.batchByteSize > rpc.UserMaxPayload/4 {
				submit = true
			}
			if lte(s.batchSubmitTm, now) {
				submit = true
			}
			if len(s.batchToSubmit) >= 100 {
				submit = true
			}
			if submit {
				needSave2, didSave := s.replicateBatch()
				if didSave {
					needSave = false
				} else if needSave2 {
					needSave = true
				}
				return
			}
			// not yet time to submit this batch
			return
		}
		// start of new batch
		s.batchInProgress = true
		s.batchByteSize = sz
		s.batchStartedTm = now
		s.batchSubmitTm = now.Add(s.cfg.BatchAccumulateDur)
		s.batchSubmitTimeCh = time.After(s.cfg.BatchAccumulateDur)
		return
	}

	now := time.Now()
	var idx int64
	lli, llt := s.wal.LastLogIndexAndTerm()
	if lli > 0 {
		// try to be compaction ready
		idx = lli + 1
	} else {
		// empty log. since lli can be -1 force a 1 here
		idx = 1
	}

	entry := s.prepOne(tkt, now, idx)

	// index of log entry immediately preceeding new ones
	var prevLogIndex int64
	// term of prevLogIndex
	var prevLogTerm int64
	if lli > 0 {
		prevLogIndex = lli
		prevLogTerm = llt
	}

	// persist to disk. in replicateTicket.
	s.wal.saveRaftLogEntry(entry)

	// replicateTicket is only for the no-op zero now. Force a compaction?

	// At least this is in line with our aggressive compaction vs snapshot
	// testing. but also we saw the leader never compacting with was a concern.
	// log compaction: here in replicateTicket().

	// relax our compact every time policy now that we have tested
	// it for a while, to allow for some performance...
	if s.wal.logSizeOnDisk() > s.wal.compactionThresholdBytes { // over 6MB, then compact (if compact on).
		didCompact := s.wal.maybeCompact(s.state.LastApplied, &s.state.CompactionDiscardedLast, s)
		if !s.cfg.NoLogCompaction && !didCompact {
			_, _, err := s.wal.Compact(s.state.LastApplied, &s.state.CompactionDiscardedLast, s)
			panicOn(err)
		}
	}
	//if true {
	//s.wal.assertConsistentWalAndIndex(s.state.CommitIndex)
	//}
	if tkt.Op == MEMBERSHIP_SET_UPDATE ||
		tkt.Op == MEMBERSHIP_BOOTSTRAP {

		// single node membership changes take effect
		// immediately upon hitting the log.
		//
		// Section 4.1 on page 35:
		//
		// "The new configuration takes effect on each
		// server as soon as it is added to that server’s
		// log: the Cnew entry is replicated to the Cnew
		// servers, and a majority of the new configuration
		// is used to determine the Cnew entry’s commitment.
		// This means that servers do not wait for
		// configuration entries to be committed, and
		// each server always uses the latest
		// configuration found in its log.

		// Prov was appended above.
		amInLatest, _ := s.setMC(tkt.MC.Clone(), fmt.Sprintf("replicateTicket() on '%v'", s.name))
		_ = amInLatest

		// Log compaction means we want MC in the separate
		// persistor state on disk. So save it.
		needSave = true
	}
	if s.clusterSize() > 1 {
		// add to Waiting. NB in replicateTicket(); we are leader.
		//vv("%v wait for AppendEntries to complete; tkt='%v'", s.me(), tkt.Short())
		s.WaitingAtLeader.set(tkt.TicketID, tkt)
		tkt.Stage += "_normal_replication_added_to_WaitingAtLeader"
	} else {
		//vv("%v is singleton cluster in replicateTicket; no AppendEntries replication to wait for", s.me())

		// SPECIAL CASE COMMIT ON SINGLE NODE CLUSTER.

		// for 1 node, being on disk is committed.
		s.state.CommitIndex = idx
		s.state.CommitIndexEntryTerm = s.state.CurrentTerm
		saved := s.commitWhatWeCan(true)
		if saved {
			needSave = false // avoid another fsync
		}
		// -------- commitWhatWeCan does for us:
		// s.state.LastApplied = idx
		// entry.commited = true
		// tkt.Committed = true
		// s.FinishTicket(tkt)
		// -------- end commitWhatWeCan does for us.

		tkt.Stage += ":_cluster_size_1_immediate_commit"
		return
	}

	// send to other servers.
	es := []*RaftLogEntry{entry}
	s.bcastAppendEntries(es, prevLogIndex, prevLogTerm)
	tkt.Stage += ":_bcastAppendEntries_called"

	// end of replicateTicket
}

func (s *TubeNode) prepOne(tkt *Ticket, now time.Time, idx int64) *RaftLogEntry {

	tkt.LogIndex = idx // tkt.LogIndex is assigned here.
	if tkt.Op == SESS_NEW {
		tkt.NewSessReq.SessionAssignedIndex = idx
	}
	if tkt.Op == MEMBERSHIP_SET_UPDATE ||
		tkt.Op == MEMBERSHIP_BOOTSTRAP {

		// changeMembership() fills in tkt.MemberConfig & tkt.PrevMemberConfig
		// in a well-checked way that we want. anyone else?
		// testSetupFirstRaftLogEntryBootstrapLog() does, setting
		// the boot.blank entries. these are both okay; better
		// than what we can do below.

		// Hold up. I'm confused why this is needed?? Should not
		// all the membership work be done earlier in changeMembership?
		// We hit this when a stalled ticket was resubmitted
		// to itself as leader...maybe?
		// Stage:
		// :redirectToLeader(true,cannot_redirect_to_leader) (no leader yet)
		// :handleLocalModifyMembership_WaitingAtFollow
		// :redirectToLeader(true,sent_RedirectTicketToLeaderMsg)
		// :transfer_WAF_to_WAL
		// :resubmitStalledTickets_resub_LogIndex_0
		// :replicateTicket
		if tkt.MC == nil {
			panic(fmt.Sprintf("tkt.MC (MemberConfig) cannot be nil for a MEMBERSHIP Op here in replicateTicket! tkt=%v", tkt))
		}

		tkt.MC.addProv("replicateTicket", s.name, idx)
		//vv("%v tkt.MemberConfig = '%v'", s.me(), tkt.MC.Short())
		// addProv does for us:
		//tkt.MemberConfig.RaftLogIndex = idx

	} else {
		// not a membership action
		if tkt.MC == nil && s.state.MC != nil {
			tkt.MC = s.state.MC.Clone()
		}
		// note that all replicated tickets will have tkt.MC because
		// of the above, unless they are
		// MEMBERSHIP_SET_UPDATE or MEMBERSHIP_BOOTSTRAP.
	}
	tkt.Term = s.state.CurrentTerm
	entry := &RaftLogEntry{
		Tm:                 now,
		LeaderName:         s.name,
		Term:               s.state.CurrentTerm,
		Index:              idx,
		Ticket:             tkt,
		CurrentCommitIndex: s.state.CommitIndex,
		node:               s,
	}
	tkt.RaftLogEntryTm = entry.Tm
	if tkt.LeaseRequestDur > 0 {
		tkt.LeaseUntilTm = entry.Tm.Add(tkt.LeaseRequestDur)
	}
	return entry
}

func (s *TubeNode) replicateBatch() (needSave, didSave bool) {

	//vv("%v top of replicateBatch with %v in batch", s.me(), len(s.batchToSubmit))
	batch := s.batchToSubmit
	s.batchToSubmit = nil
	s.batchSubmitTimeCh = nil
	s.batchInProgress = false
	s.batchByteSize = 0
	if len(batch) == 0 {
		return
	}

	now := time.Now()
	clusterSz := s.clusterSize()
	var idx int64

	lli, llt := s.wal.LastLogIndexAndTerm()
	if lli > 0 {
		// try to be compaction ready
		idx = lli + 1
	} else {
		// empty log. since lli can be -1 force a 1 here
		idx = 1
	}

	var prevLogIndex, prevLogTerm int64
	if lli > 0 {
		prevLogIndex = lli
		prevLogTerm = llt
	}

	idx-- // allow the for loop just below to increment for each tkt.

	var es []*RaftLogEntry
	for _, tkt := range batch {

		// should be checked before ever batching.
		//if s.leaderDoneEarlyOnSessionStuff(tkt) {
		//	continue
		//}
		// else replicate it
		idx++
		entry := s.prepOne(tkt, now, idx)

		if tkt.Op == MEMBERSHIP_SET_UPDATE ||
			tkt.Op == MEMBERSHIP_BOOTSTRAP {

			// Prov was appended above.
			s.setMC(tkt.MC.Clone(), fmt.Sprintf("replicateTicket() on '%v'", s.name))

			// Log compaction means we want MC in persistor on disk.
			needSave = true
		}
		es = append(es, entry)
	}

	// persist all es entries to disk, in one fsync instead of the range loop
	last := len(es) - 1
	var doFsync bool
	for i, entry := range es {
		doFsync = (i == last)
		s.wal.saveRaftLogEntryDoFsync(entry, doFsync) // in replicateBatch
	}
	// we compact aggressively for better testing of compaction vs appendEntries logic.
	// TODO: uncomment below to only compact occassionally, since it
	// involves lots of slow fsyncs and file rewrites. but less testing.
	if s.wal.logSizeOnDisk() > s.wal.compactionThresholdBytes { // over 6MB, then compact (if compact on).
		s.wal.maybeCompact(s.state.LastApplied, &s.state.CompactionDiscardedLast, s)
	}

	if clusterSz > 1 {
		for _, tkt := range batch {
			s.WaitingAtLeader.set(tkt.TicketID, tkt)
			tkt.Stage += ":_batch_normal_replication_added_to_WaitingAtLeader"
		}
	} else {
		//vv("%v is singleton cluster in replicateTicket; no AppendEntries replication to wait for", s.me())

		// SPECIAL CASE COMMIT ON SINGLE NODE CLUSTER.

		// for 1 node, being on disk is committed.
		s.state.CommitIndex = idx
		s.state.CommitIndexEntryTerm = s.state.CurrentTerm
		if didSave = s.commitWhatWeCan(true); didSave {
			needSave = false
		}
		for _, tkt := range batch {
			tkt.Stage += ":_batch_cluster_size_1_immediate_commit"
		}
		return
	}

	// send to other servers.
	s.bcastAppendEntries(es, prevLogIndex, prevLogTerm)
	for _, tkt := range batch {
		tkt.Stage += ":_batch_bcastAppendEntries_called"
	}
	return
}

type testTermChange struct {
	peerID  string
	oldterm int64
	newterm int64
}

const SAVE_STATE = true
const SKIP_SAVE = false

func (s *TubeNode) becomeFollower(term int64, mc *MemberConfig, save bool) {
	//vv("%v becomeFollower. new term = %v. was: %v; stack=\n%v", s.me(), term, s.state.CurrentTerm, stack())
	if s.role == LEADER {
		s.lastLeaderActiveStepDown = time.Now()
	}
	if term < s.state.CurrentTerm {
		// we cannot allow follower to stay follower
		// even at current term, because we clear our votes below!
		panic(fmt.Sprintf("%v becomeFollower violates Raft rule: must have monotone term increses: curTerm = %v, next = %v", s.me(), s.state.CurrentTerm, term))
	}
	// INVAR: term >= s.state.CurrentTerm
	termIncrease := (term > s.state.CurrentTerm)

	oldRole := s.role
	//vv("%v becomeFollower so nil out s.leaderSendsHeartbeatsCh", s.name)
	s.leaderSendsHeartbeatsCh = nil
	s.leaderSendsHeartbeatsDue = time.Time{}
	s.role = FOLLOWER

	if termIncrease && s.isTest() {
		select {
		case panicAtCap(s.cfg.testCluster.termChanges) <- &testTermChange{
			peerID:  s.PeerID,
			oldterm: s.state.CurrentTerm,
			newterm: term,
		}:
		case <-s.Halt.ReqStop.Chan:
			return
		}
	}

	// clear out leader-only state
	s.initialNoop0Tkt = nil // let it be garbage collected
	s.initialNoop0HasCommitted = false

	mcVersionUpdate := false
	if mc != nil &&
		s.state != nil &&
		s.state.MC != nil &&
		mc.VersionGT(s.state.MC) {

		mcVersionUpdate = true
		upd := mc.CloneForUpdate(s)
		provdesc := fmt.Sprintf("%v becomeFollower mc.VersionGT(cur): mc.ConfigVersion=%v; mc.ConfigTerm=%v", s.me(), mc.ConfigVersion, mc.ConfigTerm)
		s.setMC(upd, provdesc) // becomeFollower
		//upd.addProv(provdesc, s.name, upd.RaftLogIndex, upd.IsCommitted)
		//s.state.MC = upd // becomeFollower
	}

	if termIncrease || mcVersionUpdate {
		s.state.CurrentTerm = term
		s.state.VotedFor = ""
		s.state.VotedForName = ""
		s.state.HaveVoted = false
		s.state.HaveVotedTerm = 0

		// discard as stale (section 6.2)
		// "[Followers] must discard [who is the leader cache] when
		// starting a new election or when the term changes.
		// Otherwise, they might needlessly delay clients (for
		// example, it would be possible for two servers to redirect
		// to each other, placing clients in an infinite loop)."
		s.leaderID = ""
		s.leaderName = ""
		s.leaderURL = ""
	}
	s.resetElectionTimeout("becomeFollower")
	// stale out any in-progress pre-vote that is racing.
	s.resetPreVoteState(false, true) // in becomeFollower().

	// must persist our CurrentTerm to disk.
	if save {
		s.saver.save(s.state)
	}
	if oldRole == LEADER {
		s.tellClientsImNotLeader()
	}
}

func (s *TubeNode) electionTimeoutDur() time.Duration {
	var T time.Duration
	if s.cfg.MinElectionDur > 0 {
		T = s.cfg.MinElectionDur
		if T < 10*time.Millisecond {
			panic(fmt.Sprintf("%v leader election timeout cannot be < 10 msec. Fix cfg.MinElectionDur = %v", s.me(), s.cfg.MinElectionDur))
		}
	} else {
		// default to 150ms - 300ms
		T = 150 * time.Millisecond
	}
	// compute T + a random fraction of T

	// avoid the biased sampling of straight modulo here:
	// see https://stackoverflow.com/questions/10984974/why-do-people-say-there-is-modulo-bias-when-using-a-random-number-generator
	// https://research.kudelskisecurity.com/2020/07/28/the-definitive-guide-to-modulo-bias-and-how-to-avoid-it/
	frac := float64(cryptoRandNonNegInt64Range(1e8)) / 1e8 // unbiased

	spread := float64(max(1, s.clusterSize()-2)) // automatically widen when more nodes
	r := time.Duration(float64(T) * spread * frac)
	dur := T + r // somewhere in [T, 2T] uniformly.
	return dur
}

func (s *TubeNode) maxElectionTimeoutDur() time.Duration {
	var T time.Duration
	if s.cfg.MinElectionDur > 0 {
		T = s.cfg.MinElectionDur
		if T < 10*time.Millisecond {
			panic(fmt.Sprintf("%v leader election timeout cannot be < 10 msec. Fix cfg.MinElectionDur = %v", s.me(), s.cfg.MinElectionDur))
		}
	} else {
		// default to 150ms - 300ms
		T = 150 * time.Millisecond
	}
	return 2 * T
}

func (s *TubeNode) minElectionTimeoutDur() time.Duration {
	var T time.Duration
	if s.cfg.MinElectionDur > 0 {
		T = s.cfg.MinElectionDur
		if T < 10*time.Millisecond {
			panic(fmt.Sprintf("%v leader election timeout cannot be < 10 msec. Fix cfg.MinElectionDur = %v", s.me(), s.cfg.MinElectionDur))
		}
	} else {
		// default to 150ms - 300ms
		T = 150 * time.Millisecond
	}
	return T
}

func (s *TubeNode) resetElectionTimeout(where string) time.Duration {
	dur := s.electionTimeoutDur()
	now := time.Now()
	s.nextElection = now.Add(dur)
	//vv("%v resetElectionTimeout(%v) top. called from '%v'; s.nextElection in %v, at %v", s.me(), where, fileLine(2), dur, nice(s.nextElection))
	s.electionTimeoutCh = time.After(dur)
	s.electionTimeoutSetAt = now
	s.electionTimeoutWasDur = dur
	// keep separate! no: s.resetPreVoteState() otherwise 050 times out.
	return dur
}

// become a candidate, send out RequestVote
// Two elections in a row due to the election
// timer should NOT increment the term again!
func (s *TubeNode) beginElection() {
	_, weOK := s.state.MC.PeerNames.Get2(s.name)
	if !weOK {
		//vv("%v in beginElection, mongo ignore: do not start an election since I am not in MC", s.me())
		return
	}
	// not enough info yet, log below instead.
	//vv("%v \n-------->>>    beginElection()  <<<--------\n", s.me())

	s.resetPreVoteState(true, false)
	//vv("%v beginElection so nil out s.leaderSendsHeartbeatsCh", s.name)
	s.leaderSendsHeartbeatsCh = nil

	// causing problems for 402 build up from 1...
	// a new nodes think they are leadership material??
	// maybe we need to set a really high configured membership count.
	if s.clusterSize() == 1 {
		//vv("%v beginElection sees single node! about to becomeLeader directly... c.clusterSize() = 1; s.clusterSize()=%v; s.state.MC = %v", s.me(), s.clusterSize(), s.state.MC)

		// AM I THE DESIGNATED LEADER?
		amDesignated := (s.cfg.InitialLeaderName == s.name)
		_ = amDesignated

		// AM I THE EVEN IN THE CLUSTER; let alone the designated leader?
		_, iAmReplica := s.state.MC.PeerNames.Get2(s.name)
		//vv("%v iAmReplica = %v; amDesignated = %v; (s.cfg.isTest && s.cfg.testNum < 400) = %v; s.state.MC.PeerNames = '%v'; s.cfg.InitialLeaderName = '%v'", s.me(), iAmReplica, amDesignated, (s.cfg.isTest && s.cfg.testNum < 400), s.state.MC, s.cfg.InitialLeaderName)

		//if (iAmReplica && amDesignated) || (s.cfg.isTest && s.cfg.testNum < 400) {
		// seems too restrictive for live prod single node cluster.
		// we should not be requiring amDesignated!
		if iAmReplica || (s.cfg.isTest && s.cfg.testNum < 400) {
			// Test001_no_replicas_write_new_value tube_test.go.

			// no point in waiting for votes because it
			// is only us, all alone, and there is no one to ask.
			s.becomeLeader()
			return
		}
	}

	// If we are a candidate, then we have
	// already incremented our term,
	// do not do so again. We've seen two
	// election timeouts in a row, but
	// (with pre-voting) continually
	// incrementing our term would be bad.
	if s.role == FOLLOWER {
		s.state.CurrentTerm++
	}
	s.role = CANDIDATE
	s.state.VotedFor = s.PeerID
	s.state.VotedForName = s.name
	s.state.HaveVoted = true
	s.state.HaveVotedTerm = s.state.CurrentTerm

	// discard as stale (section 6.2)
	s.leaderID = ""
	s.leaderName = ""
	s.leaderURL = ""

	// vote for self. but per section 4.2.2, only
	// if we are in current config!
	if s.weAreMemberOfCurrentMC() {
		s.votes = map[string]bool{s.PeerID: true}
		s.yesVotes = 1
		s.noVotes = 0
	} else {
		s.votes = map[string]bool{}
		s.yesVotes = 0
		s.noVotes = 0
	}

	// as candidate! so votes can tell
	// use import data about what others have,
	// like their PeerLogTermsRLE.
	s.candidateReinitFollowerInfo()

	//vv("%v \n-------->>>    in begin Election()  <<<--------\n", s.me())

	if s.wal == nil {
		panic("huh? wal is nil?")
	}
	lastLogIndex, lastLogTerm := s.wal.LastLogIndexAndTerm()
	// lli=-1, llt=0 if empty log.
	if lastLogIndex < 0 {
		lastLogIndex = 0
	}
	//k := len(s.wal.RaftLog)
	//if k > 0 { // might be empty on startup
	//	lastLogIndex = s.wal.RaftLog[k-1].Index
	//	lastLogTerm = s.wal.RaftLog[k-1].Term
	//}

	// send RequestVote to all
	rv := &RequestVote{
		ClusterID:           s.ClusterID,
		FromPeerID:          s.PeerID,
		FromPeerName:        s.name,
		FromPeerServiceName: s.PeerServiceName,
		CandidatesTerm:      s.state.CurrentTerm,
		CandidateID:         s.PeerID,
		LastLogIndex:        lastLogIndex,
		LastLogTerm:         lastLogTerm,
		MC:                  s.state.MC,
		SenderHLC:           s.hlc.CreateSendOrLocalEvent(),
	}

	// we only vote for ourselves if we are in current MC
	if s.weAreMemberOfCurrentMC() {
		// are we a singleton and so done?
		if s.clusterSize() <= 1 {
			//vv("%v we are singleton, skipping request votes", s.me())
			selfVote := &Vote{
				ClusterID:           s.ClusterID,
				FromPeerID:          s.PeerID,
				FromPeerName:        s.name,
				FromPeerServiceName: s.PeerServiceName,
				FromPeerCurrentTerm: s.state.CurrentTerm,
				FromPeerCurrentLLI:  s.lastLogIndex(),
				FromPeerCurrentLLT:  s.lastLogTerm(),
				CandidateID:         s.PeerID,
				CandidatesTerm:      s.state.CurrentTerm,
				VoteGranted:         true,
				MC:                  s.state.MC,
				SenderHLC:           s.hlc.CreateSendOrLocalEvent(),
			}
			if selfVote.MC == nil {
				panic("fix this")
			}
			s.tallyVote(selfVote)
			return
		}
	}

	rvFrag := s.newFrag()
	bts, err := rv.MarshalMsg(nil)
	panicOn(err)
	rvFrag.Payload = bts
	rvFrag.FragOp = RequestVoteMsg
	rvFrag.FragSubject = "RequestVote"

	// begin election, so use cktReplica not ckt.
	left := len(s.cktReplica) - 1
	for _, cktP := range sorted(s.cktReplica) {

		//vv("%v requesting vote from %v", s.me(), rpc.AliasDecode(peer))
		ckt := cktP.ckt
		err = s.SendOneWay(ckt, rvFrag, -1, left)
		_ = err // don't panic on halting.
		if err != nil {
			alwaysPrintf("%v non nil error '%v' on Vote to '%v'", s.me(), err, ckt.RemotePeerID)
		}
		left--
	}
	s.resetElectionTimeout("end of beginElection")
}

// includes ourselves in the count (if we are in s.state.MC)
func (s *TubeNode) clusterSize() int {

	// chapter 4 on membership changes:

	// (but avoid crash on tube_test Test000_election_timeout_dur,
	// where s.stat is not set...)
	if s.state != nil {
		if s.state.MC != nil {
			return s.state.MC.PeerNames.Len()
		}
	}
	if !s.cfg.isTest {
		panicf("we want to always use s.state.MC.PeerNames.Len() in prod, but it is nil!")
	}
	// too noisy on tests like Test000_election_timeout_dur
	//vv("%v warning! s.state.MC is nil returning static s.cfg.ClusterSize: %v because s.state.MC is not available.", s.name, s.cfg.ClusterSize)
	return s.cfg.ClusterSize
}

func (s *TubeNode) quorum() int {
	tot := s.clusterSize()
	if s.cfg.NoFaultTolDur > 0 {
		if s.t0.Add(s.cfg.NoFaultTolDur).After(time.Now()) {
			return tot
		}
	}
	return (tot / 2) + 1
}

func (s *TubeNode) quorumIgnoreNoFaultTolDur() int {
	tot := s.clusterSize()
	return (tot / 2) + 1
}

func (s *TubeNode) quorumAndIsNoFaultTolDur() (quor int, isNoFaultTolDur bool) {
	tot := s.clusterSize()
	if s.cfg.NoFaultTolDur > 0 {
		if s.t0.Add(s.cfg.NoFaultTolDur).After(time.Now()) {
			isNoFaultTolDur = true
			quor = tot
			return
		}
	}
	quor = (tot / 2) + 1
	return
}

func (s *TubeNode) votesString(votes map[string]bool) (r string) {
	r = "{"
	for who, yes := range votes {
		y := "no"
		if yes {
			y = "yes"
		}
		r += fmt.Sprintf("[%v %v]", rpc.AliasDecode(who), y)
	}
	r += "}"
	return
}

// me: safety_test 050 looks at split pre-vote resolution.
// Does a successful pre-vote proceed to a real election,
// where the election randomization should avoid ties. So I'm thinking:
// the initial pre-vote is independent, but if won,
// then apply a randomized wait period after the pre-vote
// is won, but before becoming a candidate for a real
// election. I want to avoid the endless rounds of
// pre-voting that cross talk and prevent even getting
// to a real election. But that real election should
// also have a good chance of suceeding. So it seems
// that we need randomization after a successful pre-vote.
//
// REMINDER: be idempotent even after quorum! we'll see
// lots of extra pre-votes.
func (s *TubeNode) tallyPreVote(vote *Vote) {
	//vv("%v \n-------->>>    tally PreVote()  <<<--------\n vote = %v", s.me(), vote)

	s.hlc.ReceiveMessageWithHLC(vote.SenderHLC)

	// update lastContactTm
	cktP, ok := s.cktall[vote.FromPeerID]
	if ok {
		// in tallyPreVote
		cktP.seen(vote.MC, vote.FromPeerCurrentLLI, vote.FromPeerCurrentLLT, vote.FromPeerCurrentTerm)
	}

	if vote.CandidatesTerm != s.preVoteTerm {
		// once we have set our s.preVoteTerm back to 0
		// it will cause late arriving votes to be dropped here.
		//vv("%v dropping pre-vote not equal to PreVoteTerm %v: '%v'", s.me(), s.preVoteTerm, vote)
		// in CANDIDATE state, observed:
		// preVote.Term == 1, but our preVoteTerm = 0, should we become follower?
		return
	}

	if s.role == LEADER {
		//vv("%v ignore pre-vote received since already LEADER", s.me())
		return
	}
	if s.role == CANDIDATE {
		//vv("%v ignore pre-vote received since already CANDIDATE. ignored vote='%v'", s.me(), vote)
		return
	}
	// INVAR: in FOLLOWER role. In tallyPreVote()
	//vv("%v in tally pre-vote, we are follower", s.me())

	// make sure we have not otherwise
	// advanced the current term in the meantime
	if s.preVoteTerm != s.state.CurrentTerm+1 {
		//vv("%v pre-vote sanity check failed/pre-vote is discarded: s.preVoteTerm(%v) != s.state.CurrentTerm+1 = %v", s.me(), s.preVoteTerm, s.state.CurrentTerm+1)
		s.resetPreVoteState(false, true)
		return
	}

	if vote.CandidateID == "" {
		panic(fmt.Sprintf("pre-vote must have CandidateID; vote= '%v'", vote))
	}
	if vote.FromPeerName == "" {
		panic(fmt.Sprintf("pre-vote must have FromPeerName; vote= '%v'", vote))
	}
	if vote.FromPeerID == "" {
		panic(fmt.Sprintf("pre-vote must have FromPeerID; vote= '%v'", vote))
	}

	// this seems to be preventing alot of voting in 401
	// as we try to remove node_5. node_5 is rejecting
	// pre-votes with later MC that do not include node_5.
	// But that seems correct: we want node_5 out so
	// it should not be able to win a pre-vote.
	//
	// mongo reconfig: is our config >= their config. If not, exit.
	if vote.MC != nil && vote.MC.VersionGT(s.state.MC) {
		//vv("%v mongo ignore pre-vote from: '%v' since vote.MC(%v) > s.state.MC(%v); vote.VoteGranted=%v", s.me(), vote.FromPeerName, vote.MC.Short(), s.state.MC.Short(), vote.VoteGranted)
		return
	}

	// mongo reconfig: are both we and the voter in
	// our current config?
	//
	// Update: actually, per section 4.2.2 of the Raft dissertation,
	// I don't think we want to require that we
	// ourselves (the leader) are in the current MC,
	// since for liveness we may have to manage a
	// cluster we are leaving for a while longer.
	//_, weOK := s.state.MC.PeerNames.Get2(s.name)
	//if !weOK {
	//	vv("%v in tallyPreVote, mongo ignore pre-vote received since I am not in MC", s.me())
	//	return
	//}

	// Is the voter in our current config? (checks our own vote too).
	_, voterOK := s.state.MC.PeerNames.Get2(vote.FromPeerName)
	if !voterOK {
		//vv("%v mongo ignore pre-vote received since voter is not in MC: '%v'", s.me(), vote.FromPeerName)
		return
	}

	// is self vote? accept even though self never in cktall.
	// to allow single node cluster to elect its only node.
	fromSelf := s.PeerID == vote.FromPeerID

	// double check that they were intending to vote for us.
	if vote.CandidateID == s.PeerID {
		// dedup votes
		_, already := s.preVotes[vote.FromPeerName]
		if !already {
			// are they live?
			// Per Ch 4, we allow voting for non-replicas, to
			// allow configuration changes to work.
			_, alive := s.cktall[vote.FromPeerID]
			if alive || fromSelf { // since self is never in cktall
				s.preVotes[vote.FromPeerName] = vote.VoteGranted
				//vv("%v pre-vote from '%v' is '%v'", s.name, vote.FromPeerName, vote.VoteGranted)
				if vote.VoteGranted {
					s.yesPreVotes++
				} else {
					s.noPreVotes++
				}
			} else {
				alwaysPrintf("%v warning! drat! not live vote-- arg: '%v'", s.me(), vote)
			}
		}
	}

	quor := s.quorum()
	switch {
	case s.yesPreVotes >= quor:
		//vv("%v won the pre-vote quorum succeeded with %v of %v votes, wait one election dur to begin real election; preVotes='%v'", s.me(), quor, s.clusterSize(), s.votesString(s.preVotes))
		// great. phase one complete.
		// begin phase two, waiting for a random election timeout
		// to complete, just like in regular raft. Two
		// random timeouts in a row means the same
		// sequence of two timeouts on two
		// separate nodes would be needed to split vote;
		// a very low probability event.

		now := time.Now()
		s.preVotePhase1EndedAt = now
		s.preVotePhase2Began = now

		dur := s.electionTimeoutDur()
		s.preVoteOkLeaderElecDeadline = now.Add(dur)
		// notice that this sets a timer
		s.preVoteOkLeaderElecTimeoutCh = time.After(dur)
		s.preVoteOkLeaderElecTerm = s.preVoteTerm
		s.preVotePhase2EndsBy = s.preVoteOkLeaderElecDeadline
		s.preVotePhase2EndedAt = time.Time{}

		// we don't need any more pre-votes, just drop them
		// as of the end of phase 1, begin phase 2 of election.
		s.preVoteTerm = 0
		// is this needed?
		//s.resetElectionTimeout("tallyPreVote")

		return
	case s.noPreVotes >= quor:
		//vv("%v pre-vote quorum failed, saw quorum %v of %v no; s.preVotes='%#v'", s.me(), quor, s.clusterSize(), s.votesString(s.preVotes))
		// ignore any further pre-votes that come in preVoteTerm
		s.resetPreVoteState(false, false)
		s.resetElectionTimeout("tallyPreVote noPreVotes >= quor")
	case s.yesPreVotes+s.noPreVotes >= s.clusterSize():
		// have to let 2 node clusters try again once pre-vote has failed,
		// 1 yes (from self), 1 no from other (in split vote, 050 safety_test).
		//vv("%v pre-vote concluded and failed, cluster size %v, preVotes='%v'", s.me(), s.clusterSize(), s.votesString(s.preVotes))
		s.resetPreVoteState(false, false)
		s.resetElectionTimeout("tallyPreVote pre-vote has failed for sure")
	default:
		//vv("%v pre-vote quorum not met: s.yesPreVotes(%v), s.noPreVotes(%v); s.quor = %v; ClusterSize=%v; s.preVotes='%#v'", s.me(), s.yesPreVotes, s.noPreVotes, quor, s.clusterSize(), s.votesString(s.preVotes))
	}
	// end of tallyPreVote()
}

func (s *TubeNode) tallyVote(vote *Vote) {
	//vv("%v \n-------->>>    tally Vote()  <<<--------\n cur votes = %v \n vote = %v", s.me(), s.votesString(s.votes), vote)

	s.hlc.ReceiveMessageWithHLC(vote.SenderHLC)

	// As an extra safety precaution, let's be sure
	// all the votes are from unique peers, _and_ that
	// we still have live circuits for them, so
	// they aren't zombie/ghost votes from the past.

	// update lastContactTm
	cktP, ok := s.cktall[vote.FromPeerID]
	if ok {
		// in tallyVote
		cktP.seen(vote.MC, vote.FromPeerCurrentLLI, vote.FromPeerCurrentLLT, vote.FromPeerCurrentTerm)
	}

	if vote.FromPeerServiceName != TUBE_REPLICA {
		return
	}

	// Regular vote handling
	if vote.CandidatesTerm != s.state.CurrentTerm {
		if vote.CandidatesTerm < s.state.CurrentTerm {
			//vv("%v tallyVote: ignoring vote for me in wrong term: '%#v'", s.me(), vote)
			return
		}
		//vv("%v tallyVote: stand down, become follower with higher term: '%#v'", s.me(), vote)

		// INVAR: vote.Term > s.state.CurrentTerm
		s.becomeFollower(vote.CandidatesTerm, vote.MC, SAVE_STATE)
		return
	}

	if s.role == LEADER {
		//vv("%v tallyVote: ignore extra votes as leader? track something else?", s.me())
		return
	}
	// INVAR: not leader
	if s.role == FOLLOWER {
		//vv("%v tallyVote: ignoring votes as follower. vote = '%v'", s.me(), vote)

		// page 16
		// "A server reamins in the follower state as long as it
		// receives valid RPCs from a leader or a candidate"
		s.resetElectionTimeout("tallyVote, am follower, not leader")
		return
	}
	// INVAR: we are a candidate
	//vv("%v in tallyVote, we are a candidate...", s.me()) // never seen 050 no elec

	if vote.CandidateID == "" {
		panic(fmt.Sprintf("vote must have CandidateID; vote= '%v'", vote))
	}
	if vote.FromPeerName == "" {
		panic(fmt.Sprintf("vote must have FromPeerName; vote= '%v'", vote))
	}
	if vote.FromPeerID == "" {
		panic(fmt.Sprintf("vote must have FromPeerID; vote= '%v'", vote))
	}

	// mongo reconfig: are both we and the voter in
	// our current config? update: per 4.2.2 I think
	// we must allow elections without ourselves.
	//_, weOK := s.state.MC.PeerNames.Get2(s.name)
	//if !weOK {
	//	//vv("%v mongo ignore vote received since I am not in MC", s.me())
	//	return
	//}
	_, voterOK := s.state.MC.PeerNames.Get2(vote.FromPeerName)
	if !voterOK {
		//vv("%v mongo ignore vote received since voter is not in MC: '%v'", s.me(), vote.FromPeerName)
		return
	}
	// mongo reconfig: is our config >= their config
	if vote.MC != nil && vote.MC.VersionGT(s.state.MC) {
		//vv("%v mongo ignore vote received since vote.MC.VersionGT(our MC); from: '%v'", s.me(), vote.FromPeerName)
		if vote.VoteGranted {
			return
		}
	}
	// mongo reconfig: our new term must be > voter's term.
	if vote.FromPeerCurrentTerm >= s.state.CurrentTerm {
		//vv("%v ignore vote: vote.FromPeerCurrentTerm(%v) >= s.state.CurrentTerm(%v)", s.me(), vote.FromPeerCurrentTerm, s.state.CurrentTerm)
		if vote.VoteGranted {
			return
		}
	}
	// adopt their config? nope! wait for ae. else bad things happen.

	fromSelf := s.PeerID == vote.FromPeerID

	// double check that they were intending to vote for us.
	if vote.CandidateID == s.PeerID {
		// dedup votes by Name not PeerID so even if they restart they
		// don't get to vote twice!
		_, already := s.votes[vote.FromPeerName]
		if !already {
			// are they live? do we have their peer info?
			_, alive := s.cktall[vote.FromPeerID]
			if alive || fromSelf {
				s.votes[vote.FromPeerName] = vote.VoteGranted
				//vv("%v tallyVote from '%v' is '%v'", s.name, vote.FromPeerName, vote.VoteGranted)
				if vote.VoteGranted {
					s.yesVotes++
				} else {
					s.noVotes++
				}
				info, ok := s.peers[vote.FromPeerID]
				if !ok {
					info = s.newRaftNodeInfo(vote.FromPeerID, vote.FromPeerName, vote.FromPeerServiceName, vote.FromPeerServiceNameVersion)
					s.peers[vote.FromPeerID] = info
				}
				info.MC = vote.MC // tallyVote
			}
		}
	}

	quor := s.quorum()
	if s.yesVotes >= quor {
		//vv("%v tallyVote (I won vote: am elected!) result: %v yesVotes >= quorum %v, becoming leader of term %v with s.votes='%v'", s.me(), s.yesVotes, quor, s.state.CurrentTerm, s.votesString(s.votes))

		s.becomeLeader()
		return
	}
	if s.noVotes >= quor {
		//vv("%v tallyVote (I lost) result: %v noVotes >= quorum %v, becoming follower without advancing term; s.votes='%v'", s.me(), s.noVotes, quor, s.votesString(s.votes))
		// this might be a post-quorum duplicate extra message
		// if we already stepped down.
		if s.role != FOLLOWER {
			s.becomeFollower(s.state.CurrentTerm, nil, SKIP_SAVE)
		}
		return
	}
	//vv("%v tallyVote (inconclusive): yesVotes=%v; noVotes=%v; quorum=%v ; s.votes='%v'", s.me(), s.yesVotes, s.noVotes, quor, s.votesString(s.votes))
}

func (s *TubeNode) leaderBeatDur() time.Duration {
	//if s.cfg.HeartbeatDur <= 0 {
	//	return time.Millisecond * 15
	//}
	return s.cfg.HeartbeatDur
}

func (s *TubeNode) resetLeaderHeartbeat(where string) {
	//vv("%v top resetLeaderHeartbeat. where='%v'", s.me(), where)

	if !s.leaderSendsHeartbeatsDue.IsZero() {
		elap := time.Since(s.leaderSendsHeartbeatsDue)
		if !s.leaderSendsHeartbeatsDue.IsZero() && elap > 300*time.Millisecond {
			//alwaysPrintf("%v warning! elap=%v between resetLeaderHeartbeat() calls.", s.me(), elap)
			alwaysPrintf("%v warning! elap=%v between resetLeaderHeartbeat() calls.", s.name, elap)
		}
	}
	dur := s.leaderBeatDur()
	s.leaderSendsHeartbeatsDue = time.Now().Add(dur)
	s.leaderSendsHeartbeatsCh = time.After(dur)
	//if s.cfg.testNum != 52 && s.cfg.testNum != 51 {
	//vv("%v resetLeaderHeartbeat called from %v, next due in '%v'", s.name, where, dur)
	//}
}

func (s *TubeNode) becomeLeader() {
	vv("%v becomeLeader top", s.me())
	//defer func() {
	//	vv("%v end of becomeLeader", s.me())
	//}()

	if s.role == LEADER {
		panic(fmt.Sprintf("%v I am already leader, doing this again will mess up leaderElectedTm", s.me()))
	}
	s.leaderElectedTm = time.Now()

	// beginElec=false, haveLeader=true
	s.resetPreVoteState(false, true)

	s.role = LEADER
	s.leaderID = s.PeerID
	s.leaderName = s.name
	s.leaderURL = s.URL

	// Section 4.1, page 36: "Unfortunately, this
	// decision does imply that a log entry for a configuration
	// change can be removed (if leadership changes);
	// in this case, a server must be prepared to
	// fall back to the previous configuration in its log."
	// side effect: sets s.state.MC from log
	// if available.
	// This sets s.state.MC and returns it.
	// Moreover  s.state.CurrentEpoch is set from newestMemCfg.ConfigVersion.
	if s.state != nil {
		if s.state.MC != nil {
			if s.state.CurrentTerm < s.state.MC.ConfigTerm {
				panic(fmt.Sprintf("should be impossible to have leader term less than latest config term, right? s.state.CurrentTerm=%v; s.state.MC.ConfigTerm=%v", s.state.CurrentTerm, s.state.MC.ConfigTerm))
			}
			if s.state.MC.ConfigTerm != s.state.CurrentTerm {
				//vv("%v updating s.state.MC.ConfigTerm(%v) <- s.state.CurrentTerm(%v)", s.me(), s.state.MC.ConfigTerm, s.state.CurrentTerm)

				if s.state.MC.ConfigTerm > s.state.CurrentTerm {
					panic(fmt.Sprintf("what? should never happen: becomeLeader sees s.state.MC.ConfigTerm(%v) > s.state.CurrentTerm(%v)", s.state.MC.ConfigTerm, s.state.CurrentTerm))
				}

				// from https://github.com/will62794/logless-reconfig/blob/master/notes/raft_reconfig_bug/raft_reconfig_bug.pdf
				// prevent divergence by rewriting the confirm term
				// on leader election, then having that be logless-committed.

				// page 6, Algorithm 1, line 31 of mongo paper
				// configTerm <- term[i] + 1 (the new term)
				// "Rewrite the current config with the
				// latest term on winning elections"
				// from Day 5 of
				// https://conf.tlapl.us/2024/SiyuanZhou-HowWeDesignedAndModelCheckedMongoDBReconfigurationProtocol.pdf
				// Note that the noop0 ticket we replicate
				// immedately below will convey this new MC
				// to followers promptly.
				s.state.MC.ConfigTerm = s.state.CurrentTerm
			} else {
				// INVAR: s.state.MC.ConfigTerm == s.state.CurrentTerm
				// single node trying to become leader of itself
				// gets to here.

				if s.weAreMemberOfCurrentMC() && s.clusterSize() == 1 {
					// we are the only possible leader.
					s.state.CurrentTerm++
					s.state.MC.ConfigTerm = s.state.CurrentTerm
				} else {
					// not sure about other cases.
					panicf("already equal? should we advance the MC.ConfirmTerm? we would have to advance our term too. s.state.MC.ConfigTerm == s.state.CurrentTerm(%v)", s.state.MC.ConfigTerm)
				}
			}
		}
	}
	// note: s.saver.save(s.state) will happen at the end.

	s.resetElectionTimeout("becomeLeader")
	s.preVoteOkLeaderElecTimeoutCh = nil
	s.countLeaderHeartbeat = 0
	s.leaderFullPongPQ.deleteAll()
	s.lastBecomeLeaderTerm = s.state.CurrentTerm

	//s.nextElection = time.Time{}
	s.readIndexOptim = 0
	//vv("%v becomeLeader of term %v, with cluster s.state.MC = '%v'", s.me(), s.state.CurrentTerm, s.state.MC)

	// election_test.go	020 confirms leader was elected.
	if s.isRegularTest() {
		select {
		case panicAtCap(s.cfg.testCluster.LeaderElectedCh) <- s.leaderName:
			//vv("submitted on LeaderElectedCh: s.leaderName = '%v'", s.leaderName)
		default:
			//vv("could not submit on LeaderElectedCh: s.leaderName = '%v'", s.leaderName)
		}
	}

	// should we skip this for single node?

	// immediately send 1st heartbeat and establish connectivity:
	// makes 401 hang! comment the s.leaderSendsHeartbeats() just below, and it goes green.
	// also without commenting, we get a ton of lost timers:
	// simnet.go:1906 [goID 20] 2000-01-01 00:00:08.767100020 +0000 UTC defer dispatchTimer re-queuing undeliverable timer: mop{SERVER(srv_node_5) TIMER init:-5.004399943s, arr:unk, complete:-4.299943ms op.sn:164, who:77, msg.sn:0 timer set at pump.go:254}
	//so just no: s.leaderSendsHeartbeats()

	// leaderSendsHeartbeats does this at the end:
	s.resetLeaderHeartbeat("becomeLeader")

	// already did this as candidate
	//s.leaderReinitFollowerInfo()

	// we've got to get a no-op *commited* (not just replicated) in order to
	// do any configuration changes, and it is typical to do
	// this when the new leader starts (for other reasons I think...)
	//
	// Ongaro discussing the 2015 bug in single-membership changes:
	// https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E/m/d2D9LrWRza8J
	//
	// "The solution I'm proposing is exactly like the dissertation
	// describes except that a leader may not append a new configuration
	// entry until it has committed an entry from its current term.
	//
	// In a typical Raft implementation, a leader appends a no-op
	// entry to the log when it is elected. This change would mean
	// rejecting or delaying membership change requests until the
	// no-op entry is committed.
	//
	// Once a leader has committed an entry in its current term, it
	// knows it has the latest committed configuration, and no existing
	// uncommitted configurations from prior terms can be committed
	// anymore (the servers that store the current leader's entry
	// won't vote for the servers that have the uncommitted
	// configuration). So then it is safe for the leader to
	// create a new configuration (that differs by at most
	// one server) and begin replicating it.
	//
	// In John Ousterhout's words, which tend to be better than
	// mine, "The overall requirement is that a leader must not
	// begin replicating a new configuration entry if there is
	// an older configuration entry that is incompletely
	// replicated (it is stored on at least one server, is not
	// currently committed, but could become committed in the
	// future). This ensures that if two configurations "compete",
	// they differ by at most one server and hence have
	// overlapping consensuses. In the algorithm from the
	// dissertation, leaders were careful to make sure there
	// were no incomplete configuration entries from the current
	// term, but the algorithm did not properly handle incomplete
	// configuration entries from previous leaders (which might
	// not even be visible to the current leader). The new
	// approach fixes that by ensuring that any such entries,
	// if they exist, cannot be committed in the future, hence
	// cannot be used for making decisions."

	// in becomeLeader
	desc := fmt.Sprintf("noop0 first commit of leader(%v) at term %v", s.name, s.state.CurrentTerm)
	noopTkt := s.NewTicket(desc, "", "", nil, s.PeerID, s.name, NOOP, -1, s.MyPeer.Ctx)
	noopTkt.MC = s.state.MC.Clone()
	s.initialNoop0Tkt = noopTkt
	s.WaitingAtLeader.set(noopTkt.TicketID, noopTkt)

	// note that replicateTicket takes care of setting
	// noopTkt.MemberConfig, so we should not.
	s.replicateTicket(noopTkt)
	noopTkt.Stage += ":replicateTicket_for_noopTkt_called"

	// take over any WaitingAtFollow too
	for _, v := range s.WaitingAtFollow.all() {
		v.Stage += ":transfer_WAF_to_WAL"
		s.WaitingAtLeader.set(v.TicketID, v)
	}
	s.WaitingAtFollow = newImap() // make(map[string]*Ticket)

	s.dispatchAwaitingLeaderTickets()

	if s.saver != nil {
		s.saver.save(s.state)
	}
	// client sessions were with the old leader and otherwise
	// may not know to contact the new one.
	s.notifyClientSessionsOfNewLeader()
}

// internal, called by becomeLeader() just above.
func (s *TubeNode) notifyClientSessionsOfNewLeader() {
	n := s.sessByExpiry.Len()
	if n <= 0 {
		return
	}
	//vv("%v top of sending NotifyClientNewLeader have %v sessions to notify", s.name, n)

	for it := s.sessByExpiry.tree.Min(); it != s.sessByExpiry.tree.Limit(); it = it.Next() {

		fragUpdateLeader := s.newFrag()
		fragUpdateLeader.FragOp = NotifyClientNewLeader
		fragUpdateLeader.FragSubject = "NotifyClientNewLeader"
		fragUpdateLeader.SetUserArg("leaderName", s.leaderName)
		fragUpdateLeader.SetUserArg("leaderURL", s.leaderURL)
		fragUpdateLeader.SetUserArg("leaderID", s.leaderID)

		if s.state.MC != nil && s.state.MC.BootCount == 0 {
			// BootCount == 0 means is not a psuedo-config from boot time.
			bts, err := s.state.MC.MarshalMsg(nil)
			panicOn(err)
			fragUpdateLeader.Payload = bts
		}

		ste := it.Item().(*SessionTableEntry)
		cktP0, ok := s.cktAllByName[ste.ClientName] // ste.ClientPeerID avail too
		var ckt *rpc.Circuit
		if !ok || cktP0.ckt == nil {
			//vv("%v ugh: no circuit to ste.ClientName: '%v'. making one on a background goro...", s.name, ste.ClientName)

			if !isValidURL(ste.ClientURL) {
				alwaysPrintf("arg. could not inform client of new leader; bad url '%v' for client SessionID '%v'; clientName='%v'; clientPeerID='%v'", ste.ClientURL, ste.SessionID, ste.ClientName, ste.ClientPeerID)
				return
			}

			// background b/c trying to not block the leader main event loop
			go func(clientURL string, firstFrag *rpc.Fragment) {
				defer func() {
					// not sure why the isValidURL check above is not working... hmm.
					r := recover()
					if r != nil {
						alwaysPrintf("ignoring error trying to contact clientURL='%v' about new leader: '%v", clientURL, r)
					}
				}()
				ckt, _, _, _ = s.MyPeer.NewCircuitToPeerURL("tube-ckt", clientURL, firstFrag, 0)
				if ckt != nil {
					select {
					case s.MyPeer.NewCircuitCh <- ckt:
					case <-s.MyPeer.Halt.ReqStop.Chan:
					}
				}
			}(ste.ClientURL, fragUpdateLeader)
			continue
		} else {
			ckt = cktP0.ckt
		}

		s.SendOneWay(ckt, fragUpdateLeader, -1, 0)
		vv("%v sent new leader(me) notification to %v", s.name, ste.ClientName)
	}
}

func isValidURL(url0 string) bool {
	_, err := url.Parse(url0)
	return err == nil
}

// if reject is returned, then
// idx is -1 if our log is too short; else the
// conflictTerm1stIndex
//
// note this only checks ae.PrevLogIndex/Term
// for mismatch. Err, it used to. Now we
// check a little more under compaction to have
// have a 2nd backstop.
func (s *TubeNode) logsAreMismatched(ae *AppendEntries) (
	reject bool, // reject will be true if we cannot apply
	conflictTerm int64, // -1 if no conflict
	conflictTerm1stIndex int64) { // -1 if no conflict

	if true { // !s.cfg.isTest || s.cfg.testNo != 802 {
		s.wal.assertConsistentWalAndIndex(0)
		if s.state.CompactionDiscardedLast.Index != s.wal.logIndex.BaseC {

			panicf("s.state.CompactionDiscardedLastIndex(%v) != s.wal.logIndex.BaseC(%v)", s.state.CompactionDiscardedLast.Index, s.wal.logIndex.BaseC) // panic: s.state.CompactionDiscardedLastIndex(5117) != s.wal.logIndex.BaseC(0)
		}
	}

	//defer func() {
	//vv("%v logsAreMismatched returning reject=%v; conflictTerm=%v; conflictTerm1stIndex=%v; ae='%v'", s.me(), reject, conflictTerm, conflictTerm1stIndex, ae)
	//}()

	//ae.PrevLogIndex // index of log entry immediately preceeding new ones
	//ae.PrevLogTerm  // term of prevLogIndex entry	rlog := s.wal.RaftLog

	nes := len(ae.Entries)
	if nes == 0 {
		// don't reject heartbeats
		return false, -1, -1
	}
	// INVAR: nes > 0

	if ae.PrevLogIndex <= 0 {
		//no constraint on log
		return false, -1, -1
	}
	// INVAR: ae.PrevLogIndex > 0

	lli, llt := s.wal.LastLogIndexAndTerm()
	_ = llt

	if lli < ae.PrevLogIndex {
		return true, -1, -1 // reject, our log is too short.
	}
	// INVAR: lli >= ae.PrevLogIndex

	// page 19
	// "log entries never change their position in the log."

	// we can only really check at the earliest these guys,
	// if they are available ( > 0 ).
	baseC := s.wal.logIndex.BaseC
	if baseC > 0 {
		// have to assume we match before what we compacted
		// away, since we only compact committed values.

		if ae.PrevLogIndex <= baseC {

			if ae.PrevLogIndex == baseC {
				if ae.PrevLogTerm != s.state.CompactionDiscardedLast.Term {
					return true, ae.PrevLogTerm, ae.PrevLogIndex
				}
				// INVAR: ae.PrevLogTerm == s.state.CompactionDiscardedLast.Term
				// we match terms at BaseC, but have to look at BaseC+1...
				// BUT, this _does_ mean we can overwrite our entries with leaders,
				// so below should never reject(!) so we can stop now...
				return false, -1, -1
			}
			// else: ae.PrevLogIndex < baseC, so I guess we need a snapshot.

			// compaction must/assumes that we always have
			// agreement though <= s.state.CompactionDiscardedLastIndex.
			// check the rest

			//for i, e := range ae.Entries {

			last := nes - 1
			e0 := ae.Entries[0]
			e0index := e0.Index
			eLast := ae.Entries[last]
			eLastIndex := eLast.Index
			if e0index > lli+1 {
				//vv("gap, no overlap, no idea how far back it goes. e0index(%v) > lli(%v)", e0index, lli)
				return true, -1, -1
			}
			if eLastIndex < baseC {
				// we have all of their stuff anyway
				return false, -1, -1
			}
			// INVAR: we have some overlap.
			// ae.PrevLogIndex          <=  baseC <= eM.Index
			//                 e0.Index <=        <= lli

			// the earliest we can start comparing is
			// ae.PrevLogIndex on the leader side, and
			// baseC on the follower side here. We
			// know from above that baseC >= ae.PrevLogIndex.
			beg := baseC
			if beg == 0 {
				beg = 1 // earliest possible term we can compare
			}
			for fTerm, fIndex := range s.wal.logIndex.getTermIndexStartingAt(beg) {
				// INVAR: fIndex >= 1
				if fIndex < ae.PrevLogIndex {
					// no comparison possible
					continue
				}
				if fIndex == ae.PrevLogIndex {
					if fTerm != ae.PrevLogTerm {
						// conflict, but we can overwrite with leaders!! so not:
						//return true, fTerm, fIndex
						return false, fTerm, fIndex
					}
					continue
				}
				// INVAR: fIndex > ae.PrevLogIndex

				i := fIndex - ae.PrevLogIndex - 1
				e := ae.Entries[i]
				if e.Index != fIndex {
					panicf("bad internal logic here, beg=%v; we should have "+
						"matching indexes but leader data e.Index=%v "+
						"while follower fIndex=%v (our baseC=%v)\n follower state='%v'", beg, e.Index, fIndex, baseC, s.state)
				}
				if e.Term != fTerm {
					return true, e.Term, e.Index
				}
				if fIndex == eLastIndex {
					// last viable comparison (last overlap),
					// no conflict found.
					return false, -1, -1
				}
			} // end for fTerm
			return false, -1, -1
		}
		if ae.PrevLogIndex == baseC {
			if ae.PrevLogTerm == s.state.CompactionDiscardedLast.Term {
				// ok. no reject. as above, we only check ae.PrevLogTerm.
				return false, -1, -1
			} else {
				// mismatched at PrevLogIndex and maybe before, but
				// since we are compacted, we cannot scan back
				// through the log to find the first mis-match (below).
				//vv("ae.PrevLogIndex(%v) == baseC(%v) but ae.PrevLogTerm(%v) != s.state.CompactionDiscardedLast.Term(%v)", ae.PrevLogIndex, baseC, ae.PrevLogTerm, s.state.CompactionDiscardedLast.Term)
				return true, -1, -1
			}
		}
		// INVAR: ae.PrevLogIndex > baseC

	} // end if baseC > 0

	// INVAR: ae.PrevLogIndex > 0
	entry, err := s.wal.GetEntry(ae.PrevLogIndex)
	if err != nil {
		//vv("not present, our log is too short; ae.PrevLogIndex=%v; baseC=%v", ae.PrevLogIndex, baseC)
		return true, -1, -1 // not present, our log is too short.
	}
	prevTerm := entry.Term
	if prevTerm == ae.PrevLogTerm {
		//vv("good: acceptable. no reject.") // simae_test version 802 runs.

		// we used to accept here: return false, -1, -1;
		// but its safer to check for agreement
		// again, esp with compaction being pretty new.

		if lli >= ae.PrevLogIndex+1 {
			// fine grain checking, redundant with Extends?
			j := 0
			for fTerm, fIndex := range s.wal.logIndex.getTermIndexStartingAt(ae.PrevLogIndex + 1) {
				e := ae.Entries[j]
				if e.Index != fIndex {
					panicf("bad internal logic here, we should have "+
						"matching indexes but leader data e.Index=%v "+
						"while follower fIndex=%v", e.Index, fIndex)
				}
				if e.Term != fTerm {
					return true, e.Term, e.Index
				}
				j++
				if j == nes {
					return false, -1, -1 // no reject
				}
			}
		}
		return false, -1, -1
	}

	// tell the server how we mismatch
	//
	// page 21
	// "For example, when rejecting an AppendEntries request,
	// the follower can include the term of the conflicting
	// entry and the first index it stores for that term.
	conflictTerm = prevTerm
	conflictTerm1stIndex = ae.PrevLogIndex
	i := ae.PrevLogIndex
	for i--; i > 0; i-- {

		entry, err = s.wal.GetEntry(i)
		if err != nil {
			// err not returned anyway, no need to nil out.
			break
		}
		if entry.Term == conflictTerm {
			conflictTerm1stIndex--
		} else {
			break
		}
	}
	// not matching term, reject
	reject = true

	return
}

// helper for handleAppendEntries. deals with
// MC update.
func (s *TubeNode) aeMemberConfigHelper(ae *AppendEntries, numNew int, ack *AppendEntriesAck) {

	if s.state != nil && s.state.Known != nil && ae.MC != nil {
		s.state.Known.merge(ae.MC)
	}
	if s.state.MC != nil &&
		s.state.MC.VersionEqual(ae.MC) {
		if ae.MC.IsCommitted && !s.state.MC.IsCommitted {
			s.state.MC.IsCommitted = true
		}
		return // no change in membership
	}

	if s.state.MC != nil &&
		s.state.MC.VersionGTE(ae.MC) {
		//vv("%v: no change in member config; ae = '%v'", s.me(), ae)
		return // no change in membership
	}
	// we have a change of membership

	// reporting for logging/analysis.
	if s.state.MC == nil {
		if ae.MC != nil {
			//vv("%v handleAE: nil current config, updating config:\n new ID:'%v'\nconfig='%v'", s.name, ae.MC.MemberConfigID, ae.MC.Short())
		}
	} else {
		diff := s.membershipDiffOldNew(s.state.MC, ae.MC)
		_ = diff
		const longOutput = true
		var oldmc, aemc string
		_, _ = oldmc, aemc
		if longOutput {
			oldmc = s.state.MC.String()
			aemc = ae.MC.String()
		} else {
			oldmc = s.state.MC.ShortProv()
			aemc = ae.MC.ShortProv()
		}
		//vv("%v handleAE: updating config:\n old ID:'%v'\n new ID:'%v'\n membershipDiff='%v'\n text diff = '\n%v'\n old mem config=%v\n new mem config=%v\n", s.name, s.state.MC.MemberConfigID, ae.MC.MemberConfigID, diff, textDiff(s.state.MC.String(), ae.MC.String()), oldmc, aemc)
	}

	// actually adopt the update for ShadowReplicas
	if ae.ShadowReplicas != nil {
		s.state.ShadowReplicas = ae.ShadowReplicas
	}
	// actually adopt the update
	_, ignored := s.setMC(ae.MC, "aeMemberConfigHelper")
	if ignored {
		panic("what? why ignored?")
	}
	upd := s.state.MC
	ack.PeerMC = upd

	//vv("%v updated MC to '%v'", s.me(), ae.MC)
	if numNew == 0 {
		// not going to otherwise save... could be bootstrap
		// situation, for instance and we just got our first
		// heartbeat.
		if s.saver != nil {
			s.saver.save(s.state)
		}
	}
	if !s.cfg.NoBackgroundConnect {
		//vv("%v: in handleAE, have new member config '%v'. calling connectToMC()", s.me(), ae.MC.Short())
		s.connectToMC("AE")
	}
}

// AppendEntries has:
// Term
// PrevLogIndex // index of log entry immediately preceeding new ones
// PrevLogTerm // term of prevLogIndex entry
// Entries []*RaftLogEntry `zid:"6"`
// LeaderCommitIndex // Leader's commitIndex
//
// Section 3.7, page 26
// "Raft RPCs have the same effect if repeated, so
// this [getting a message twice] causes no harm.
// For example, if a follower receives an AppendEntries
// request that includes log entries already present
// in its log, it ignores those entries in the new request."
// So we must be idempotent.
func (s *TubeNode) handleAppendEntries(ae *AppendEntries, ckt0 *rpc.Circuit) (numOverwrote, numTruncated, numAppended int64) {

	var ack *AppendEntriesAck
	// the first Extends call reponses, so we can assert
	// on them in our defer.
	var extends, needSnapshot bool
	var largestCommonRaftIndex int64

	s.hlc.ReceiveMessageWithHLC(ae.LeaderHLC)

	chatty802 := false
	s.testAEchoices = nil
	if false {
		lli := s.wal.LastLogIndex()
		alwaysPrintf("%v: diagnostic handleAppendEntries top; from '%v'; starting lli=%v; this ae='%v'", s.me(), ae.FromPeerName, lli, ae)

		defer func() {
			lli := s.wal.LastLogIndex()
			alwaysPrintf("%v: diagnostic end handleAppendEntries from '%v'; ending lli=%v", s.me(), ae.FromPeerName, lli)
		}()
	}
	//if !s.cfg.isTest {
	//	//vv("%v: handleAppendEntries top; from '%v'; ae=%v", s.me(), ae.FromPeerName, ae)
	//}

	// assert that we update s.state.CommitIndex promptly upon learning
	// of any update from leader. We were missing this on early
	// return 2 (upToDate) below, and thus follower checks like P1
	// could have lagged.
	defer func(aeCommitIndex int64) {
		lli := s.wal.LastLogIndex()
		shouldHaveCI := min(aeCommitIndex, lli)
		if s.state.CommitIndex < shouldHaveCI {
			panic(fmt.Sprintf("%v why was not s.state.CommitIndex(%v) updated to be (%v)shouldHaveCI = min(aeCommitIndex(%v), lli(%v))???", s.me(), s.state.CommitIndex, shouldHaveCI, aeCommitIndex, lli))
		}
	}(ae.LeaderCommitIndex)

	// assert that we got caught up if we could have.
	// with compaction we were seeing not all logs keeping up.
	// made 059 compact_test red, good.
	nes := len(ae.Entries)
	if nes > 0 {
		defer func(aeLastLogIndex int64) {
			lli := s.wal.LastLogIndex()
			if aeLastLogIndex > lli {
				// Test052_partition_leader_away_and_rejoin and 055,057 still
				// asserts here even with the NeedSnapshotGap check.
				if !ack.NeedSnapshotGap && extends && ack.ConflictTerm1stIndex <= 0 {
					panic(fmt.Sprintf("%v why did we not apply the aeLastLogIndex(%v) > our now lli(%v) ???\n ae='%v'\n ack='%v'\n extends=%v, largestCommonRaftIndex=%v, needSnapshot=%v", s.me(), aeLastLogIndex, lli, ae, ack, extends, largestCommonRaftIndex, needSnapshot)) // extends=true, largestCommonRaftIndex=2, needSnapshot=false; 	             RejectReason: "ae.PrevLogIndex(1) not compatible. conflictTerm=-1, conflictTerm1stIndex=-1; largestCommonRaftIndex=2; alsoGap=false (true if ae.PrevLogIndex(1) > largestCommonRaftIndex(2))",
					// before adding ack.ConflictTerm1stIndex <= 0 : term mismatched so cannot apply what we got? why not overwriting??
					// RejectReason: "ae.PrevLogIndex(20547) not compatible. conflictTerm=10, conflictTerm1stIndex=20548; largestCommonRaftIndex=20549; alsoGap=false (true if ae.PrevLogIndex(20547) > largestCommonRaftIndex(20549))",
				}
				//vv("%v end handleAE. good: aeLastLogIndex(%v) <= our now lli(%v)\n ae='%v'\n ack='%v'\n extends=%v, largestCommonRaftIndex=%v, needSnapshot=%v", s.me(), aeLastLogIndex, lli, ae, ack, extends, largestCommonRaftIndex, needSnapshot)
			}
		}(ae.Entries[nes-1].Index)
	}

	defer func() {
		//if chatty802 || true {
		// fine, stay quiet. does not append to s.testAEchoice
		//if s.isRegularTest() {
		//if !s.cfg.isTest {
		//vv("%v \n-------->>>    END  handle AppendEntries()  <<<--------\n", s.me())
		//	if len(s.testAEchoices) == 0 {
		//		panic(fmt.Sprintf("internal logic error: must have made choices below! not 802: s.cfg.ClusterID//='%v'", s.cfg.ClusterID))

		//vv("%v \n-------->>>    END  handle AppendEntries()  <<<--------\n choices = %v\n ack = %v\n", s.me(), s.choiceString(s.testAEchoices), ack)
		r := recover()
		if r != nil {
			alwaysPrintf("%v recover TubeNode.handleAppendEntries() ae='%v' numOverwrote=%v; numTruncated=%v; numAppended=%v; partially prepared ack='%v'", s.me(), ae, numOverwrote, numTruncated, numAppended, ack)
			panic(r)
		}
	}()

	if s.isTest() {
		// use cfg.ClusterID (works for 802) not s.cfg.testCluster.TestName
		// since testCluster will be nil under simae_test 802.
		if s.isSimAeTest() {
			// super chatty during 802 test, leave out.
			chatty802 = true
		} else {
			//vv("%v \n-------->>>    handle AppendEntries() from %v <<<--------\n", s.me(), ae.FromPeerName)
			//vv("%v \n-------->>>    handle AppendEntries()  <<<--------\n%v\nTestName = '%v'", s.me(), ae, s.cfg.testCluster.TestName)

			select {
			case panicAtCap(s.testGotAeFromLeader) <- ae.clone():
			case <-s.Halt.ReqStop.Chan:
				//vv("%v (test code) shutdown... below prod code should handle", s.me())
			}
		}
	} else {
		// monitoring prod, not test
		if false {
			// yes we saw this logging in the follower's
			// logs. noisy, so off for now.
			s.countProdAE++
			if s.countProdAE%10 == 0 {
				alwaysPrintf("%v [%v] (%v) countProdAE = %v", s.name, s.role, s.PeerServiceName, s.countProdAE)
			}
		}
	}

	if s.state.CurrentTerm <= 0 {
		// on first joining, we don't have a current term.
		// this is normal, the AE we just got will give a term.
	}

	if !chatty802 { // so for prod and regular tests
		s.suppressWatchdogs(ae)
	}
	if ae.LeaderTerm < 1 {
		report := fmt.Sprintf("%v handleAppendEntries() dropping message, bad term number: %v < 1 ;\n ae=%v\n", s.me(), ae.LeaderTerm, ae)
		s.host.choice(report)
		alwaysPrintf(report)
		//vv("%v early AE return 0", s.name)
		return
	}

	stateSaveNeeded := false
	prevci := s.state.CommitIndex
	lli, llt := s.wal.LastLogIndexAndTerm()
	lli2, llt2 := ae.LeaderCommitIndex, ae.LeaderCommitIndexEntryTerm

	maxPossibleCI := lli
	maxPossibleCIterm := llt
	// take the minimum of what we have and the ae.
	if lli2 < lli {
		maxPossibleCI = lli2
		maxPossibleCIterm = llt2
	}
	if maxPossibleCI > prevci {
		// make 707 green with the new assert that CommitIndex has
		// been updated (in the defer at top).
		//vv("%v in handleAppendEntries: updating s.state.CommitIndex from %v -> %v (as min(ae.LeaderCommitIndex(%v), lli(%v))", s.name, s.state.CommitIndex, maxPossibleCI, ae.LeaderCommitIndex, lli)
		s.state.CommitIndex = maxPossibleCI
		s.state.CommitIndexEntryTerm = maxPossibleCIterm
		s.updateMCindex(maxPossibleCI, maxPossibleCIterm)
		if !chatty802 {
			if s != nil && s.saver != nil {
				stateSaveNeeded = true
				defer func() {
					if stateSaveNeeded { // set to false just after commitWhatWeCan below
						// better to apply immediately too, not just save.
						s.host.commitWhatWeCan(false)
						// commitWhatWeCan does: s.saver.save(s.state)
					}
				}()
			}
		}
	}

	if ae.LeaderName != s.leaderName {
		// try to detect flapping between 2
		// different leaders under split brain.
		s.currentLeaderFirstObservedTm = time.Now()
	}
	s.leaderName = ae.LeaderName
	s.leaderID = ae.LeaderID
	s.leaderURL = ae.LeaderURL

	// earlier but may be redundant with  :6443
	didDeferRetryTicketsAwaitingLeader := false
	if len(s.ticketsAwaitingLeader) > 0 {
		defer s.dispatchAwaitingLeaderTickets()
		didDeferRetryTicketsAwaitingLeader = true
	}

	numNew := len(ae.Entries)
	isHeartbeat := (numNew == 0)
	_ = isHeartbeat
	//vv("%v handleAppendEntries (hb:%v), my currentTerm=%v, ae.LeaderTerm=%v\n AppendEntries:\n%v\n my local follower log:\n%v\n", s.me(), isHeartbeat, s.state.CurrentTerm, ae.LeaderTerm, ae, s.wal.getTermsRLE())

	s.host.choice("Starting handleAppendEntries with %v new entries", numNew)

	rlogicalLen := s.wal.LogicalLen() // s.lli aka s.logIndex.Endi

	s.host.choice("on %v: handleAppendEntries, pre-existing log: %v", s.me(), s.wal.logIndex)

	var entriesIdxBeg, entriesIdxEnd, entriesEndxIdx int64
	//vv("%v numNew = %v", s.me(), numNew)
	if numNew == 0 {
		// heartbeat maybe? they will still want
		// to know about our info though, right? yep. fallthrough
		// to below and tell leader what are log looks like.
		s.host.choice("Empty heartbeat")
	} else {
		entriesIdxBeg = ae.Entries[0].Index
		entriesIdxEnd = ae.Entries[numNew-1].Index
		entriesEndxIdx = entriesIdxEnd + 1
		_ = entriesEndxIdx
		//vv("%v entriesIdxBeg=%v; entriesIdxEnd=%v", s.me(), entriesIdxBeg, entriesIdxEnd)
		//vv("%v Append range [%v:%v] aka [%v:%v); len = %v", s.me(), entriesIdxBeg, entriesIdxEnd, entriesIdxBeg, entriesEndxIdx, numNew)
		s.host.choice("Append range [%v:%v] aka [%v:%v); len = %v", entriesIdxBeg, entriesIdxEnd, entriesIdxBeg, entriesEndxIdx, numNew)
	}
	localFirstIndex, localFirstTerm, localLastIndex,
		localLastTerm := s.host.getRaftLogSummary()

	// don't be fooled by the name follower here;
	// we might be the leader, but we use the
	// name that the follower logic below needs to avoid
	// having to call getTermsRLE and clone it twice.
	followerLog := s.wal.getTermsRLE()

	// be ready to reject half-dozen ways...
	ack = &AppendEntriesAck{
		ClusterID:           s.ClusterID,
		FromPeerID:          s.PeerID,
		FromPeerName:        s.name,
		FromPeerServiceName: s.PeerServiceName,
		Rejected:            true,

		AEID:                  ae.AEID,
		Term:                  s.state.CurrentTerm,
		MinElectionTimeoutDur: s.cachedMinElectionTimeoutDur,

		// For LargestCommonRaftIndex, lets declare that
		// -1 means unknown, by the convention I just invented.
		// I added this optimization (LargestCommonRaftIndex)
		// for Tube, so this should serve to establish expectations.
		LargestCommonRaftIndex: -1,

		PeerLogTermsRLE:     followerLog,
		PeerLogCompactIndex: s.wal.logIndex.BaseC,
		PeerLogCompactTerm:  s.wal.logIndex.CompactTerm,
		PeerLogFirstIndex:   localFirstIndex,
		PeerLogFirstTerm:    localFirstTerm,
		PeerLogLastIndex:    localLastIndex,
		PeerLogLastTerm:     localLastTerm,

		SuppliedCompactIndex: ae.LeaderCompactIndex,
		SuppliedCompactTerm:  ae.LeaderCompactTerm,

		SuppliedPrevLogIndex:               ae.PrevLogIndex,
		SuppliedPrevLogTerm:                ae.PrevLogTerm,
		SuppliedEntriesIndexBeg:            entriesIdxBeg,
		SuppliedEntriesIndexEnd:            entriesIdxEnd,
		SuppliedLeaderCommitIndex:          ae.LeaderCommitIndex,
		SuppliedLeaderCommitIndexEntryTerm: ae.LeaderCommitIndexEntryTerm,
		SuppliedLeader:                     ae.LeaderID,
		SuppliedLeaderName:                 ae.LeaderName,
		SuppliedLeaderTermsRLE:             ae.LogTermsRLE,
		SuppliedLeaderLastTerm:             ae.LeaderTerm,
		SuppliedLeaderLLI:                  ae.LeaderLLI,
		SuppliedLeaderLLT:                  ae.LeaderLLT,

		PeerMC:      s.state.MC,
		FollowerHLC: s.hlc.ReceiveMessageWithHLC(ae.LeaderHLC),
	}
	// report on past compations too.
	compactedTo := s.state.CompactionDiscardedLast.Index
	if compactedTo > 0 {
		ack.PeerCompactionDiscardedLastIndex = compactedTo
		ack.PeerCompactionDiscardedLastTerm = s.state.CompactionDiscardedLast.Term
		ack.LargestCommonRaftIndex = compactedTo // rather than -1
	}

	// figure 3.1 summary
	//
	// Reply false if term < current term. Section 3.3
	//
	// This is crucial for the safefty property proof, point 3, on page
	// 25 of Ongaro 2014 dissertation, section 3.6.3
	// Reject means we cannot apply _any_ of the entries!

	// "if the term is smaller, the candidate rejects
	// the [AppendEntries] RPC and continues in candidate state"

	if ae.LeaderTerm < s.state.CurrentTerm {
		s.host.choice("Rejecting: term too low (ae.Term=%v < currentTerm=%v)", ae.LeaderTerm, s.state.CurrentTerm)
		// caller's term is stale.
		ack.RejectReason = fmt.Sprintf("term too low: ae.Term(%v) < s.state.CurrentTerm(%v)", ae.LeaderTerm, s.state.CurrentTerm)
		s.host.ackAE(ack, ae)
		//vv("%v early AE return 1", s.name)
		return
	}

	s.host.choice("Term check passed: ae.LeaderTerm=%v >= currentTerm=%v", ae.LeaderTerm, s.state.CurrentTerm)

	// INVAR: ae.LeaderTerm >= s.state.CurrentTerm
	s.lastLegitAppendEntries = time.Now()

	// don't apply the membership until it is in the log!
	// BUT problem is that new nodes who just get
	// heartbeat need to discover the cluster. So
	// we do want to use this information if we
	// don't have a prior cluster available at all.
	// We may not have any log info to write, either(!)
	if !chatty802 {
		s.aeMemberConfigHelper(ae, numNew, ack)
	}
	// page 16 - page 17
	// "While waiting for votes, a candidate may receive
	// an AppendEntries from another server claiming to be a leader.
	// If the leader's term is at least as large as the
	// candidates current term, the candidate recognizes the
	// leader and returns to follower state."
	if ae.LeaderTerm > s.state.CurrentTerm {
		// leaders too!
		s.host.choice("Becoming follower due to higher term")
		s.host.becomeFollower(ae.LeaderTerm, ae.MC, SAVE_STATE)
		// below set s.leader

		// since this is the _not_ "the current leader", we
		// do not reset our election timeout here.
		// "If election timeout elapses without receiving
		// AppendEntries RPC **from current leader** or granting
		// vote to candidate: convert to candidate."
		// me: but this is also ambiguous since they ARE
		// the leader now...
	} else {
		// INVAR: ae.LeaderTerm == s.state.CurrentTerm, since
		// we already returned above when ae.LeaderTerm < s.state.CurrentTerm.
		// looking at StepDown() RaftConsensus.cc:2909
		if s.role != FOLLOWER {
			// without discarding our votes or changing our term, whoa.
			s.host.choice("Stepping down to follower role")
			s.role = FOLLOWER // RaftConsensus.cc:2925
			// just in case we were leader until just now
			//if s.cfg.testNum != 802 {
			//	//vv("%v handleAE: step down to follower, so nil out s.leaderSendsHeartbeatsCh", s.name)
			//}
			s.leaderSendsHeartbeatsCh = nil
			// below set s.leader
		}
		// in response to this legit packet from current leader.
		// I moved this below because even with a higher term,
		// only a leader sends AE, so we recognized the new leader
		// and must reset election timeout either way.
		//s.resetPreVoteState(false, true) // in handleAppendEntries()
		//s.host.resetElectionTimeout("handleAppendEntries from current leader")
	}
	s.resetPreVoteState(false, true) // in handleAppendEntries()
	s.host.resetElectionTimeout("handleAppendEntries >= term seen.")

	ack.Term = s.state.CurrentTerm

	// INVAR: we are follower from here on.

	// record leader for client forwarding
	//if s.leaderID != ae.FromPeerID {
	leaderChanged := false
	if s.leaderID != ae.LeaderID {
		leaderChanged = true
		//s.choice( "New leader detected: %v", ae.LeaderID)
		//s.choice( "New leader detected: %v", ae.FromPeerID)
		s.leaderID = ae.LeaderID
		s.leaderName = ae.LeaderName
		s.leaderURL = ae.LeaderURL
		//vv("%v sees new leader %v",rpc.AliasDecode(s.me()), rpc.AliasDecode(s.leader))
	}
	if !didDeferRetryTicketsAwaitingLeader {
		if leaderChanged || len(s.ticketsAwaitingLeader) > 0 {
			defer func() {
				s.host.dispatchAwaitingLeaderTickets()
			}()
		}
	}

	leaderLog := ae.LogTermsRLE

	extends, largestCommonRaftIndex, needSnapshot = leaderLog.Extends(followerLog)
	var firstEntry string
	_ = firstEntry
	if nes > 0 {
		firstEntry = fmt.Sprintf("ae.Entries[0])='%v'", ae.Entries[0])
	}
	if nes > 0 {
		//vv("%v nes=%v; extends=%v, largestCommonRaftIndex=%v; neesShapshot=%v; leaderLog.Endi=%v\nfollowerLog=%v\nleaderLog=%v; firstEntry='%v'\n", s.name, nes, extends, largestCommonRaftIndex, needSnapshot, leaderLog.Endi, followerLog, leaderLog, firstEntry)
	}
	if needSnapshot {
		ack.NeedSnapshotGap = true
	}

	s.host.choice("extends=%v, largestCommonRaftIndex=%v; leaderLog.Endi=%v ", extends, largestCommonRaftIndex, leaderLog.Endi)

	upToDate := largestCommonRaftIndex == leaderLog.Endi
	// set this immediately, so any rejection still conveys this essential info.
	ack.LargestCommonRaftIndex = largestCommonRaftIndex

	if upToDate {
		s.host.choice("Logs are up to date")
		ack.Rejected = false
		ack.LogsMatchExactly = true
		s.host.ackAE(ack, ae)
		//vv("%v early AE return 2", s.name)
		return
	}
	// INVAR: logs differ in some way, and we are not up to date.

	// Overall approach: if we cannot use everything sent, or still need
	// more data afterwards, we should reject so
	// leader sends us the necessary updates.
	// Accepting (ack.Rejected = false) means the
	// leader will leave us alone until heartbeat
	// or a client txn. If we need to complete our
	// log so that things can commit, it is urgent
	// that we do so ASAP.

	// if extends is true, then we might think we are good to apply
	// anything from the leader log, except that there
	// might be a gap! We cannot allow gaps, and must reject on them.
	// Note this only checks ae.PrevLogIndex/Term.
	reject, conflictTerm, conflictTerm1stIndex := s.host.logsAreMismatched(ae)
	//reject, conflictTerm, conflictTerm1stIndex := s.logsAreMismatched(ae)
	if conflictTerm > 0 {
		ack.ConflictTerm = conflictTerm
		ack.ConflictTerm1stIndex = conflictTerm1stIndex
	}

	s.host.choice("Logs differ: extends=%v, largestCommonRaftIndex=%v; reject=%v, conflictTerm=%v, conflictTerm1stIndex=%v", extends, largestCommonRaftIndex, reject, conflictTerm, conflictTerm1stIndex)

	if reject {
		alsoGap := ae.PrevLogIndex > largestCommonRaftIndex
		ack.RejectReason = fmt.Sprintf("ae.PrevLogIndex(%v) not compatible. conflictTerm=%v, conflictTerm1stIndex=%v; largestCommonRaftIndex=%v; alsoGap=%v (true if ae.PrevLogIndex(%v) > largestCommonRaftIndex(%v))", ae.PrevLogIndex, conflictTerm, conflictTerm1stIndex, largestCommonRaftIndex, alsoGap, ae.PrevLogIndex, largestCommonRaftIndex)

		// tell leader we cannot do anything without
		// getting a snapshot to transfer state.
		if alsoGap {
			//vv("%v alsoGap true!", s.me())
			// BUT! this is not a snapshot gap need, just a
			// a redo of the AE a little further back! so
			// maybe this is overkill... but at the
			// snapshots are very cheap with small state.
			// TODO: revisit can we do better by requesting
			// less with a finer comparison of rle Terms?
			ack.NeedSnapshotGap = true
		}

		s.host.choice(ack.RejectReason)
		s.host.ackAE(ack, ae)

		// 059 compact_test gets here. The gap means
		// we need a snapshot state transfer.
		//vv("%v early AE return 3; alsoGap = %v; ack.ConflictTerm1stIndex=%v; ack.PeerCompactionDiscardedLastIndex=%v", s.name, alsoGap, ack.ConflictTerm1stIndex, ack.PeerCompactionDiscardedLastIndex) // 059: ack.ConflictTerm1stIndex = -1; ack.PeerCompactionDiscardedLastIndex=0

		return
	}
	// If this is a heartbeat, definitely reject so we get data.
	if numNew == 0 { // isHeartbeat equivalent
		ack.RejectReason = fmt.Sprintf("heartbeat detected log update needed. extends=%v, largestCommonRaftIndex=%v", extends, largestCommonRaftIndex)
		ack.Rejected = true
		s.host.choice(ack.RejectReason)
		s.host.ackAE(ack, ae)
		//vv("%v early AE return 4", s.name)
		return
	}

	// INVAR: len(ae.Entries) > 0, since we just rejected heartbeats
	// (We could still have dups and no really new information).

	didAddOrOverData := false
	done := false

	//vv("%v in AE: logs differ! local log='%v'; leader log='%v'; reject=%v; extends=%v, largestCommonRaftIndex=%v; entriesIdxBeg=%v", s.me(), followerLog, leaderLog, reject, extends, largestCommonRaftIndex, entriesIdxBeg)

	// this is probably redundant with s.logsAreMismatched, but
	// is safe, and allows us to be sure there is no gap if we
	// overwrite or append (if we accept, which this check might prevent).
	if !extends && ae.PrevLogIndex > largestCommonRaftIndex {
		s.host.choice("Rejecting: gap in log at prev log index %v > lcp %v", ae.PrevLogIndex, largestCommonRaftIndex)
		ack.RejectReason = fmt.Sprintf("gap: need earlier in leader log: ae.PrevLogIndex(%v) > largestCommonRaftIndex(%v)", ae.PrevLogIndex, largestCommonRaftIndex)
		s.host.ackAE(ack, ae)
		//vv("%v early AE return 5", s.name)
		return
	}
	// INVAR: we should be able to do something, if
	// leader sent us data and not an empty heartbeat.
	// Since we try hard to dedup and not overwrite
	// an existing log entry, we may not end up doing
	// anything with what leader sent, but we shouldn't
	// reject their AE request in that case, as long
	// as it was a match to our log.

	if !extends {
		// already returned above on numNew == 0

		// additional sanity checks before we accept a write,
		// order prevent re-writing something we already have (dedup).

		// if numNew == 0 {
		// 	ack.RejectReason = fmt.Sprintf("!extends, so gap we cannot bridge, and we got no data this time.")
		// 	s.ackAE(ack, ae)
		// 	return
		// }

		// INVAR: numNew > 0
		if entriesIdxBeg > largestCommonRaftIndex+1 {
			s.host.choice("Rejecting: cannot bridge gap at index %v", entriesIdxBeg)
			ack.RejectReason = fmt.Sprintf("cannot gap: entriesIdxBeg(%v) > largestCommonRaftIndex+1(%v)", entriesIdxBeg, largestCommonRaftIndex+1)
			s.host.ackAE(ack, ae)
			//vv("%v early AE return 6", s.name)
			return
		} // else we can apply (append or overwrite, or both).

		// INVAR: entriesIdxBeg <= largestCommonRaftIndex+1
		//    =>  entriesIdxBeg < largestCommonRaftIndex

		if entriesIdxBeg == largestCommonRaftIndex+1 {
			//vv("%v perfectly aligned AE write! no re-writing "+"of already matching log; entriesIdxBeg(%v) == largestCommonRaftIndex+1(%v)", s.me(), entriesIdxBeg, largestCommonRaftIndex+1)
			s.host.choice("comment: Perfectly aligned append at index %v", entriesIdxBeg)
		} else {
			//vv("warning: potential overwrite at index %v", entriesIdxBeg)
			s.host.choice("Warning: potential overwrite at index %v", entriesIdxBeg)
		}

		// !extends, no gap. Cannot just append.
		// possible overwrite, but have to dedup first to know.

		// THIS IS THE DEDUP IMPLEMENTATION (maybe need another below?)
		//
		// Want to ignore replacement of same with same.
		// and then should we reject or accept?
		// fine to accept/reject, either way handleAEAck will
		// update us with the next thing we need. so accept.

		if entriesIdxEnd <= largestCommonRaftIndex {
			// nothing new
			//vv("nothing new to append after clipping off the redundant. leaving neededEntries empty")
			s.host.choice("nothing new to append after clipping off the redundant. leaving neededEntries empty")
			ack.RejectReason = fmt.Sprintf("nothing new after clipping off redundant. largestCommonRaftIndex(%v); ae=[%v,%v)", largestCommonRaftIndex, entriesIdxBeg, entriesIdxEnd+1)
			ack.Rejected = false
			s.host.ackAE(ack, ae)
			//vv("%v early AE return 7", s.name)
			return
		}
		// !extends. no gap. Cannot just append. entriesIdxEnd > lcp.

		s.host.choice("!extends. no gap. Cannot just append. entriesIdxEnd > lcp.")

		// INVAR:  entriesIdxEnd > lcp, so
		//                   lcp <  entriesIdxEnd
		//         entriesIdxBeg <= entriesIdxEnd, by creation of AE;
		// could there be a gap between lcp and entriesIdxBeg? nope
		//         entriesIdxBeg < lcp from above.
		// so we have
		// entriesIdxBeg < lcp < entriesIdxEnd
		// so we only need the ones from [lcp+1, entriesIdxEnd]
		// and we can discard [entriesIdxBeg, lcp]
		keepCount := largestCommonRaftIndex
		//startWritingAt := largestCommonRaftIndex + 1
		baseActuallyNew0 := largestCommonRaftIndex + 1 - entriesIdxBeg
		//vv("baseActuallyNew0 = %v = largestCommonRaftIndex(%v) +1 -entriesIdxBeg(%v)", from, largestCommonRaftIndex, entriesIdxBeg)

		neededEntries := ae.Entries[baseActuallyNew0:]
		needed := len(neededEntries)
		if needed == 0 {
			s.host.choice("nothing new to append after clipping off the redundant")
			ack.Rejected = false
			ack.RejectReason = fmt.Sprintf("nothing new after clipping off redundant. largestCommonRaftIndex(%v); ae=[%v,%v)", largestCommonRaftIndex, entriesIdxBeg, entriesIdxEnd+1)
			s.host.ackAE(ack, ae)
			//vv("%v early AE return 8", s.name)
			return
		}
		writeBegIdx := neededEntries[0].Index
		keepCount = writeBegIdx - 1
		// use writeBegIdx just below, NOT entriesIdxBeg !!

		s.host.choice("writeBegIdx=%v, rlogicalLen=%v; lcp=%v, needed=%v", writeBegIdx, rlogicalLen, largestCommonRaftIndex, needed)

		//numOverwrote = max(0, largestCommonRaftIndex-writeBegIdx+1)
		//numOverwrote = max(0, int64(len(rlog))-writeBegIdx+1)

		// adjust the AE? ugh. leave immutable, and just write now.

		// we might be shorter afterwards, if the AE shrinks our log.
		numTruncated = max(0, rlogicalLen-entriesIdxEnd) // yes
		// max(0, ) not really needed here, but is more general.
		numAppended = max(0, entriesIdxEnd-rlogicalLen) // yes
		numOverwrote = int64(needed) - numAppended

		s.host.choice("have %v neededEntries after clipping off the redundant. nOver=%v, nTrunc=%v, nAppend=%v", needed, numOverwrote, numTruncated, numAppended)

		// general sanity assert to our AE logic.
		if s.cfg.NoLogCompaction {
			if keepCount < s.state.CommitIndex {
				panic(fmt.Sprintf("%v log violation: keepCount(%v) < s.state.CommitIndex(%v): overwriteEntries would kill a committed entry", s.me(), keepCount, s.state.CommitIndex))
			}
		}

		// in handleAppendEntries here.
		err := s.wal.overwriteEntries(keepCount, neededEntries, false, s.state.CommitIndex, s.state.LastApplied, &s.state.CompactionDiscardedLast, s)
		panicOn(err)
		if true { // TODO restore: s.cfg.isTest {
			s.wal.assertConsistentWalAndIndex(s.state.CommitIndex)
		}
		didAddOrOverData = true
		done = true // don't call s.wal.overwriteEntries() again below.

	} // end !extends

	ack.Rejected = false

	if !done {
		s.host.choice("Processing %v new entries", numNew)

		if extends {
			keepCount := rlogicalLen // int64(len(rlog))

			// what (subset) of ae.Entries do we want? depends on where they start.
			// ae.Entries[0] is at entriesIdxBeg, and
			// we want to start at keepCount+1
			// INVAR: entriesIdxBeg > 0
			from := keepCount + 1 - entriesIdxBeg
			//vv("%v extends=true. number of ae to extend with: %v [%v, %v]", s.me(), entriesIdxEnd-entriesIdxBeg+1, entriesIdxBeg, entriesIdxEnd)
			//vv("%v from = %v <- keepCount + 1 - entriesIdxBeg;\n where keepCount=%v, entriesIdxBeg=%v, entriesIdxEnd=%v", s.me(), from, keepCount, entriesIdxBeg, entriesIdxEnd)
			switch {
			case from >= int64(len(ae.Entries)):
				// no useful data it seems: but we should still accept not reject!
				s.host.choice("No useful data to append") // not true yet! have to compare the term data too.

				// These are the defaults, but stated here for
				// clarity. Keep them to make reasoning easier.
				numOverwrote = 0
				numTruncated = 0
				numAppended = 0
			case from < 0:
				//vv("keepCount = %v; from = %v; entriesIdxBeg = %v", keepCount, from, entriesIdxBeg) // keepCount = 0; from = -1; entriesIdxBeg = 2
				ack.RejectReason = fmt.Sprintf("from(%v) was negative: gap/insuffic follower log (rlogicalLen= %v); entriesIdxBeg=%v", from, rlogicalLen, entriesIdxBeg)
				s.host.choice("from index negative: gap/insufficient follower log")
				s.host.ackAE(ack, ae)
				//vv("%v early AE return 9", s.name)
				return

			default:
				entries := ae.Entries[from:]
				s.host.choice("Appending %v entries starting at index %v", len(entries), entries[0].Index)
				if len(entries) == 0 {
					panic("should have rejected/diverted above? or from calculation is wrong?")
				}

				// general sanity assert to our AE logic
				if s.cfg.NoLogCompaction {
					if keepCount < s.state.CommitIndex {
						panic(fmt.Sprintf("%v log violation: keepCount(%v) < s.state.CommitIndex(%v): overwriteEntries would kill a committed entry", s.me(), keepCount, s.state.CommitIndex))
					}
				}

				numOverwrote = 0
				numTruncated = 0
				numAppended = int64(len(entries))
				// in handleAppendEntries here.
				err := s.wal.overwriteEntries(keepCount, entries, false, s.state.CommitIndex, s.state.LastApplied, &s.state.CompactionDiscardedLast, s)
				panicOn(err)

				if true { // TODO restore: s.cfg.isTest {
					s.wal.assertConsistentWalAndIndex(s.state.CommitIndex)
				}
				didAddOrOverData = true
			}
		} // end if extends
	} // end if !done

	// disk write can be slow.
	s.host.resetElectionTimeout("handleAppendEntries, after disk write")

	// page 27
	// "Each server also persists new log entries before they
	// are counted towards the entries' commitment;
	// this prevents committed entries from being lost
	// or "uncommitted" when servers restart."
	//
	// me: so now they have been saved to disk, we
	// can increment our s.state.CommitIndex.

	// figure 3.1 says (section 3.5)
	// [5] If leaderCommit > commitIndex, set
	//       commitIndex = min(leaderCommit, index of last new entry)

	// we aren't counting, we are being told the leader
	// has counted, and we just accept that as fact.

	//vv("%v got to ae.LeaderCommitIndex(%v) check vs s.state.CommitIndex(%v)", s.name, ae.LeaderCommitIndex, s.state.CommitIndex)
	if ae.LeaderCommitIndex > s.state.CommitIndex {
		// basic Raft says this in [5] above
		//s.state.CommitIndex = min(ae.LeaderCommitIndex, ae.Entries[numNew-1].Index)
		// but we've implemented alot of batching and
		// deduping, and we don't want to be mistaken here
		// because we avoided truncating a perfectly valid
		// log tail, so we use our own our log's last index as the bound.
		// If the AE got applied above, and extended the log,
		// then it is already reflected in the log now.

		//lli := int64(len(s.wal.RaftLog))
		lli, llt := s.wal.LastLogIndexAndTerm()

		if !chatty802 {
			if s.state.CommitIndex > prevci {
				//vv("%v handle AE: s.state.CommitIndex advanced from %v -> %v (lli= %v)", s.me(), prevci, s.state.CommitIndex, lli)
			}
		}

		newCI := min(ae.LeaderCommitIndex, lli)
		s.host.choice("Updating commitIndex from %v to %v", prevci, newCI)

		if newCI != s.state.CommitIndex {
			//vv("%v updating s.state.CommitIndex from %v -> %v", s.name, s.state.CommitIndex, newCI)
		}
		// no: s.state.CommitIndex = min(ae.LeaderCommitIndex, lli)
		// instead, to get the accurate CommitIndexEntryTerm:
		lli2, llt2 := ae.LeaderCommitIndex, ae.LeaderCommitIndexEntryTerm
		if lli < lli2 {
			s.state.CommitIndex = lli
			s.state.CommitIndexEntryTerm = llt
		} else {
			s.state.CommitIndex = lli2
			s.state.CommitIndexEntryTerm = llt2
		}

	}
	sawUpdate := prevci != s.state.CommitIndex
	_ = sawUpdate
	//vv("%v commitIndex sawUpdate=%v, going %v -> %v, b/c ae.LeaderCommitIndex = %v", s.me(), sawUpdate, prevci, s.state.CommitIndex, ae.LeaderCommitIndex)
	// comment on the above from example/logcabin/RaftConensus.cc, which does:
	//    in handleAppendEntries() line 1411:
	// "Set our committed ID from the request's. In rare cases, this would make
	// our committed ID decrease. For example, this could happen with a new
	// leader who has not yet replicated one of its own entries. While that'd
	// be perfectly safe, guarding against it with an if statement lets us
	// make stronger assertions.
	//    if (commitIndex < request.commit_index()) {
	//        commitIndex = request.commit_index();
	//        assert(commitIndex <= log->getLastLogIndex());
	//        ...
	//    }

	// page 21
	// The leader keeps track of the highest index it
	// knows to be committed, and it includes that index
	// in future AppendEntries RPCs (including heartbeats)
	// so that the other servers eventually find out.
	// Once a follower learns that a log entry is committed,
	// it applies the entry to its local state machine (in log order).

	// Apply (commit) commands: Fig 3.1
	// "All servers: for commitIndex > lastApplied,
	// increment lastApplied and apply
	// log[lastApplied] to the [application] state machine."
	//
	// THIS IS THE COMMIT ON THE FOLLOWER.
	//
	// Application is immediate. It happens inside this call too.
	// Application means the application's FSM executes the commands.
	s.host.commitWhatWeCan(false)
	stateSaveNeeded = false // no save again needed in defer.

	// update these so leader has an accurate picture on success too.
	localFirstIndex, localFirstTerm, localLastIndex,
		localLastTerm = s.host.getRaftLogSummary()

	// If we apply new state, we have just saved to disk anyway
	// inside commitWhatWeCan(), so leave out (for now at any rate).
	//s.saver.save(s.state)

	if didAddOrOverData {
		// followersLog changed, so be sure to correct before sending.
		ack.PeerLogTermsRLE = s.wal.getTermsRLE()
	}
	// Use the Log Matching invariant. (Fig 3.2, page 14).
	ack.LogsMatchExactly = localLastIndex == leaderLog.lastIndex() &&
		localLastTerm == leaderLog.lastTerm()
	if ack.LogsMatchExactly {
		ack.LargestCommonRaftIndex = localLastIndex
	} else {
		_, ack.LargestCommonRaftIndex, ack.NeedSnapshotGap = leaderLog.Extends(ack.PeerLogTermsRLE)
		if ack.LargestCommonRaftIndex == 0 {
			// account for compactions that already happened.
			ack.LargestCommonRaftIndex = s.state.CompactionDiscardedLast.Index
		}
	}

	ack.Rejected = false
	ack.PeerLogCompactIndex = s.wal.logIndex.BaseC
	ack.PeerLogCompactTerm = s.wal.logIndex.CompactTerm

	ack.PeerLogFirstIndex = localFirstIndex
	ack.PeerLogFirstTerm = localFirstTerm
	ack.PeerLogLastIndex = localLastIndex
	ack.PeerLogLastTerm = localLastTerm
	ack.SuppliedLeaderTermsRLE = ae.LogTermsRLE

	if s.isRegularTest() {
		select {
		case panicAtCap(s.testGotGoodAE_lastLogIndexNow) <- localLastIndex:
		case <-s.Halt.ReqStop.Chan:
		}
	}

	s.host.choice("Sending success ack")
	s.host.ackAE(ack, ae)

	s.host.resetElectionTimeout("resetElectionTimeout sent ackAE")

	// end of handleAppendEntries
	return
}

func (s *TubeNode) ackAE(ack *AppendEntriesAck, ae *AppendEntries) {
	ackFrag := s.newFrag()
	bts, err := ack.MarshalMsg(nil)
	panicOn(err)
	ackFrag.Payload = bts
	ackFrag.FragOp = AppendEntriesAckMsg
	ackFrag.FragSubject = "AppendEntriesAck"

	// ackAE should ignore configs, so use cktall not cktReplica.
	cktP, ok := s.cktall[ae.FromPeerID]
	if !ok {
		//vv("%v don't know how to contact '%v' to send AppendEntriesAckMsg: assuming we are shutting down.", s.me(), ae.FromPeerID)
		return
	}
	ckt := cktP.ckt
	err = s.SendOneWay(ckt, ackFrag, -1, 0)
	_ = err // don't panic on halting.
	if err != nil {
		alwaysPrintf("%v non nil error '%v' on AppendEntriesAck to '%v'", s.me(), err, ckt.RemotePeerID)
	}
}

// also check if we need to try and contact
// our cluster again.
// Use immediately to send a heartbeat right
// away, even if nothing else is needed.
// If !immediately, we wait until it has
// been leaderBeatDur() before sending.
// This may use more bandwidth
// than is strictly necessary.
func (s *TubeNode) leaderSendsHeartbeats(immediately bool) {
	if s.role != LEADER {
		//vv("%v as non leader, ignoring stray leader hb timeout, nil out s.leaderSendsHeartbeatsCh", s.me())
		s.leaderSendsHeartbeatsCh = nil
		return
	}

	if s.isRegularTest() {
		if false { // s.cfg.testNum == 65 {
			alwaysPrintf("%v \n-------->>>    leaderSendsHeartbeats() len(s.ckt)=%v; len(s.peers)=%v  <<<--------\n", s.me(), len(s.cktReplica), len(s.peers))
		}
	}

	defer func() {
		s.resetLeaderHeartbeat("leaderSendsHeartbeats")
	}()

	begIdx, begTerm, leaderLastLogIndex, endTerm := s.getRaftLogSummary()
	_, _, _ = begIdx, begTerm, endTerm

	uncontactedMemberName := make(map[string]bool)
	if s.state != nil {
		if s.state.MC != nil {
			for name := range s.state.MC.PeerNames.All() {
				if name != s.name { // no need to contact self.
					uncontactedMemberName[name] = true
				}
			}
		}
		if s.state.ShadowReplicas != nil {
			for name := range s.state.ShadowReplicas.PeerNames.All() {
				if name != s.name { // no need to contact self.
					uncontactedMemberName[name] = true
				}
			}
		}

		// nodes recently removed from MC peers are added
		// to the Observers set with a small count of
		// gcAfterHeartbeatCount, and thus we try to tell them
		// about the new membership for a couple of
		// heartbeats. This lets them know they can
		// shut down if desired, since they were
		// deliberately removed from the MC and the
		// lack of heartbeats coming to them in the
		// near future is not due to leader failure.
		for name, det := range s.state.Observers.PeerNames.All() {
			if name == s.name {
				continue
			}
			cktP, ok := s.cktall[det.PeerID]
			if ok {
				s.sendAppendEntriesEmptyHeartbeat(det.PeerID, cktP.PeerName, cktP.PeerServiceName, cktP.PeerServiceNameVersion)
				// zero gc count means never garbage collect
				if det.gcAfterHeartbeatCount > 0 {
					if det.gcAfterHeartbeatCount == 1 {
						s.state.Observers.PeerNames.Delkey(name)
					} else {
						det.gcAfterHeartbeatCount--
					}
				}
			} else {
				//vv("%v no circuit available to this observer currently: '%v'", s.name, cktP.PeerName)
				if name != s.name { // no need to contact self.
					uncontactedMemberName[name] = true
				}
			}
		}
	}

	// in leaderSendsHeartbeats here.
	//for name, det := range s.state.MC.PeerNames.All() {

	// so the way this works is: we try send to everyone
	// we have in cktReplica. If any members are not
	// in cktReplica then at the end we try to establish
	// circuits with them.

	// also, we want to heartbeat to newly excluded replicas
	// too, so they can know that they are out of the
	// membership and shut down. so basically any replicas
	// we know about should get heartbeats, even if not
	// in the cktReplica or s.state.MC; so that they can
	// find out the current MC and MC.IsCommitted.
	// Observers too: they want this information.

	//vv("%v leaderSendsHeartbeats() to heartbeat to '%v'", s.me(), keys(s.cktReplica))

	//for peerID, cktP := range sorted(s.cktReplica) {
	for peerID, cktP := range sorted(s.cktall) {

		delete(uncontactedMemberName, cktP.PeerName)

		switch cktP.PeerServiceName {
		case TUBE_REPLICA: // should include shadows
			// ok
		default:
			continue // can skip TUBE_CLIENT, TUBE_OBS_MEMBERS
		}

		//vv("%v leaderSendsHeartbeats() to heartbeat to '%v'", s.me(), cktP.PeerName)
		mcVersStale := false
		if cktP.MC != nil && s.state.MC.VersionGT(cktP.MC) {
			mcVersStale = true
		}
		foll, ok := s.peers[peerID]
		if !ok {
			foll = s.newRaftNodeInfo(peerID, cktP.PeerName, cktP.PeerServiceName, cktP.PeerServiceNameVersion)
			if s.peers == nil {
				// test 402 membership_test gets in here
				// while trying to bootstrap up a cluster.
				s.candidateReinitFollowerInfo()
			}
			s.peers[peerID] = foll

			// we are probably just shutting down the
			// test, and just deleted from s.peers? no
			// then it would not be in s.ckt.
			//panic(fmt.Sprintf("%v should have entry in s.peers for %v by now", s.me(), peer)) // hit this on test003, added an info entry
			// could just be a test shutdown thing?
		}

		// send missing entries to followers who are behind.
		if cktP.PeerName == "node_2" {
			//vv("%v is foll behind? foll='%v'; leaderLastLogIndex(%v) > (%v)foll.LargestCommonRaftIndex == %v", s.me(), cktP.PeerName, leaderLastLogIndex, foll.LargestCommonRaftIndex, leaderLastLogIndex > foll.LargestCommonRaftIndex)
		}

		if leaderLastLogIndex > foll.LargestCommonRaftIndex {
			s.sendAppendEntriesTo(peerID, cktP.PeerName, cktP.PeerServiceName, cktP.PeerServiceNameVersion, foll.LargestCommonRaftIndex+1, 0, nil)
		} else {
			// if we have not hear from them recently...
			// Note that we do the same optimization that
			// RaftConensus.cc:2325 does when it resets the
			// leader beat for a single peer during AE response
			// handling. Likewise, we set foll.LastFullPong in
			// handleAppendEntriesAck() when we hear from a follower.
			lastHeardDur := time.Now().Sub(foll.LastHeardAnything)

			if mcVersStale || immediately || lastHeardDur > s.leaderBeatDur() {
				if false { // cktP.PeerName == "node_2" {
					//vv("%v leader trying to send AE heartbreat to '%v'", s.name, cktP.PeerName)
				}
				s.sendAppendEntriesEmptyHeartbeat(peerID, cktP.PeerName, cktP.PeerServiceName, cktP.PeerServiceNameVersion)
			}
		}
	}

	if !s.cfg.NoBackgroundConnect {
		//if s.cfg.testNum != 52 && s.cfg.testNum != 51 {
		//vv("%v leaderSendsHeartbeats sees uncontactedMemberName: '%#v'; len(s.cktReplica) = %v", s.me(), uncontactedMemberName, len(s.cktReplica))
		//}
		for peerName := range uncontactedMemberName {
			//if s.cfg.testNum != 52 && s.cfg.testNum != 51 {
			//vv("%v ugh in MC but NOT in cktReplica: we see uncontactedMemberName '%v'; calling connectInBackgroundIfNoCircuitTo", s.me(), peerName)
			//}
			s.connectInBackgroundIfNoCircuitTo(peerName, "leaderSendsHeartbeats") // call 1
		}
	}
}

func (s *TubeNode) newAE() (ae *AppendEntries) {
	ae = &AppendEntries{
		ClusterID:           s.ClusterID,
		FromPeerID:          s.PeerID,
		FromPeerName:        s.name,
		FromPeerServiceName: s.PeerServiceName,
		LeaderTerm:          s.state.CurrentTerm,
		LeaderLLI:           s.lastLogIndex(),
		LeaderLLT:           s.lastLogTerm(),
		LeaderID:            s.PeerID,
		LeaderURL:           s.URL,
		LeaderName:          s.name,
		// let caller fill in, might be empty heartbeat
		//	PrevLogIndex:
		//	PrevLogTerm:
		//	Entries:
		LeaderCommitIndex:          s.state.CommitIndex,
		LeaderCommitIndexEntryTerm: s.state.CommitIndexEntryTerm,
		LogTermsRLE:                s.wal.getTermsRLE(),
		AEID:                       rpc.NewCallID(""),
		MC:                         s.state.MC.Clone(),
		ShadowReplicas:             s.state.ShadowReplicas.Clone(),
		PeerID2LastHeard:           make(map[string]time.Time),
		LeaderCompactIndex:         s.wal.logIndex.BaseC,
		LeaderCompactTerm:          s.wal.logIndex.CompactTerm,
		LeaderHLC:                  s.hlc.CreateSendOrLocalEvent(),
	}
	// assert agreement, so it does not matter which we
	// write to LeaderCompactIndex/Term
	if s.state.CompactionDiscardedLast.Index != s.wal.logIndex.BaseC {
		panic(fmt.Sprintf("%v: these should be in sync! s.state.CompactionDiscardedLastIndex(%v) != s.wal.logIndex.BaseC(%v)", s.name, s.state.CompactionDiscardedLast.Index, s.wal.logIndex.BaseC))
	}
	if s.state.CompactionDiscardedLast.Term != s.wal.logIndex.CompactTerm {
		panic(fmt.Sprintf("%v: these should be in sync! s.state.CompactionDiscardedLast.Term(%v) != s.wal.logIndex.CompactTerm(%v)", s.name, s.state.CompactionDiscardedLast.Term, s.wal.logIndex.CompactTerm))
	}
	for peerID, foll := range s.peers {
		ae.PeerID2LastHeard[peerID] = foll.LastHeardAnything
	}
	return
}

func (s *TubeNode) newAEtoPeer(toPeerID, toPeerName, toPeerServiceName, toPeerServiceNameVersion string, sendThese []*RaftLogEntry) (ae *AppendEntries, foll *RaftNodeInfo) {

	if toPeerServiceName != TUBE_REPLICA {
		panic(fmt.Sprintf("only send AE to replica; not '%v'", toPeerName))
	}

	ae = s.newAE()
	//vv("%v newAE called to '%v', AEID='%v', stack=\n%v", s.me(), toPeerName, ae.AEID, stack())

	var ok bool
	foll, ok = s.peers[toPeerID]
	if !ok {
		foll = s.newRaftNodeInfo(toPeerID, toPeerName, toPeerServiceName, toPeerServiceNameVersion)
		s.peers[toPeerID] = foll
	}
	foll.UnackedPing.Sent = time.Now()
	foll.UnackedPing.Term = ae.LeaderTerm
	foll.UnackedPing.AEID = ae.AEID
	foll.MC = ae.MC.Clone()
	//foll.UnackedPing.Span = Entries(sendThese).span()

	return ae, foll
}

func (s *TubeNode) sendAppendEntriesEmptyHeartbeat(peerID, peerName, peerServiceName, peerServiceNameVersion string) {

	if s.name == peerName {
		return // no need to send to self
	}

	ae, foll := s.newAEtoPeer(peerID, peerName, peerServiceName, peerServiceNameVersion, nil)
	_ = foll

	aeFrag := s.newFrag()
	bts, err := ae.MarshalMsg(nil)
	panicOn(err)
	aeFrag.Payload = bts
	aeFrag.FragOp = AppendEntriesMsg
	aeFrag.FragSubject = "heartbeat AppendEntries"

	// in sendAppendEntriesEmptyHeartbeat()
	cktP, ok := s.cktall[peerID]
	if !ok {
		//vv("%v don't know how to contact '%v' to heartbeat. Assuming they died. s.cktReplica = '%#v'", s.me(), peerID, s.cktReplica)
		panic("what??")
		return
	}
	ckt := cktP.ckt
	//vv("%v sendAppendEntriesEmptyHeartbeat to %v", s.me(), rpc.AliasDecode(peer))
	err = s.SendOneWay(ckt, aeFrag, -1, 0)
	_ = err // don't panic on halting.
	if err != nil {
		alwaysPrintf("%v non nil error on heartbeat to '%v': '%v'", s.me(), ckt.RemotePeerName, err)
		// saw lots of trying to reconnect to auto-cli, which
		// seems bad/unlikely to succeed. try forcing reconnect
		// to get a new ckt to the actual server?
		// =  non nil error 'error: client local: ''/name='auto-cli-from-srv_node_0-to-127.0.0.1:51636___GaVTgBAUooRNmFObf-rW' failed to connect to server: 'dial tcp 127.0.0.1:51636: connect: connection refused'' on heartbeat to 'kdZXXCTlZWrbuLB4GOmTGO6jatuZ'
	}

} // end sendAppendEntriesEmptyHeartbeat

func (s *TubeNode) wasConnRefused(err error, ckt *rpc.Circuit) bool {
	if err == nil {
		return false
	}
	const connRefused = "connect: connection refused"
	if strings.Contains(err.Error(), connRefused) {
		alwaysPrintf("%v saw '%v', forcing recompute of ckt", s.me(), connRefused)

		cktP, ok := s.cktall[ckt.RemotePeerID]
		if ok && cktP != nil {
			s.deleteFromCktAll(cktP)
			s.adjustCktReplicaForNewMembership()
		}
		return true
	}
	return false
}

type Entries []*RaftLogEntry

func (es Entries) span() (r *LogEntrySpan) {
	n := len(es)
	if n == 0 {
		return
	}
	r = &LogEntrySpan{
		Beg:  es[0].Index,
		Endx: es[n-1].Index + 1,
	}
	return
}

func (s *TubeNode) bcastAppendEntries(es []*RaftLogEntry, prevLogIndex, prevLogTerm int64) {

	if s.clusterSize() <= 1 {
		//vv("%v bcastAppendEntries exit early: nobody else to send to", s.me())
		return
	}
	//vv("%v top bcastAppendEntries of es[0]='%v'", s.me(), es[0])
	//defer func() {
	//	vv("%v end bcastAppendEntries of es[0]='%v'", s.me(), es[0])
	//}()

	ae := s.newAE()
	ae.PrevLogIndex = prevLogIndex
	ae.PrevLogTerm = prevLogTerm
	ae.Entries = es

	//rangeAE := Entries(es).span()

	aeFrag := s.newFrag()
	bts, err := ae.MarshalMsg(nil)
	panicOn(err)
	aeFrag.Payload = bts
	aeFrag.FragOp = AppendEntriesMsg
	aeFrag.FragSubject = "AppendEntries"

	// chapter 4.1: (page 36)
	// Only send AE to peers in our MC.
	for peer, info := range sorted(s.peers) {
		if peer == s.PeerID {
			continue // skip self
		}
		if info.PeerName == "" {
			panic(fmt.Sprintf("yikes! empty info.PeerName for '%#v'", info))
		}

		if s.state.MC == nil {
			// election_tests want to do this so as to hold
			// an election and not just designate a leader;
			// tests from way back we don't really want to
			// complicate or update.
			// Update: now it is time to update them if need be!
			//if !s.cfg.isTest {
			//vv("%v ugh. no MC ???...", s.me())
			panic("fix this! must always have a MC")
			//}
		}
		if s.state.ShadowReplicas == nil {
			panic("fix this! must have ShadowReplicas, even if empty")
		}
		// in bcastAppendEntries here.
		if s.skipAEBecauseNotReplica(info.PeerID,
			info.PeerName, info.PeerServiceName) {
			alwaysPrintf("%v skipping non MC replica: %v", s.me(), info.PeerName)
			continue
		}

		// in bcastAppendEntries here. We used to use cktReplica,
		// but now we filter to only replicas and shows in the
		// above skipAEBecauseNotReplica() filter and use cktall.
		// (cktReplica lacks the shadow replicas).
		cktP, ok := s.cktall[peer]
		if !ok {
			alwaysPrintf("%v don't know how to contact '%v' to heartbeat: not in cktReplica. Assuming they are gone; trying others", s.me(), info.PeerName)
			continue
		}
		ckt := cktP.ckt
		//vv("%v send AE to %v", s.me(), cktP.PeerName)
		// tricky to figure out which is the
		// "last" send, so free manually below.
		const willFreeOurselves = 1
		err = s.SendOneWay(ckt, aeFrag, -1, willFreeOurselves)

		_ = err // don't panic on halting.
		if err != nil {
			alwaysPrintf("%v non nil error '%v' on heartbeat to '%v'", s.me(), err, ckt.RemotePeerID)
		}
		info.UnackedPing.Sent = time.Now()
		info.UnackedPing.Term = ae.LeaderTerm
		info.UnackedPing.AEID = ae.AEID
	}
	s.MyPeer.FreeFragment(aeFrag)

	// don't need to heartbeat for a while longer.
	s.resetLeaderHeartbeat("bcastAppendEntries")
}

// ckt might be nil
func (s *TubeNode) peerJoin(frag *rpc.Fragment, ckt *rpc.Circuit) {

	// since this is called for every message,
	// we want it to be pretty fast.

	//vv("%v peerJoin frag from '%v'; onlineReplicas0 = '%v'", s.me(), rpc.AliasDecode(frag.FromPeerID), onlineReplicas0)

	peer := frag.FromPeerID
	if ckt != nil {
		if peer != ckt.RemotePeerID {
			if frag.FragSubject == "RedirectTicketToLeader" {
				tmpTkt := &Ticket{}
				_, err := tmpTkt.UnmarshalMsg(frag.Payload)
				panicOn(err)
				vv("about to panic on frag.FromPeerID(%v) != ckt.RemotePeerID(%v); on a 'RedirectTicketToLeader' here is the original Ticket: '%v'", frag.FromPeerID, ckt.RemotePeerID, tmpTkt)

			}
			//panic(fmt.Sprintf(
			vv("%v: %v sanity check failed, frag should be from ckt. frag.FromPeerID='%v' but ckt.RemotePeerID='%v'; \n frag='%v'\n ckt = '%v'", s.me(), nice(time.Now()), peer, ckt.RemotePeerID, frag, ckt) // just saw again.
			if frag.ToPeerID == s.PeerID {
				vv("since it was sent to me, process it anyway")
			} else {
				panic("fit this mis-directed packet!")
			}
			/*
								panic: 2026-01-14T03:54:41.687Z sanity check failed, frag should be from ckt. frag.FromPeerID='2iC9T3fXs9WmvcRqihtFEd48OfGmrJ6P9MAvVQuKnrqy' but ckt.RemotePeerID='W_U8id0xDApdmaFJw3oMLVmpwCJNK0EIopYLeo5l1_8u';

								 frag='&rpc25519.Fragment{
								    "Created": 2026-01-14T03:54:41.687Z,
								    "FromPeerID": "2iC9T3fXs9WmvcRqihtFEd48OfGmrJ6P9MAvVQuKnrqy" 2iC9T3fXs9WmvcRqihtFEd48OfGmrJ6P9MAvVQuKnrqy, // <<<<<<<< checking this failed!
								    "FromPeerName": "member_SRxDCbOR3UjVYclM7rzQ",
								    "FromPeerServiceName": "tube-client",
								    "ToPeerID": "CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw" (node_1) CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw ,
								    "ToPeerName": "node_1",
								    "ToPeerServiceName": "tube-replica",
								    "CircuitID": "PiSRITfW_jlqeim1n-P_A27YH2uWlv0oh3oxTdK8-aJx (tube-client)",
								    "Serial": 413,
								    "Typ": CallPeerTraffic,
								    "FragOp": 5,
								    "FragSubject": "RedirectTicketToLeader",
								    "FragPart": 0,
								    "Args": map[string]string{"#fromBaseServerAddr":"tcp://100.89.245.101:35817", "#fromBaseServerName":"srv_member_SRxDCbOR3UjVYclM7rzQ", "#fromPeerServiceNameVersion":"", "#fromServiceName":"tube-client", "ClusterID":"123", "leader":"CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw", "leaderName":"node_1"},
								    "Payload": (len 1295 bytes),
								    "Err": "",
								}'

					 ckt = '&Circuit{
					     CircuitSN: 362,
					          Name: "tube-ckt",
					     CircuitID: "PiSRITfW_jlqeim1n-P_A27YH2uWlv0oh3oxTdK8-aJx (tube-client)",

					   LocalPeerID: "CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw" (node_1) CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw ,
					 LocalPeerName: "node_1",

					  RemotePeerID: "W_U8id0xDApdmaFJw3oMLVmpwCJNK0EIopYLeo5l1_8u" ,
					RemotePeerName: "",

					 LocalServiceName: "tube-replica",
					RemoteServiceName: "tube-client",

					 LocalPeerServiceNameVersion: "",
					RemotePeerServiceNameVersion: "",

					     PreferExtant: false,
					   MadeNewAutoCli: true,

					 // LocalCircuitURL: "tcp://100.89.245.101:7001/tube-replica/CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw/PiSRITfW_jlqeim1n-P_A27YH2uWlv0oh3oxTdK8-aJx",
					 // RemoteCircuitURL: "tcp://100.89.245.101:35817/tube-client/W_U8id0xDApdmaFJw3oMLVmpwCJNK0EIopYLeo5l1_8u/PiSRITfW_jlqeim1n-P_A27YH2uWlv0oh3oxTdK8-aJx",

					   UserString: "",
					    FirstFrag: &rpc25519.Fragment{
					    "Created": 2026-01-14T03:54:27.640Z,
					    "FromPeerID": "CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw" (node_1) CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw ,
					    "FromPeerName": "node_1",
					    "FromPeerServiceName": "tube-replica",
					    "ToPeerID": "W_U8id0xDApdmaFJw3oMLVmpwCJNK0EIopYLeo5l1_8u" ,
					    "ToPeerName": "",
					    "ToPeerServiceName": "tube-client",
					    "CircuitID": "PiSRITfW_jlqeim1n-P_A27YH2uWlv0oh3oxTdK8-aJx (tube-client)",
					    "Serial": 22933,
					    "Typ": CallNone,
					    "FragOp": 17,
					    "FragSubject": "NotifyClientNewLeader",
					    "FragPart": 0,
					    "Args": map[string]string{"ClusterID":"123", "leaderID":"CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw", "leaderName":"node_1", "leaderURL":"tcp://100.89.245.101:7001/tube-replica/CBjzVlb6UoN6woxH9h7oxIHnxAH54nrrk5sNrqUMkrIw"},
					    "Payload": (len 2069 bytes),
					    "Err": "",
					}
					}' [recovered, repanicked]

				goroutine 26 [running]:
				github.com/glycerine/rpc25519/tube.(*TubeNode).Start.func1()
					/home/jaten/rpc25519/tube/tube.go:739 +0xf5
				panic({0xcadb00?, 0xcccf345040?})
					/mnt/oldrog/usr/local/go1.25.3/src/runtime/panic.go:783 +0x132
				github.com/glycerine/rpc25519/tube.(*TubeNode).peerJoin(0xc000181008, 0xcc8e198a20, 0xccac957560)
					/home/jaten/rpc25519/tube/tube.go:8206 +0x65e
				github.com/glycerine/rpc25519/tube.(*TubeNode).Start(0xc000181008, 0xc0001faa50, {0xf323e0, 0xc0001db9a0}, 0xc0004ce700)
					/home/jaten/rpc25519/tube/tube.go:1549 +0x2d9a
				github.com/glycerine/rpc25519.(*peerAPI).unlockedStartLocalPeer.func1()
					/home/jaten/rpc25519/ckt.go:1449 +0x109
				created by github.com/glycerine/rpc25519.(*peerAPI).unlockedStartLocalPeer in goroutine 1
					/home/jaten/rpc25519/ckt.go:1443 +0x659


			*/
			// wtf. how did this happen??? still seen every with cryrand long PeerID.
			/*
				panic: sanity check failed, frag should be from ckt. frag.FromPeerID='CSVruwG6F6Z1sPs4hCJHgG6Xtige' but ckt.RemotePeerID='EK5_4sQPPAcbr5Rq0yE20Ub2cBP0' [recovered, repanicked]

				goroutine 26 [running]:
				github.com/glycerine/rpc25519/tube.(*TubeNode).Start.func1()
					/home/jaten/rpc25519/tube/tube.go:729 +0xf5
				panic({0xcab940?, 0xc3c937cb90?})
					/mnt/oldrog/usr/local/go1.25.3/src/runtime/panic.go:783 +0x132
				github.com/glycerine/rpc25519/tube.(*TubeNode).peerJoin(0xc0004b3c08, 0xc27e77d8c0, 0xc0371faea0)
					/home/jaten/rpc25519/tube/tube.go:7935 +0x5d4
				github.com/glycerine/rpc25519/tube.(*TubeNode).Start(0xc0004b3c08, 0xc0001faa50, {0xf2f2e0, 0xc0001db9a0}, 0xc0004ce700)
					/home/jaten/rpc25519/tube/tube.go:1524 +0x2cda
				github.com/glycerine/rpc25519.(*peerAPI).unlockedStartLocalPeer.func1()
					/home/jaten/rpc25519/ckt.go:1449 +0x109
				created by github.com/glycerine/rpc25519.(*peerAPI).unlockedStartLocalPeer in goroutine 1
					/home/jaten/rpc25519/ckt.go:1443 +0x659

			*/
		}
	}

	// in peerJoin, generic for clients or replicas => use cktall.
	cktP, ok := s.cktall[peer]
	if !ok {
		if ckt != nil {
			//vv("new ckt! not previously registered in s.cktall: peer='%v'; ckt='%#v'; s.cktall = '%#v'; frag = '%v'", peer, ckt, s.cktall, frag)

			// seen when:
			// we are node 1
			// ckt remote is node 2
			// node2 is establishing the grid, and this is our first
			// encounter with them. So we just need to register them.
			var rejected bool
			cktP, rejected = s.addToCktall(ckt)
			if rejected {
				alwaysPrintf("ouch 777777777 ckt rejected from cktall: '%v'", ckt.RemotePeerName)
				panic("when ckt rejected? 777")
			}
		}
	}

	isReplica := (cktP.PeerServiceName == TUBE_REPLICA)
	if isReplica && s.cfg.isTest {
		s.verifyCluster(frag.FromPeerID)
	}

	_, known := s.peers[frag.FromPeerID]
	//vv("%v peerJoin, known=%v for frag.FromPeerID = '%v'; onlineReplicaCount()=%v; len(peers)=%v; len(cktall)=%v; isReplica='%v'", s.me(), known, rpc.AliasDecode(frag.FromPeerID), s.onlineReplicaCount(), len(s.peers), len(s.cktall), isReplica)
	if known || !isReplica {
		return
	}

	// INVAR: have new replica, probably needs heartbeating
	// immediately to let it know a leader is present
	// (if that is the case).

	// conservatively assume this is a new peer
	// by giving them a fresh info status.
	// It would be bad to avoid sending them
	// log entries if we were mistaken about what
	// they have.
	if s.role == LEADER {
		info := s.newRaftNodeInfo(frag.FromPeerID, frag.FromPeerName, frag.FromPeerServiceName, frag.FromPeerServiceNameVersion)
		// clients get added here too; they still
		// will not get heartbeats or vote requests.
		//vv("%v about to assign to s.peers=%p", s.name, s.peers)
		if s.peers == nil {
			// test 402 membership_test gets in here
			// while trying to bootstrap up a cluster.
			s.candidateReinitFollowerInfo()
		}
		s.peers[frag.FromPeerID] = info // in peerJoin, adding to s.peers

		//vv("%v added %v to Peers. cluster size including ourself = %v; size of state.peers = %v; onlineReplicaCount() = %v", s.me(), frag.FromPeerID, s.clusterSize(), len(s.peers), s.onlineReplicaCount()) // not seen, manual

		//vv("%v as leader sending heartbeat to new peer (and everyone)", s.me())
		s.leaderSendsHeartbeats(true)
	}
}

func (s *TubeNode) onlineReplicaCount() (online int) {
	// cktReplica never includes ourselves, so we must + 1
	if s.cktReplica == nil {
		panic("should have s.cktReplica not nil by now")
	}
	return len(s.cktReplica) + 1
	// online = 1
	// for _, cktP := range s.cktReplica {
	// 	if cktP.isReplica {
	// 		online++
	// 	} else {
	// 		//vv("cktReplica '%v' is not marked replica", rpc.AliasDecode(cktP.ckt.RemotePeerID))
	// 		panic("fix this")
	// 	}
	// }
	return
}

// intentionally panics on an empty log. i.e.
// this asserts that our log is not empty and
// returns its last index.
// Use s.lastLogIndex() to not assert.
func (s *TubeNode) mustGetLastLogIndex() (lli int64) {
	lli = s.wal.LastLogIndex()
	if lli <= 0 {
		panic("no log available!")
	}
	return
}

func (s *TubeNode) logUpToDateAsOurs(rv *RequestVote) (ans bool) {

	//defer func() {
	//vv("%v finishing logUpToDateAsOur(ans='%v');  ourMC(%v); rv.MC(%v)", s.me(), ans, s.state.MC.Short(), rv.MC.Short())
	//}()

	if s.state != nil &&
		s.state.MC != nil &&
		// Algorithm 1, page 6, line 26 of mongo paper:
		s.state.MC.VersionGT(rv.MC) {

		//vv("%v logUpToDateAsOur fails: mongo condition: requestor (%v) is behind us. ourMC(%v) > rv.MC(%v)", s.me(), rv.FromPeerName, s.state.MC.Short(), rv.MC.Short())
		return false
	}
	// Algorithm 1, page 6, line 27 of mongo paper:
	// update: if we use <= here, 401 we never get an election won.
	// and apparently not due to pre-voting. We will also
	// verify the original condition in tallyVote
	// before becoming leader. Candidates don't heartbeat.
	if rv.FromPeerName != s.name &&
		rv.CandidatesTerm < s.state.CurrentTerm {

		//vv("%v logUpToDateAsOur fails mongo condition: requestor(%v) rv.CandidatesTerm(%v) <= our CurrentTerm(%v); is pre-vote: %v", s.me(), rv.FromPeerName, rv.CandidatesTerm, s.state.CurrentTerm, rv.IsPreVote)
		return false
	}

	ourLastLogIndex, ourLastLogTerm := s.wal.LastLogIndexAndTerm()
	if ourLastLogIndex < 1 {
		//vv("%v logUpToDateAsOurs: we have no log", s.name)
		return true // we have no log
	}

	//defer func() {
	//	//vv("%v logUpToDateAsOurs: our log (%v:term %v) vs (%v:term %v) from %v (%v): as up to date:%v", s.name, last.Index, last.Term, rv.LastLogIndex, rv.LastLogTerm, rv.FromPeerName, rv.FromPeerID, ans)
	//}()

	// page 23 - page 24
	// "If the logs have last entries with different terms,
	// then the log with the term is more up to date."
	if rv.LastLogTerm > ourLastLogTerm {
		return true // requestor is more up to date
	}
	if rv.LastLogTerm < ourLastLogTerm {
		//vv("requestor is behind us rv.LastLogTerm(%v) < ourLastLogTerm(%v)", rv.LastLogTerm, ourLastLogTerm)
		return false // requestor is behind us.
	}
	// INVAR: terms are equal
	// page 24
	// "If the logs end with the same term, then the
	// whichever log is longer is more up to date."
	if rv.LastLogIndex >= ourLastLogIndex {
		return true
	}
	//vv("end false")
	return false
}

// in tests, this was deleting our MC just after SimBoot.
func (s *TubeNode) onRestartRecoverPersistentRaftStateFromDisk() (err error) {

	//vv("%v top onRestartRecoverPersistent; MC='%v'", s.name, s.state.MC)
	//defer func() {
	//vv("%v end of onRestartRecoverPersistent; MC='%v'", s.name, s.state.MC)
	//}()

	var st *RaftState

	var path string
	// if we force s.cfg.NewRaftStatePersistor()
	// below, without a disk, that is always empty state,
	// which was causing 065 shadow_test to lose its MC.
	// Do we need to give 065 an s.saver??
	if s.cfg.isTest { // 065 why was this here???: && s.saver != nil {

		//vv("%v isTest true, s.saver = %p)", s.name, s.saver)
		// allow testSetupFirstRaftLogEntryBootstrapLog()
		// to pre-inject wal and state.
		st = s.state
	} else {
		path = s.GetPersistorPath()
		//vv("%v creating new saver for path='%v'", s.name, path)
		s.saver, st, err = s.cfg.NewRaftStatePersistor(path, s, false)
		panicOn(err)
		if err != nil {
			return
		}
	}
	if st == nil {
		//vv("%v loaded RaftState st == nil, but cannot bail early b/c need the init below", s.name)
		// this is super bad, since we have so much init
		// below! i.e. ack.MinElectionTimeoutDur == 0 and such.
		//so don't: return early!
	} else {
		lastLogTerm := s.wal.LastLogTerm()
		_ = lastLogTerm
		//vv("%v restored state from disk path='%v': lastLogTerm=%v; st='%#v'", s.me(), path, lastLogTerm, st)
	}

	// be sure we don't overwrite some things like our
	// just restored WAL.
	if st != nil {
		s.state = st
	} // else leave our state in place
	// we want to let the Raft mechanism and pre-vote
	// do any CurrentTerm changes, so we don't race ahead.
	// So no CurrentTerm updates here.

	// we don't want to vote twice in a term, but not voting
	// is fine; so if they have changed their id we won't
	// vote for them again, like if we cannot find them.

	// get rid of the stuff that is not supposed to
	// persist across restarts. Peers will have new IDs for instance.

	if st != nil {
		// Serial is internal debug stuff anyway, not that critical
		s.state.Serial = atomic.AddInt64(&debugSerialRaftState, 1)
		s.state.PeerName = s.name
		s.state.PeerID = s.PeerID       // probably changed
		s.state.ClusterID = s.ClusterID // probably changed
	}
	s.votes = nil
	s.yesVotes = 0
	s.noVotes = 0
	if s.state.KVstore == nil {
		// might not have had anything on disk.
		s.state.KVstore = newKVStore()
	}

	// raft does not require CommitIndex to be on disk,
	// as it can be reconverged from network messages,
	// but its cheap and should speed up recover, so we keep it.
	// rather than changing s.state.CommitIndex after recover.

	// re s.LastApplied: it should be persistent.
	// Raft: Last applied volatility should match the state machine.
	// "If the state machine is volatile, lastApplied should
	//  be volatile. If the state machine is persistent,
	//  lastApplied should be just as persistent."
	// To avoid re-applying (violates linearizability) a command.
	// So we will let it restore from disk.

	// CurrentTerm must be restored from disk. (so don't reset it here!)

	// page 27, section 3.8
	// "If a server loses any of its persistent state,
	// it cannot safely rejoin the cluster with its prior identity.
	// Such a server can usually be added back into the cluster
	// with a new identity by invoking a cluster membership change"

	s.peers = nil

	//zz("%v restored path '%v': s.state.CurrentTerm='%v'", s.me(), path, s.state.CurrentTerm)

	// Critical defaults
	// If unset (left at zero), these settings are used:
	//
	// if s.cfg.HeartbeatDur is 0, default to 50 msec; and
	//
	// if s.cfg.MinElectionDur  is 0, default to 10*s.cfg.HeartbeatDur;
	//
	// Hence if both were 0, then 50msec (heartbeat) and 500msec (election) result.
	// The actually elections are then uniformly random between
	// [MinElectionDur, 2*MinElectionDur].
	// (must set election before it is cached below)
	s.setConfigDefaultsIfZero()

	if s.insaneConfig() {
		//vv("%v insaneConfig detected. self-shutdown.", s.me())
		panic("fix the config")
		return
	}

	// cache these frequently accessed timeout periods.
	s.cachedMinElectionTimeoutDur = s.minElectionTimeoutDur()
	s.cachedMaxElectionTimeoutDur = s.maxElectionTimeoutDur()

	//vv("%v using election '%v', heartbeat '%v'", s.me(), s.cachedMinElectionTimeoutDur, s.cfg.HeartbeatDur)

	// It is actually desirable that our PeerID is random
	// and will change upon restart-- as per this discussion
	// https://groups.google.com/g/raft-dev/c/fbwv8-qMFYE/m/1qJP7XpCNAAJ
	//
	// Mike Percy 2016 April 20 raft-dev
	// "This is exactly right. If you forget anything,
	// you must also forget everything, and you must also
	// forget who you are and reject requests that come
	// to you with your "old" name. For exactly the reasons you describe.
	//
	// When we noticed this, we quickly added "target node id"
	// to all of our Raft-related RPCs and validated that
	// the message received was really intended for
	// us (and this incarnation of "us").

	return
} // end onRestartRecoverPersistentRaftStateFromDisk

// Always returns s.state.MC, which might be nil.
// Calls s.adjustCktReplicaForNewMembership() to
// start circuits to our member replicas, if
// s.state.MC is available.
func (s *TubeNode) startCircuitsToLatestMC() *MemberConfig {

	if s != nil && s.state != nil && s.state.MC != nil {
		// if we have any cktall at this point, start
		// to populate cktReplica.
		s.adjustCktReplicaForNewMembership()
	}
	return s.state.MC
}

func (v *Vote) String() string {
	if v == nil {
		return "(nil Vote)"
	}
	return fmt.Sprintf(`Vote{
    FromPeerID: %v,
  FromPeerName: %v,
FromPeerCurrentTerm: %v,
     CandidatesTerm: %v,
   CandidateID: %v,
   VoteGranted: %v,
     IsPreVote: %v,
      MC: %v,
        Reason: "%v" }`,
		rpc.AliasDecode(v.FromPeerID), v.FromPeerName,
		rpc.AliasDecode(v.CandidateID), v.FromPeerCurrentTerm,
		v.CandidatesTerm, v.VoteGranted, v.IsPreVote,
		v.MC.Short(),
		v.Reason)
}

func (a *AppendEntries) String() string {
	if a == nil {
		return "(nil AppendEntries)"
	}
	es := ""
	const compressedVersion = true
	if compressedVersion {
		es = fmt.Sprintf("(len %v) ", len(a.Entries))
		for i, e := range a.Entries {
			if i == 0 {
				es += e.String()
			} else {
				es += ", " + e.String()
			}
		}
	} else {
		for i, e := range a.Entries {
			if i == 0 {
				es += e.LongString()
			} else {
				es += ", " + e.LongString()
			}
		}
	}
	if es == "" {
		es = "(empty entries)"
	}
	return fmt.Sprintf(`AppendEntries{
            ClusterID: %v,
         FromPeerName: %v,
           FromPeerID: %v,
             LeaderID: %v,
                 Term: %v,
         PrevLogIndex: %v,
          PrevLogTerm: %v,
    LeaderCommitIndex: %v,
LeaderCommitIndexEntryTerm: %v,
              Entries: %v,
                 AEID: %v,
    MC: %v
}
`,
		rpc.AliasDecode(a.ClusterID), a.FromPeerName, a.FromPeerID, rpc.AliasDecode(a.LeaderID), a.LeaderTerm, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommitIndex, a.LeaderCommitIndexEntryTerm, es, a.AEID, a.MC.Short())

}

func (e *RaftLogEntry) LongString() string {
	if e == nil {
		return "(nil RaftLogEntry)"
	}
	return fmt.Sprintf(`RaftLogEntry{
	  Term: %v,
	 Index: %v,
	Ticket: %v,
}
`, e.Term, e.Index, e.Ticket)
}
func (e *RaftLogEntry) String() string {
	if e == nil {
		return "(nil RaftLogEntry)"
	}
	if e.Ticket == nil {
		return fmt.Sprintf(`
[term: %v, i: %v, Ticket:nil]
`, e.Term, e.Index)

	}
	ticketVal := "(ticket is nil)"
	if e.Ticket != nil {
		ticketVal = string(e.Ticket.Val)
	}
	var extra string
	if e.Ticket.Op == MEMBERSHIP_SET_UPDATE && e.Ticket.MC != nil {
		extra = e.Ticket.MC.ShortProv()
	}

	return fmt.Sprintf(`
[_term: %v, i: %v, key:"%v", val:"%v", op:%v; committed:%v%v]
`, e.Term, e.Index, e.Ticket.Key, ticketVal, e.Ticket.Op, e.committed, extra)
}

func (s *TubeNode) newRaftNodeInfo(peerID, peerName, peerServiceName, peerServiceNameVersion string) (info *RaftNodeInfo) {
	info = &RaftNodeInfo{
		PeerName:               peerName,
		PeerServiceName:        peerServiceName,
		PeerServiceNameVersion: peerServiceNameVersion,
		PeerID:                 peerID,
		MatchIndex:             0,
	}
	return
}

// now used by candidate rather than leader,
// to collect info as early as possible, from votes.
func (s *TubeNode) candidateReinitFollowerInfo() {
	s.peers = make(map[string]*RaftNodeInfo)
	for peerID, cktP := range s.cktReplica {
		s.peers[peerID] = s.newRaftNodeInfo(peerID, cktP.PeerName, cktP.PeerServiceName, cktP.PeerServiceNameVersion)
	}
}

// voter about to reply with vote on candidate,
// the candidate sent us reqVote, we may want
// to update our info on them if we are leader.
func (s *TubeNode) updateLogInfo(reqVote *RequestVote, vote *Vote) {
	if s.role != LEADER {
		return
	}
	if reqVote.FromPeerServiceName != TUBE_REPLICA {
		return
	}
	follower, ok := s.peers[reqVote.FromPeerID]
	if !ok {
		follower = s.newRaftNodeInfo(reqVote.FromPeerID, reqVote.FromPeerName, reqVote.FromPeerServiceName, reqVote.FromPeerServiceNameVersion)
		s.peers[reqVote.FromPeerID] = follower
	}

	// reqVote.LastLogIndex // index of candidates last log entry
	// reqVote.LastLogTerm // term of candidates last log entry
	if reqVote.LastLogIndex > follower.LargestCommonRaftIndex {

		s.sendAppendEntriesTo(follower.PeerID, follower.PeerName, follower.PeerServiceName, follower.PeerServiceNameVersion, follower.LargestCommonRaftIndex+1, 0, nil)
	}
}

func (s *TubeNode) getRaftLogSummary() (begIdx, begTerm, endIdx, endTerm int64) {
	n := len(s.wal.raftLog)
	if n > 0 {
		begIdx = s.wal.raftLog[0].Index
		begTerm = s.wal.raftLog[0].Term
		endIdx = s.wal.raftLog[n-1].Index
		endTerm = s.wal.raftLog[n-1].Term
	}
	return
}

// cluster membership and role checks: allow
// both members and shadows to get AE.
func (s *TubeNode) skipAEBecauseNotReplica(followerID, followerName, followerServiceName string) bool {

	if followerServiceName == "" {
		panic("must not have empty followerServiceName")
	}
	if followerServiceName != TUBE_REPLICA {
		//vv("warning: not sending AE to client '%v' (we only send AE to replicas)", followerName)
		return true
	}
	// allow 020 etc election_tests to not need member config
	if s.isTest() && s.state.MC == nil {
		return false
	}

	if s == nil || s.state == nil || s.state.MC == nil {
		return false
	}
	_, isShadow := s.state.ShadowReplicas.PeerNames.Get2(followerName)
	if isShadow {
		return false // allow shadows.
	}
	_, isMember := s.state.MC.PeerNames.Get2(followerName)
	if isMember {
		return false
	}
	//vv("%v warning: not sending AE to replica that is not shadow and not a current member '%v'; MC='%v'", s.me(), followerName, s.state.MC)
	return true
}

// used by leader
// note
// in RaftConsensus.cc:2256, after log compression is
// implemented, this may have to send an (incremental?) state
// snapshot rather than log entries if they are
// no longer available due to compression.
func (s *TubeNode) sendAppendEntriesTo(followerID, followerName, followerServiceName, followerServiceNameVersion string, beginIndex int64, numLimit int, ack *AppendEntriesAck) {

	if s.skipAEBecauseNotReplica(followerID,
		followerName, followerServiceName) {
		return
	}
	if beginIndex < 1 {
		//panic("beginIndex must be >= 1")
		return
	}
	if s.role != LEADER {
		panic("should only be called by leader")
	}
	n := s.wal.LogicalLen()
	if n == 0 || beginIndex > n {
		return
	}

	// index of log entry immediately preceeding new ones
	var prevLogIndex int64
	var prevLogTerm int64 // term of prevLogIndex

	// assert s.state.CompactionDiscardedLastIndex is up to date
	if s.state.CompactionDiscardedLast.Index != s.wal.logIndex.BaseC {
		panic(fmt.Sprintf("s.state.CompactionDiscardedLastIndex(%v) != s.wal.logIndex.BaseC(%v); keep these in sync!", s.state.CompactionDiscardedLast.Index, s.wal.logIndex.BaseC))
	}

	compacted := s.state.CompactionDiscardedLast.Index
	if beginIndex <= compacted {
		// cannot send entries that have been compacted away.
		// note this might turn us into a heartbeat with no entries.
		//vv("%v due to compacted=%v, we are forced to update beginIndex from %v -> %v", s.me(), compacted, beginIndex, compacted+1)
		beginIndex = compacted + 1
	}
	sendThese, needSnapshot := s.wal.GetAllEntriesFrom(beginIndex)

	// NB if there is a gap then needSnapshot will
	// be true and sendThese will be nil.
	if needSnapshot { // 707 hits with compaction on.
		// even though it looks as though the problem is that
		// "we" the leader do not have entries, really this
		// means that the follower lacks some entries in
		// our state, so try sending them a snapshot.
		// Otherwise we get 100s of these building up, just
		// waiting.

		ackstr := "nil ack"
		if ack != nil {
			ackstr = ack.String()
		}
		alwaysPrintf("%v leader saw need snapshot. hmm. what is the real problem? follower '%v' wants beginIndex=%v; we have '%v';\n and the ack was: '%v'\n caller: '%v'", s.me(), followerName, beginIndex, s.wal.StringWithCommit(s.state.CommitIndex), ackstr, fileLine(2))
	}

	if numLimit > 0 && len(sendThese) > numLimit {
		sendThese = sendThese[:numLimit]
	}

	// try not to send a 12MB message with a huge 4900 entry catchup
	if len(sendThese) > 1 {
		lim := rpc.UserMaxPayload / 4
		sz := 0
		for i, e := range sendThese {
			sz += e.Msgsize()
			if sz >= lim {
				//vv("limiting batch of sent AE to %v entries (%v bytes)", i+1, sz)
				sendThese = sendThese[:(i + 1)]
				break
			}
		}
	}

	if beginIndex > 1 && beginIndex <= n {

		prevLog, err := s.wal.GetEntry(beginIndex - 1)
		if err == nil {
			prevLogIndex = prevLog.Index
			prevLogTerm = prevLog.Term
		} else {
			// note compacted == s.state.CompactionDiscardedLast.Index

			if compacted > 0 && beginIndex == compacted+1 {
				//vv("%v using prevLogTerm from s.state.CompactionDiscardedLast.Term=%v; prevLogIndex will be %v (from s.state.CompactionDiscardedLastIndex)", s.name, s.state.CompactionDiscardedLast.Term, s.state.CompactionDiscardedLastIndex)

				prevLogIndex = s.state.CompactionDiscardedLast.Index
				prevLogTerm = s.state.CompactionDiscardedLast.Term
			} else {
				// can s.wal.logIndex.BaseC save us?
				if beginIndex == s.wal.logIndex.BaseC+1 {
					prevLogIndex = s.wal.logIndex.BaseC
					prevLogTerm = s.wal.logIndex.CompactTerm
				} else {
					panic(fmt.Sprintf("no way to send the data requested b/c no prevLogIndex to anchor it: %v; does wal", err))
				}
			}
		}
		if prevLogIndex > 0 && prevLogTerm == 0 {
			panic(fmt.Sprintf("assert failed: cannot send prevLogTerm == 0 !?! s.wal.logIndex=%v", s.wal.logIndex))
		}
	}

	ae, _ := s.newAEtoPeer(followerID, followerName, followerServiceName, followerServiceNameVersion, sendThese)

	ae.PrevLogIndex = prevLogIndex
	ae.PrevLogTerm = prevLogTerm
	ae.Entries = sendThese

	//vv("%v sendAppendEntriesTo '%v'; beginIndex=%v; numLimit=%v;  after setting Entries, ae.AEID = %v", s.name, followerName, beginIndex, numLimit, ae.AEID)

	aeFrag := s.newFrag()
	bts, err := ae.MarshalMsg(nil)
	panicOn(err)
	if len(bts) > rpc.UserMaxPayload/2 {
		alwaysPrintf("yikes-ola! very large serialized AE: %v > %v", len(bts), rpc.UserMaxPayload/2)
		alwaysPrintf("analysis of %v entries in AE:\n", len(sendThese)) // 4900 ??? but implies 13027005 / 4900 = 2658.572 mean bytes per raft log entry.
		for i, e := range sendThese {
			alwaysPrintf("===== footprint of entry [%02d]:\n %v\n", i, e.Ticket.Footprint())
		}
	}
	aeFrag.Payload = bts
	aeFrag.FragOp = AppendEntriesMsg
	aeFrag.FragSubject = "AppendEntries"

	// in sendAppendEntriesTo here.
	// is called by handleAppendEntriesAck
	// and leaderSendsHeartbeats (both by leader),
	// so I think this should be cktReplica not ckt.
	//cktP, ok := s.cktReplica[followerID] // breaks shadows, we filter above.
	cktP, ok := s.cktall[followerID] // we filter above, so okay.
	if !ok {
		alwaysPrintf("%v don't know how to contact '%v' (%v) to send AE. Assuming they died.", s.me(), followerID, followerName)
		return
	}
	ckt := cktP.ckt

	if needSnapshot {
		s.handleRequestStateSnapshot(nil, ckt, "sendAppendEntriesTo needSnapshot")
	} else {
		//vv("%v sending appendEntries to %v", s.me(), rpc.AliasDecode(follower))
		err = s.SendOneWay(ckt, aeFrag, -1, 0)
		_ = err // don't panic on halting.
		if err != nil {
			// in sendAppendEntriesTo
			// can be very noisy if one node dies...
			//alwaysPrintf("%v non nil error '%v' on sending AE to '%v'", s.me(), err, ckt.RemotePeerID)
		}
	}
}

func (s *TubeNode) handleAppendEntriesAck(ack *AppendEntriesAck, ckt *rpc.Circuit) (err0 error) {

	s.hlc.ReceiveMessageWithHLC(ack.FollowerHLC)

	//if ack.PeerLogLastIndex >= 4 {
	//vv("%v \n-------->>>    handle AppendEntriesAck() from %v  <<<--------\n ack = %v", s.me(), ack.FromPeerName, ack)
	//}

	if s.isRegularTest() {
		//if s.cfg.testNum == 55 {
		//	vv("%v \n-------->>>    handle AppendEntriesAck() from %v  <<<--------\n ack = %v", s.me(), rpc.AliasDecode(ack.FromPeerID), ack)
		//}
		//vv("%v \n-------->>>    handle AppendEntriesAck()  <<<--------\n ack = %v", s.me(), ack)
		select {
		case panicAtCap(s.testGotAeAckFromFollower) <- ack.clone():
		case <-s.Halt.ReqStop.Chan:
			//vv("%v (test code) shutdown... below prod code should handle", s.me())
		}
	}

	now := time.Now()
	// update lastContactTm, and peer MC
	cktP, known := s.cktall[ack.FromPeerID]
	if known {
		// this can unstall membership changes on leader
		cktP.seen(ack.PeerMC, ack.PeerLogLastIndex, ack.PeerLogLastTerm, ack.Term) // in handle AE ack.
		if unstallMe := s.unstallChangeMC(); unstallMe != nil {
			s.changeMembership(unstallMe)
		}
	}

	// All servers: (Figure 3.1)
	// "If RPC request OR response contains term T > currentTerm,
	// set currentTerm = T, and convert to follower."
	if ack.Term > s.state.CurrentTerm {
		// wipes all follower state anyway.
		s.becomeFollower(ack.Term, ack.PeerMC, SAVE_STATE)
		// ?? s.leader not known yet?
		//vv("%v ack.Term > s.state.CurrentTerm, exit ack AE early", s.me())
		return
	}
	if ack.Term < s.state.CurrentTerm {
		// hmm... not in protocol. RaftConsensus.cc:2322
		// asserts that ack.Term == s.state.CurrentTerm hereabouts,
		// but I'm not convinced it cannot happen, and
		// I don't want to mis-apply a communique from
		// a resurrected peer.
		//vv("%v AE ack handler: dropping too low Term response: '%v'", s.me(), ack)
		return
	}
	if s.role != LEADER {
		return
	}
	// INVAR: we are leader.
	// INVAR: ack.Term == s.state.CurrentTerm

	// check if we have been removed from
	// membership but were just leading while waiting
	// for the newest MC to become committed.
	if s.state != nil && s.state.MC != nil &&
		!s.weAreMemberOfCurrentMC() &&
		s.onLeaderIsCurrentMCcommitted() &&
		// even as shadow we should stay leader if no other...
		s.state.MC.PeerNames.Len() > 0 {

		alwaysPrintf("%v leader sees it is not in current MC which has been committed: so stepping down. current s.state.MC='%v'", s.me(), s.state.MC)

		s.leaderName = ""
		s.leaderID = ""
		s.leaderURL = ""
		s.lastLeaderActiveStepDown = time.Now()
		s.becomeFollower(s.state.CurrentTerm, nil, SAVE_STATE)
		// stay on as shadow now.
		return
		// this will exit the node, shutting us down.
		//return fmt.Errorf("cur MC committed without us, finish up by return from Start")
	}

	// Verify this is ours
	if ack.SuppliedLeader == "" {
		panic(fmt.Sprintf("ack has no SuppliedLeader! ack='%v'", ack.String()))
	}
	if ack.SuppliedLeader != s.PeerID {
		// still update, above
		alwaysPrintf("%v warning: leader ignoring ackAE "+"not from us '%v'. ", s.me(), rpc.AliasDecode(ack.SuppliedLeader))
		return
	}
	// INVAR: ack.Term == CurrentTerm
	// INVAR: we are leader, so we can send
	// additional out AppendEntries requests
	// to catch-up the followers who are behind.

	// update these before returning
	var foll *RaftNodeInfo

	var ok bool
	foll, ok = s.peers[ack.FromPeerID]
	if !ok {
		foll = s.newRaftNodeInfo(ack.FromPeerID, ack.FromPeerName, ack.FromPeerServiceName, ack.FromPeerServiceNameVersion)
		foll.PeerName = ack.FromPeerName
		s.peers[ack.FromPeerID] = foll
	}

	foll.LastHeardAnything = now // in handleAppendEntriesAck
	foll.MC = ack.PeerMC         //.Clone()

	//vv("%v leader set foll('%v').LastHeardAnything = now = '%v'; s.peers=%p len %v", s.me(), foll.PeerName, foll.LastHeardAnything, s.peers, len(s.peers))
	if foll.UnackedPing.AEID == ack.AEID {

		foll.LastFullPing = foll.UnackedPing

		foll.LastFullPong.Sent = foll.UnackedPing.Sent
		foll.LastFullPong.AEID = ack.AEID
		foll.LastFullPong.RecvTm = now
		foll.LastFullPong.Term = ack.Term
		foll.LastFullPong.LogLastIndex = ack.PeerLogLastIndex
		foll.LastFullPong.LogLastTerm = ack.PeerLogLastTerm
		foll.LastFullPong.PeerID = ack.FromPeerID
		foll.LastFullPong.PeerName = ack.FromPeerName
		foll.LastFullPong.PeerServiceName = ack.FromPeerServiceName

		// easy quorum tracking, so we step down if
		// we cannot ping a majority of cluster (section 6.2)
		s.leaderFullPongPQ.add(foll.LastFullPong) // sort on RecvTm

		// the most essential thing: longest common prefix
		// lets us know exactly what they need.
		foll.LargestCommonRaftIndex = ack.LargestCommonRaftIndex

		foll.MinElectionTimeoutDur = ack.MinElectionTimeoutDur
		if ack.MinElectionTimeoutDur == 0 {
			panic(fmt.Sprintf("%v bad peer: MinElectionTimeoutDur == 0?? peer='%v'; stack=\n%v", s.me(), rpc.AliasDecode(ack.FromPeerID), stack()))
		}
	} else {
		//vv("%v arg. ack for older ae; not updating follower! ack='%v'", ack)
	}

	// maintain and update our RaftNodeInfo for each peer.

	// this is the essential thing.
	foll.LargestCommonRaftIndex = ack.LargestCommonRaftIndex

	begIdx, begTerm, leaderLastLogIndex, endTerm := s.getRaftLogSummary()
	_, _, _, _ = begIdx, begTerm, leaderLastLogIndex, endTerm

	if ack.Rejected {
		// okay, try again. how much earlier do they need?
		// LargestCommonRaftIndex tells us, unless
		// it is the unknown -1 value. Otherwise we'll just
		// empty heatbeat to find out.

		// In any case, even on reject, we may have learned
		// follower log length that will let us commit something.
		switch {
		case ack.NeedSnapshotGap:
			//vv("%v ack.NeedSnapshotGap true: ack='%v'", s.name, ack)

			// follower has a gap in their log and needs
			// the most recent snapshot to get caught up.

			// commenting totally breaks Test059_new_node_joins_after_compaction
			s.handleRequestStateSnapshot(nil, ckt, fmt.Sprintf("case ack.NeedSnapshotGap in handleAppendEntriesAck(); ack from '%v'; ack.RejectReason='%v'; ack='%v'", ack.FromPeerName, ack.RejectReason, ack))

		case ack.LargestCommonRaftIndex >= 0: // hmm... always true? or -1 ?
			// we could: follower.MatchIndex = ack.LargestCommonRaftIndex
			// and if it changed release some waiters, too.
			if ack.LargestCommonRaftIndex > foll.MatchIndex {
				foll.MatchIndex = ack.LargestCommonRaftIndex
				s.leaderAdvanceCommitIndex()
			}

			// really this should suffice, rather
			// than all those heuristics below.
			// with just one more trip we can be caught up.
			//vv("%v ae ack sees rejected, calling sendAppendEntriesTo() for '%v'", s.me(), ack.FromPeerName)
			s.sendAppendEntriesTo(ack.FromPeerID, ack.FromPeerName, ack.FromPeerServiceName, ack.FromPeerServiceNameVersion, ack.LargestCommonRaftIndex+1, 0, ack)
			return

			// case: the rest. The following cases,
			// are all pre-LargestCommonRaftIndex,
			// so are like the original Raft updating:
			// heuristics/gossipy/slower b/c more roundtrips.

			// =======================================================
			// ==========  begin 002 works fine without this =========
			// ==========  but might be some optimizations   =========

		case ack.PeerLogLastIndex == 0:
			//vv("%v PeerLogLastIndex = 0, they need everything", s.me())
			fallthrough
		case ack.PeerLogFirstTerm != begTerm:
			//vv("%v they need everything", s.me())
			s.sendAppendEntriesTo(ack.FromPeerID, ack.FromPeerName, ack.FromPeerServiceName, ack.FromPeerServiceNameVersion, 1, 0, ack)
			return
		case ack.ConflictTerm1stIndex > 0:
			panic("we thought this was unused and could be eliminated to simplify log compaction operations")
			try := ack.ConflictTerm1stIndex
			//vv("%v guess maybe re-doing the whole conflict term", s.me())

			tryEntry, err := s.wal.GetEntry(try + 1)
			//if s.wal.RaftLog[try].Term == ack.ConflictTerm &&
			//	s.logIndexInBounds(try+1) {
			if err != nil && tryEntry.Term == ack.ConflictTerm {

				//vv("%v foll has start of the term okay. anchor off that", s.me())

				s.sendAppendEntriesTo(ack.FromPeerID, ack.FromPeerName, ack.FromPeerServiceName, ack.FromPeerServiceNameVersion, try+1, 1, ack)
				return
			} else {
				//vv("%v backup one, seems the whole term is messed up.", s.me())

				if s.logIndexInBounds(try) {
					s.sendAppendEntriesTo(ack.FromPeerID, ack.FromPeerName, ack.FromPeerServiceName, ack.FromPeerServiceNameVersion, try, 1, ack)
					return
				}
			}

			// ==========  _end_ 002 works fine without this =========
			// ==========  but might be some optimizations   =========
			// =======================================================

		default:

			// Note: we can likely omit this fallback, if desired.

			// No other specific info... just try an earlier entry?

			try := ack.SuppliedEntriesIndexBeg - 1
			if try > 0 {
				s.sendAppendEntriesTo(ack.FromPeerID, ack.FromPeerName, ack.FromPeerServiceName, ack.FromPeerServiceNameVersion, try, 0, ack)
				return
			}
		}
	}

	// this is a big difference between rejected and
	// accepted heartbeats: the leader will check
	// for commits if the AE was accepted, in the
	// orignal. So we don't miss anything, Tube
	// _also_ calls it above, in the reject path,
	// using the longest common prefix (equivalent
	// to the match-index) to judge.
	if !ack.Rejected {
		// Update follower's progress

		// advanceCheckForced() allows us
		// us to check on either side of
		// the NoFaultTolDur expiration,
		// and thus avoids an infinite wait
		// for entries that started before
		// the expiration of NoFaultTolDur.

		check := s.advanceCheckForced()
		if ack.PeerLogLastIndex > foll.MatchIndex {
			foll.MatchIndex = ack.PeerLogLastIndex
			check = true
		}
		if check {
			// do any commits possible now that
			// MatchIndex incremented (or noFaultTolDur expired).
			s.leaderAdvanceCommitIndex()

			// on membership change, we may have stepped down.
			if s.role != LEADER {
				return
			}
		}

		// Check if there are more entries to send, we
		// might have gotten some new in the meantime.
		if leaderLastLogIndex > foll.LargestCommonRaftIndex {
			// send them
			//vv("%v leaderLastLogIndex(%v) > foll.LargestCommonRaftIndex(%v), sending additional AE to '%v'", s.me(), leaderLastLogIndex, foll.LargestCommonRaftIndex, foll.PeerName)
			s.sendAppendEntriesTo(foll.PeerID, foll.PeerName, foll.PeerServiceName, foll.PeerServiceNameVersion, foll.LargestCommonRaftIndex+1, 0, ack) // big 4900 entries send 12MB came from here.
			return
		}
	} // end if !rejected
	return
}

// allow transactions that needed full
// membership b/c they were started before the
// cfg.NoFaultTolDur expired to take
// effect once that period expires -- even
// when there is a member still down --
// as long as we have quorum after the
// period.
func (s *TubeNode) advanceCheckForced() bool {
	if s.haveCheckedAdvanceOnceAfterNoFaultExpired {
		return false
	}
	now := time.Now()
	if now.After(s.noFaultTolExpiresTm) {
		// strives for efficiency by only checking
		// once after the s.cfg.NoFaultTolDur expires:
		//vv("%v advanceCheckForced() returning true once!", s.me())
		s.haveCheckedAdvanceOnceAfterNoFaultExpired = true
		return true
	}
	return false
}

func (s *TubeNode) lastLogIndex() int64 {
	if s.wal == nil {
		return -1
	}
	return s.wal.LastLogIndex()
}

func (s *TubeNode) lastLogTerm() (term int64) {
	if s.wal == nil {
		return 0
	}
	return s.wal.LastLogTerm()
}

// advanceCommitIndex:
//
// When the Raft leader server hears back from the
// peer servers and updates their matchIndexes, it knows
// the longest common prefix of all the reporting peers' logs,
// and can mark that index as "committed" when
// a) quorum is reached; and
// b) all future leaders will have that entry.
//
//	and b) means either
//	  1) the entry is from this leader's term (standard Raft rule);
//	  2) all peers have the entry already (not
//	     standard Raft, proposing implementation below
//	     to see if I've understood the idea...)
func (s *TubeNode) leaderAdvanceCommitIndex() {
	//vv("%v top of leaderAdvanceCommitIndex()", s.me())
	//defer func() {
	//	vv("%v end of leaderAdvanceCommitIndex()", s.me())
	//}()
	if s.role != LEADER {
		panic(fmt.Sprintf("%v leaderAdvanceCommitIndex can only ever be called by the leader", s.me()))
		return
	}

	// How entries get committed, a.k.a. the definition of commit.
	// from raft-dev mailing list
	// https://groups.google.com/g/raft-dev/c/MELy20MQmWo/m/Jvq8CjUKAwAJ
	//
	// John Ousterhout writes
	// ous...@cs.stanford.edu
	// Oct 31, 2019, 11:48:18 PM
	// to raft...@googlegroups.com
	//
	// 	An entry is committed in either of two ways:
	//
	// 1. The entry is accepted by a majority of the
	// servers in the cluster *in the same term as the entry*
	// (if the leader crashes before receiving the results
	// of those AppendEntries calls, it's possible that no
	// one in the cluster will know that the entry is
	// committed, but it is indeed committed; any future
	// leader is guaranteed to store that entry, and it
	// will finish propagating it to the rest of the
	// cluster, if needed).
	//
	// 2. An entry in the same log, but with higher index, is committed.
	// [ i.e. a higher index being committed then commits all lower ones].
	//
	// Your error is in your fourth step: "subsequent term leader
	// cannot decide on entries of the previous term." This is
	// not true. A subsequent leader can commit entries in
	// earlier terms; the way it does this is by committing
	// a new entry in the current term, after which rule 2 above applies.
	//
	// -John-

	// My comment: this means the commitIndex must naturally
	// reach the first-thing executed no-op. In other words,
	// all previous commands must be fully (quorum) replicated
	// first, so that the no-op can also commit.

	// Fig 3.1 summary page 13, bottom right corner:
	// If:
	//       a) there exists N such that N > commitIndex; and
	//       b) a majority of matchIndex[i] >= N; and
	//       c) log[N].Term == currentTerm;
	// then: set commitIndex = N

	// (This is like the phase 2b paxos learners hearing
	// that a quorum of acceptors have accepted).

	// Find the smallest match index that is replicated to a majority
	var matchIndexes []int64
	// add our own, since we are not in peers, to support
	// singleton or pair operation.
	matchIndexes = append(matchIndexes, s.lastLogIndex())

	//vv("%v len of s.peers = %v (%v)", s.me(), len(s.peers), peerNamesAsString(s.peers))
	for peerID, info := range s.peers {
		_ = peerID

		// seems important: only count match indexes from
		// currently configured membership
		if s.state.MC != nil {
			_, ok := s.state.MC.PeerNames.Get2(info.PeerName)
			if !ok {
				//vv("%v warning: ignoring match index from peer '%v' (%v) b/c not in MC.PeerNames: '%#v'", s.name, info.PeerName, info.PeerID, s.state.MC.PeerNames)
				continue
			}
		}

		mi := info.MatchIndex
		matchIndexes = append(matchIndexes, mi)
		//vv("%v peer '%v' has MatchIndex = '%v'", s.me(), info.PeerName, mi)
	}

	//i := s.quorum() - 1 // 0-indexed slices need -1
	quor, nofault := s.quorumAndIsNoFaultTolDur()
	i := quor - 1 // 0-indexed slices need -1

	if nofault {
		alwaysPrintf("%v warning! We are still in the nofault period(%v) (for another %v), so requiring all %v nodes (not just %v) to ack their wal writes in order for leader to commit.", s.name, s.cfg.NoFaultTolDur, s.t0.Add(s.cfg.NoFaultTolDur).Sub(time.Now()), quor, s.quorumIgnoreNoFaultTolDur())
	}

	//vv("%v s.quorum() - 1 = i=%v; len(matchIndexes)=%v (%#v)", s.me(), i, len(matchIndexes), matchIndexes)
	if i >= len(matchIndexes) {
		//vv("%v not enough for quorum(%v)... would be out of bounds. i(%v) >= len(matchIndex)=%v", s.me(), i+1, i, len(matchIndexes))
		return
	}

	// sort biggest first.
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] > matchIndexes[j]
	})

	//vv("%v quorumMinSlice = i = %v and sorted big-first matchIndexes='%#v'; setting newCommitIndex = matchIndexes[%v]='%v'", s.me(), i, matchIndexes, i, matchIndexes[i])
	newCommitIndex := matchIndexes[i]

	// especially with NoFaultTolDur, we
	// might get a newCommitIndex of 0 if
	// we have not heard from a peer.
	if newCommitIndex == 0 {
		//vv("%v wait a bit more, as we have not heard from all yet; have possible newCommitIndex == 0", s.me())
		return
	}

	// safety check: if we have not heard from a node
	// (say that crashed) in a shile, the commit index
	// could be 0 (just above) or maybe not advanced
	// as far as it should (especially when doing
	// the all-nodes-must-report-in period of
	// (the first 60 seconds) of NoFaultTolDur.
	if newCommitIndex < s.state.CommitIndex {
		//vv("%v wait a bit more, as we have not heard from all yet - we assume b/c possible newCommitIndex(%v0 < s.state.CommitIndex(%v)", s.me(), newCommitIndex, s.state.CommitIndex)
		return
	}

	if s.state.CommitIndex >= newCommitIndex {
		//vv("%v s.state.CommitIndex(%v) >= newCommitIndex(%v): return early since no new commit possible", s.me(), s.state.CommitIndex, newCommitIndex)
		// is not any higher, no new commit possible
		// under the standard Raft rule.
		return // RaftConsensus.cc:2186
	}
	//if newCommitIndex > int64(len(s.wal.RaftLog)) {
	if newCommitIndex > s.wal.LogicalLen() {
		//vv("%v  out of bounds hmmm: newCommitIndex(%v) > s.wal.LogicalLen(%v)", s.me(), newCommitIndex, s.wal.LogicalLen())
		return
	}
	// INVAR: newCommitIndex > s.state.CommitIndex

	entry, err := s.wal.GetEntry(newCommitIndex)
	if err != nil || entry.Term != s.state.CurrentTerm {

		// "At least one of these entries must also be from the current term to
		// guarantee that no server without them can be elected."
		//   -- RaftConsensus.cc:2192
		// This is what prevents the Fig 3.7 problem.
		return // RaftConsensus.cc:2194
	}
	// INVAR: newCommitIndex > s.state.CommitIndex
	// INVAR: s.wal.RaftLog[newCommitIndex-1].Term == s.state.CurrentTerm
	// Thus it is now safe to advance the CommitIndex.
	// This should be the only place and time the leader
	// updates its CommitIndex.

	s.state.CommitIndex = newCommitIndex // RaftConsensus.cc:2195
	s.state.CommitIndexEntryTerm = entry.Term

	// THIS IS THE COMMIT ON THE LEADER.
	s.commitWhatWeCan(true)

} // end leaderAdvanceCommitIndex()

// s.state.CommitIndex may have been updated, see if there
// is anything to apply.
// Return true if we just did s.saver.save(s.state); so
// caller can avoid an fsync.
func (s *TubeNode) commitWhatWeCan(calledOnLeader bool) (saved bool) {
	//vv("%v commitWhatWeCan; s.state.LastApplied(%v) s.state.CommitIndex(%v); if LastApplied < CommitIndex, we can commit.", s.me(), s.state.LastApplied, s.state.CommitIndex)
	//defer func() {
	//	vv("%v defer commitWhatWeCan; s.state.LastApplied(%v) s.state.CommitIndex(%v)", s.me(), s.state.LastApplied, s.state.CommitIndex)
	//}()

	memberUpdateLeaderShouldStepDown := false
	defer func() {
		// We run this last to try and finish all leader
		// business before stepping down due to member config change.
		if calledOnLeader && memberUpdateLeaderShouldStepDown {

			alwaysPrintf("%v step 6: leader steps down to follower b/c I am not in latest MemberConfig", s.me())
			//vv("%v sees memberUpdateLeaderShouldStepDown in commitWhatWeCan", s.me())
			s.leaderName = ""
			s.leaderID = ""
			s.leaderURL = ""
			s.lastLeaderActiveStepDown = time.Now()
			s.becomeFollower(s.state.CurrentTerm, nil, SAVE_STATE)
		}
	}()

	n := s.wal.LogicalLen()

	// are LastApplied and CommitIndex durable? See just below.
	// Raft calls them volatile state on all servers, but
	// that is not the full story.
	// How do we avoid committing things 2x then?
	// A: Raft does not prevent it. We will save
	// state if it changes.

	// Raft: Last applied volatility should match the state machine.
	// "If the state machine is volatile, lastApplied should
	//  be volatile. If the state machine is persistent,
	//  lastApplied should be just as persistent."

	origLastApplied := s.state.LastApplied
	defer func() {
		if s.state.LastApplied > origLastApplied {
			s.ifLostTicketTellClient(calledOnLeader)
		}
	}()
	for ; s.state.LastApplied < s.state.CommitIndex; s.state.LastApplied++ {

		if s.state.LastApplied >= n {
			panic("shouldn't be possible for CommitIndex to be > n")
			// (this is also a bounds assertion on LastApplied)
		}
		// If we are already compacted away, then for
		// sure we have been applied already, since we
		// wait for LastApplied to reach an index before
		// compacting it. but recovery/after compaction
		// might lack an entry, so avoid an error from
		// the wal about "index too small" by checking
		// against our compaction state:
		if s.state.LastApplied+1 <= s.state.CompactionDiscardedLast.Index {
			continue // already committed, applied, and compacted away.
		}
		do, err := s.wal.GetEntry(s.state.LastApplied + 1)
		if err != nil {
			panic(err)
			break // TODO: not sure if this is correct... but prob do not want to panic(err) just above...
		}
		if do.committed {
			// update: we can hit this? 055 intermit so racy?
			continue
		}
		// record this to remember the Term with the
		// state.LastApplied index.
		// quick sanity check that we are not writing a zero or decreasing term:
		if do.Term < s.state.LastAppliedTerm {
			panic(fmt.Sprintf("terms should be monotone up; do.Term=%v; but s.state.LastAppliedTerm = %v; do='%v'\n state='%v'", do.Term, s.state.LastAppliedTerm, do, s.state)) // saw once, added print of do and state: panic: terms should be monotone up; do.Term=6; but s.state.LastAppliedTerm = 7
		}
		s.state.LastAppliedTerm = do.Term

		do.committed = true
		tkt := do.Ticket
		tkt.Committed = true

		// THIS IS THE APPLY.

		// This is where we apply the command to the
		// application state machine. We read the reads,
		// write the writes, and recognize the special
		// stalling noop0, the leader's first commit.

		//vv("%v applying committed ticket %v at s.state.CommitIndex=%v; s.state.LastApplied=%v", s.me(), tkt.Op, s.state.CommitIndex, s.state.LastApplied)
		switch tkt.Op {
		case MEMBERSHIP_SET_UPDATE, MEMBERSHIP_BOOTSTRAP:
			//vv("%v tkt='%v' Applying new MemberConfig(%v) = %v", s.me(), tkt.TicketID[:4], tkt.Op, tkt.MemberConfig.Short()) // seen 403 only on leader node_0

			// setMC calls
			// adjustCktReplicaForNewMembership,
			// so no need to do it again.
			// in commitWhatWeCan here.
			_, _ = s.setMC(tkt.MC, fmt.Sprintf("commitWhatWeCan on %v", s.name))
			//vv("%v ignoredMemberConfigBecauseSuperceded=%v", s.name, ignoredMemberConfigBecauseSuperceded)

			if tkt.Insp == nil {
				//vv("%v is filling in tkt.Insp in commitWhatWeCan; tkt='%v'", s.me(), tkt.Desc)
				s.addInspectionToTicket(tkt)
			}
			if calledOnLeader {
				// not sure why the call is insufficient, but needed:
				// We made FinishTicket locally idempotent.
				s.FinishTicket(tkt, calledOnLeader) // 1st call
			}

			// do we have any stalled membership change requests?
			if calledOnLeader {
				// any waiting stalled next config change?
				if unstallMe := s.unstallChangeMC(); unstallMe != nil {
					defer s.changeMembership(unstallMe)
				}
			} // end if calledOnLeader

			// end of case MEMBERSHIP_SET_UPDATE/BOOSTRAP

		case NOOP:
			tkt.Applied = true

			// in CommitWhatWeCan
			//vv("%v no-op commited, ticket '%v'", s.me(), tkt.TicketID)
			tkt0 := s.initialNoop0Tkt
			if calledOnLeader && tkt0 != nil {
				if tkt.TicketID == tkt0.TicketID {
					//vv("delete noop0 from WaitingAtLeader tkt.TSN='%v'; tkt.LogIndex=%v", s.initialNoop0Tkt.TSN, s.initialNoop0Tkt.LogIndex)
					s.WaitingAtLeader.del(s.initialNoop0Tkt)
					s.initialNoop0Tkt.Stage += ":initialNoop0_deleted_from_WaitingAtLeader_in_commitWhatWeCan"
					//vv("%v leader initalNoopTkt has commited... free to do 2nd txn", s.me())
					//tkt0.Done.Close() // does it happen elsewhere... FinishTicket below does this.
					if !s.initialNoop0HasCommitted {
						s.initialNoop0HasCommitted = true
						//s.readIndexOptim = s.state.CommitIndex // ?
						s.readIndexOptim = s.state.LastApplied

						// tell tests about it, next func below.
						s.otherNoop0applyActions(tkt0)

						// warning: without the defer, this will recursively call
						// commitWhatYouCan immediately, before
						// the s.state.LastApplied has advanced,
						// which is kind of a problem; plus our
						// first no-op is not on disk; other state not adjusted...
						// So best to leave unless you rethink that flow.
						defer s.resubmitStalledTickets() // only place called.
					}
				}
			}

		case ADD_SHADOW_NON_VOTING:
			s.doAddShadow(tkt)
			tkt.Applied = true

		case REMOVE_SHADOW_NON_VOTING:
			s.doRemoveShadow(tkt)
			tkt.Applied = true

		case USER_DEFINED_FSM_OP:
			s.doApplyUserDefinedOp(tkt)
			tkt.Applied = true

		case WRITE:
			s.doWrite(tkt)
			tkt.Applied = true

		case CAS:
			s.doCAS(tkt)
			tkt.Applied = true

		case MAKE_TABLE:
			s.doMakeTable(tkt)
			tkt.Applied = true

		case DELETE_TABLE:
			s.doDeleteTable(tkt)
			tkt.Applied = true

		case RENAME_TABLE:
			s.doRenameTable(tkt)
			tkt.Applied = true

		case DELETE_KEY:
			s.doDeleteKey(tkt)
			tkt.Applied = true

		case SHOW_KEYS:
			s.doShowKeys(tkt)
			tkt.Applied = true

		case READ:
			s.doReadKey(tkt)
			tkt.Applied = true

		case READ_KEYRANGE:
			s.doReadKeyRange(tkt)
			tkt.Applied = true

		case READ_PREFIX_RANGE:
			s.doReadPrefixRange(tkt)
			tkt.Applied = true

			//vv("%v applying read of '%v', got ok=%v, val='%v'", s.me(), tkt.Key, ok, string(tkt.Val))
		case SESS_NEW:
			s.applyNewSess(tkt, calledOnLeader)
		case SESS_END:
			s.applyEndSess(tkt, calledOnLeader)

		default:
			panic(fmt.Sprintf("what to do with tkt.Op '%v'?", tkt.Op))
		}
		tkt.AsOfLogIndex = s.state.LastApplied

		if tkt.SessionID != "" {
			// =========================================
			// session implementation (for client linearizability)
			// i.e. exactly once.

			// should we not be rejecting BEFORE we apply?
			// Yes, and we do now: see leaderDoneEarlyOnSessionStuff().
			ste, ok := s.state.SessTable[tkt.SessionID]
			if !ok {
				// happens if cluster bounces but member clients stay up and
				// use stale sessions?? don't freak out.
				//panicf("how should be handled? should we not already have replicated and thus established the session? unknown tkt.SessionID for tkt=%v", tkt)

				// only one reply; from leader. replicas can just drop the session error.
				if calledOnLeader {
					tkt.Err = fmt.Errorf("unknown SessionID on tkt '%v'. Must call CreateNewSession first.", tkt)
					tkt.Stage += ":err_unknown_SessionID_in_commitWhatWeCan"
					s.respondToClientTicketApplied(tkt)
				}
				continue
			}
			if tkt.Op != SESS_END {
				// cache the Ticket for dedup; extend session.

				ste.ticketID2tkt[tkt.TicketID] = tkt
				ste.Serial2Ticket.Set(tkt.SessionSerial, tkt)
				if tkt.SessionSerial > ste.MaxAppliedSerial {
					//vv("%v: updating ste.MaxAppliedSerial to higher tkt.SessionSerial: %v -> %v; on tkt.SessionID = '%v'", s.name, ste.MaxAppliedSerial, tkt.SessionSerial, tkt.SessionID)
					ste.MaxAppliedSerial = tkt.SessionSerial

					// leader calls in the pre-replication checks,
					// but followers cleanup does not happen unless
					// we do it on them too, like here.
					s.cleanupAcked(ste, tkt.MinSessSerialWaiting)
				}

				if tkt.Op != SESS_NEW {
					// by using do.Tm below, we gain that
					// all replicas extend by the same amount each time:
					// Here 'do' is the RaftLogEntry for this most recent
					// applied log entry; do.Tm is when the leader created
					// when it in replicateTicket().
					ste.SessionReplicatedEndxTm = s.refreshSession(do.Tm, ste)
				}
			}
			// end client session implementation
			// =========================================
		}
		if tkt.FromID == s.PeerID {
			//vv("%v commitWhatWeCan (from self), calling FinishTicket for tkt4=%v", s.me(), tkt.TicketID[:4])
			s.FinishTicket(tkt, calledOnLeader) // deleting from WAF while waiting for reply! 2nd call on leader
		} else {
			if calledOnLeader {
				//vv("%v commitWhatWeCan, calledOnLeader calling respondToClientTicketApplied for tkt4=%v ; tkt.FromName='%v'", s.me(), tkt.TicketID[:4], tkt.FromName)
				// this does FinishTicket
				s.respondToClientTicketApplied(tkt)
			}
		}
	} // end for ; s.state.LastApplied < s.state.CommitIndex; s.state.LastApplied++

	// in commitWhatWeCan here
	if s.state != nil &&
		s.state.MC != nil &&
		s.state.MC.CommitIndex < s.state.CommitIndex {

		s.state.MC.CommitIndex = s.state.CommitIndex
		s.state.MC.CommitIndexEntryTerm = s.state.CommitIndexEntryTerm
	}
	if s.state.LastApplied > origLastApplied {
		// need to persist LastApplied (and any kvstore changes).
		s.saver.save(s.state)
		saved = true
	}
	return
} // end of commitWhatWeCan()

// BEGIN notes on commitWhatWeCan
//
// Notes on this last LastApplied sync, why it is safe
// even if stale (we crash right here).
//
// We don't want to skip persisting the LastApplied index
// in a separate fsync now, as it is a performance
// optimization. But it is only an optimization.
// LastApplied represents the
// last know good applied transaction. What
// if we crashed right here -- a quorum has committed
// but we would not have updated our on disk that
// we actually applied it? We have already applied it,
// but we don't "know" that we have applied it on recovery.
// On recovery, we don't want to apply it a second
// time.
//
// Insight: with the blake3 Merkle tree, the
// application has become idempotent. We are just
// bringing the "materialized view" of the
// filesystem into line with the plan corresponding
// to the most recently committed root hash.
// Recovery just has to re-run the rsync operation locally
// and confirm that we have the filesystem looking
// like the last committed plan and its root hash.
// We can then accurately conclude that the
// plan has been applied, and if was not, that we
// have just applied it with the rsync.
//
// https://claude.ai/chat/b10f9df1-83e3-4e1f-9af7-7502ed28cb87
//
// Summary: the Raft WAL tells us the root-hash
// to root-hash transitions to expect. On recovery,
// we have to see if any tail of these are on disk
// already or not. The blocks have been committed
// as a part of consensus (and the plan to assemble
// blocks to obtain the new root hash as well) is
// saved during consensus. So we know the content
// blocks, their hashes, and the plan to assemble them
// to get a new root hash are all already safely
// on disk. If the materialized view in the actual
// filesystem does not match, we must apply rsync
// to update it from the local kv store which
// has the (hash:block) and (plan:hashes)
// stored. Then we can set the LastApplied to
// match the applied commit. We plan to
// use SeaweedFS (Haystack design) for both
// the hash:block and plan-for-a-root-hash:hashes storage.
// Recovery is then: if the last-applied < last-commited
// iterate through each commit and run its rsync.
// Which it turns out is exactly what just did above,
// in FinishTicket(), thus completing the recovery.
// If we are not maintaining a "materialized view",
// as in just being a backup system (e.g. like
// a bare git repo without any checkout) then there
// isn't anything to do, and FinishTicket is
// just telling the client.
// Conclusion: FinishTicket either does the rsync
// for store to user-viewable filesystem
// (a "restore" operation in backup terms), or
// just needs to finish telling the client about
// completed/committed transactions.
//
// What does it do for simply maintaining
// a small amount of configuration state, our
// first implementation goal? I don't thinkg
// there is a separate "apply command" step
// in this case. The last committed state
// on disk is the state we want to serve.
// The apply is a no-op, I think, and FinishTicket
// just needs to respond to any outstanding
// client requests. I think it wants to only
// report the last commit to reads however. So we
// will need to wait until the end of the
// above for loop before responding to reads?
//
// Well no, reads are ordered with respect
// to the Raft log, so they only want to
// see the state that was committed when
// they were committed too. So actually we
// don't want to have them wait; but we could
// include a hint that there have been
// subsequent commits, and tell them the
// latest anyway to allow them to skip
// doing a whole other round trip, as an
// optimization for practical systems --
// but not use that "latest" during linearizability
// testing.

// Claude.AI:
// Good question about Raft's handling of command
// execution during server restarts! This is indeed
// a subtle point in the protocol.
//
// Raft does not inherently prevent a state machine
// command from being executed twice when servers restart.
// You're correct that the lastApplied and commitIndex
// are part of the volatile state, which means they are
// not persisted to stable storage and are reinitialized
// when a server restarts.
//
// Here's what happens during a restart:
// 1. The server loads its persistent state (log entries, currentTerm, votedFor)
// 2. The lastApplied index is reset (typically to 0)
// 3. When the server restarts, it will reapply commands
// from its log starting from index 1 up to the new commitIndex
//
// This means that commands that were already applied
// before the crash will be reapplied to the state machine after restart.
//
// To handle this problem properly, you have a few options:
//
// 1. **Make your state machine operations idempotent**:
// Design your operations so applying them multiple times
// has the same effect as applying them once. For example,
// "set x to 5" rather than "add 1 to x".
//
// 2. **Persist the lastApplied index**: Some Raft
// implementations extend the protocol to periodically
// persist the lastApplied index to stable storage to
// avoid reapplying commands.
//
// 3. **Take snapshots with metadata**: When taking a
// snapshot of the state machine, record the lastApplied
// index with it. After a restart, initialize lastApplied
// from the snapshot rather than 0.
//
// 4. **Command deduplication**: Have clients assign
// unique IDs to commands and have the state machine
// track which commands have already been processed.
//
// The original Raft paper acknowledges this issue but
// doesn't prescribe a specific solution, leaving it to
// the implementation to handle appropriately based on
// the specific requirements of the system.
//
// END notes on commitWhatWeCan implementation choices.

// called after membership changes are applied,
// and after AEack received.
func (s *TubeNode) unstallChangeMC() *Ticket {
	if s.role != LEADER {
		return nil
	}
	if len(s.stalledMembershipConfigChangeTkt) > 0 {
		tkt := s.stalledMembershipConfigChangeTkt[0]
		s.stalledMembershipConfigChangeTkt = s.stalledMembershipConfigChangeTkt[1:]

		//vv("%v will replicate stalled config change now: '%v'", s.me(), tkt.Short())
		return tkt // one at a time.
	}
	return nil
}

func (s *TubeNode) otherNoop0applyActions(tkt0 *Ticket) {
	// election_test.go	040 confirms no-op0 was committed
	if s.isRegularTest() {
		select {
		case panicAtCap(s.cfg.testCluster.LeaderNoop0committedCh) <- tkt0.LeaderName:
		default:
		}
	}

}

// called by leaderServedLocalRead();
// commitWhatWeCan(); and now by
// leaderDoneEarlyOnSessionStuff().
func (s *TubeNode) respondToClientTicketApplied(tkt *Ticket) {
	//tkt.StateSnapshot = nil // do not replicate again!

	calledOnLeader := s.role == LEADER
	// whelp. in desparate circumstances, a
	// forced add/remove with tubeadd -f or tuberm -f
	// we will need to respond to our caller
	// and not care if we are leader or not. So comment out:
	//if !calledOnLeader {
	//	panic("should only be called on leader")
	//}
	s.FinishTicket(tkt, calledOnLeader)

	// was it ours and not forwarded to us?
	if tkt.FromID == s.PeerID {
		//vv("%v ours; finished now.", s.name)
		tkt.Stage += ":respondToClientTicketApplies_FromID_eq_PeerID_so_fin."
		return
	}
	if tkt.Op == MEMBERSHIP_BOOTSTRAP {
		// artificial and probably from the
		// last (now dead) leader. Don't try
		// to contact them.
		//vv("%v not replying to any client for MEMBERSHIP_BOOTSTRAP", s.me())
		tkt.Stage += ":notice_MEMBERSHIP_BOOTSTRAP_and_done_early"
		return
	}
	// ignore NOOP (including noop0s)
	if tkt.Op == NOOP {
		//vv("%v not replying to any client for NOOP", s.me())
		tkt.Stage += ":notice_NOOP_and_done_early"
		return
	}

	// was client already in peer cluster? they don't need
	// a second notification, when it applied they went on...
	// but we might have had a membership change, so tell
	// them anyway to be sure.
	tkt.Stage += ":respondToClientTicketApplied_FromID_not_eq_PeerID_not_fin_yet"

	frag := s.newFrag()
	frag.FragOp = LeaderToClientTicketAppliedMsg
	frag.FragSubject = "LeaderToClientTicketApplied"
	bts, err := tkt.MarshalMsg(nil)
	panicOn(err)
	frag.Payload = bts
	if len(bts) > rpc.UserMaxPayload { // 1_200_000
		panicf("why so big? will not get through... len(bts) is %v > rpc.UserMaxPayload(%v) footprint = %v", len(bts), rpc.UserMaxPayload, tkt.Footprint())
	}

	frag.SetUserArg("leader", s.leaderID)
	frag.SetUserArg("leaderName", s.leaderName)
	frag.SetUserArg("leaderURL", s.leaderURL)

	//vv("%v: replying to client, so use ckt not cktReplica.", s.name)
	cktP, ok := s.cktall[tkt.FromID]
	if !ok {
		if tkt.FromName == s.name {
			//vv("%v respondToClientTicketApplied() bail out: looks like tkt is from an old iteration of ourselves; tkt.Term = %v; cur term = %v; not retying to reply.", s.name, tkt.Term, s.state.CurrentTerm)
			return
		}
		// This will occur always as we go through the log
		// and see the commits by the old leader. The only
		// question is why are we trying to reply to the cli
		// for these old tickets? Don't freak out.

		// did we get a replacement ckt maybe? try to recover...
		cktP, ok = s.cktAllByName[tkt.FromName]
		if !ok {
			// still nope

			alwaysPrintf("%v in respondToClientTicketApplied: loss of ticket... don't know how to contact tkt.FromID='%v' (tkt.FromName='%v') (!= s.PeerID='%v'/'%v') to fwd tkt in respondToClientTicketApplied. Assuming they died. tkt=%v; s.cktall='%v'", s.me(), tkt.FromID, tkt.FromName, alias(s.PeerID), s.PeerID, tkt, s.cktall2string())

			tkt.Stage += ":__cannot_find_ckt_for_client_lose_ticket"
			// TODO: add to dead letter?
			//panic("loss of ticket...")
			return
		}
	}
	ckt := cktP.ckt
	tkt.Stage += "__found_ckt"

	if ckt == nil || ckt.LpbFrom == nil {
		tkt.Stage += ":__cannot_find_ckt_for_client_lose_ticket2"
		// TODO: add to dead letter?
		return
	}
	err = s.SendOneWay(ckt, frag, -1, 0)
	_ = err // don't panic on halting.
	if err != nil {
		alwaysPrintf("%v non nil error '%v' on Redirect to '%v'", s.me(), err, ckt.RemotePeerID)
	}
	//vv("%v: respondToClientTicketApplied did SendOneWay to '%v'", s.me(), ckt.RemotePeerName)
}

func (s *TubeNode) tellClientsImNotLeader() {
	//vv("%v tellClientsImNotLeader top", s.me())

	if s.role == LEADER {
		panic("should only be called by non-LEADER")
	}

	for _, tkt := range s.WaitingAtLeader.all() {
		tkt.Stage += ":tellClientsImNotLeader_"

		frag := s.newFrag()
		frag.FragOp = TellClientSteppedDown
		frag.FragSubject = "TellClientSteppedDown"
		bts, err := tkt.MarshalMsg(nil)
		panicOn(err)
		frag.Payload = bts

		frag.SetUserArg("leader", s.leaderID)
		frag.SetUserArg("leaderName", s.leaderName)
		frag.SetUserArg("leaderURL", s.leaderURL)

		// in tellClientsImNotLeader, so definitely cktall not cktReplica.
		cktP, ok := s.cktall[tkt.FromID]
		if !ok {
			//vv("%v don't know how to contact '%v' to fwd tkt in respondToClientTicketApplied. Assuming they died.", s.me(), tkt.FromID)
			tkt.Stage += "_ckt_not_ok_for_tkt_FromID"
			continue
		}
		ckt := cktP.ckt
		tkt.Stage += "_SendOneWay"

		err = s.SendOneWay(ckt, frag, -1, 0)
		_ = err // don't panic on halting.
		if err != nil {
			alwaysPrintf("%v non nil error '%v' on TellClientSteppedDown to '%v'", s.me(), err, ckt.RemotePeerID)
		}
	}
	//vv("%v wait to clear WaitingAtLeader until we have ack from clients.", s.me())
	// TODO: delete when we do get acks?
	//s.WaitingAtLeader = make(map[string]*Ticket)
}

func (s *TubeNode) logIndexInBounds(idx int64) bool {
	n := s.wal.LogicalLen() // int64(len(s.wal.RaftLog))
	if n == 0 {
		return false
	}
	return 1 <= idx && idx <= n
}

func (a *AppendEntriesAck) String() string {
	if a == nil {
		return "(nil AppendEntriesAck)" // in AE panic probably
	}
	return fmt.Sprintf(`AppendEntriesAck{
                ClusterID: %v,
               FromPeerID: %v,
             FromPeerName: %v,
      FromPeerServiceName: %v,
           SuppliedLeader: %v,
       SuppliedLeaderName: %v,
                       --- summary ---
                     Term: %v,
                 Rejected: %v,
             RejectReason: "%v",
         LogsMatchExactly: %v, 
                 --- conflict identified ---
      LargestCommonRaftIndex: %v, (longest common prefix)
             ConflictTerm: %v,
     ConflictTerm1stIndex: %v,
          NeedSnapshotGap: %v,
                   --- peer log info ---
  PeerLogCompactIndex: %v,
   PeerLogCompactTerm: %v,
        PeerLogFirstIndex: %v,
         PeerLogFirstTerm: %v,
         PeerLogLastIndex: %v,
          PeerLogLastTerm: %v,
PeerCompactionDiscardedLastIndex: %v,
 PeerCompactionDiscardedLastTerm: %v,
          PeerLogTermsRLE: %v,
        --- leader supplied its own log info ---
   SuppliedLeaderTermsRLE: %v,
 SuppliedCompactIndex: %v,
  SuppliedCompactTerm: %v,
     SuppliedPrevLogIndex: %v,
      SuppliedPrevLogTerm: %v,
  SuppliedEntriesIndexBeg: %v,
  SuppliedEntriesIndexEnd: %v,
SuppliedLeaderCommitIndex: %v,
SuppliedLeaderCommitIndexEntryTerm: %v,
}
`, rpc.AliasDecode(a.ClusterID), a.FromPeerID, a.FromPeerName, a.FromPeerServiceName, rpc.AliasDecode(a.SuppliedLeader), a.SuppliedLeaderName, a.Term, a.Rejected, a.RejectReason, a.LogsMatchExactly, a.LargestCommonRaftIndex, a.ConflictTerm, a.ConflictTerm1stIndex, a.NeedSnapshotGap, a.PeerLogCompactIndex, a.PeerLogCompactTerm, a.PeerLogFirstIndex, a.PeerLogFirstTerm, a.PeerLogLastIndex, a.PeerLogLastTerm, a.PeerCompactionDiscardedLastIndex, a.PeerCompactionDiscardedLastTerm, a.PeerLogTermsRLE, a.SuppliedLeaderTermsRLE, a.SuppliedCompactIndex, a.SuppliedCompactTerm, a.SuppliedPrevLogIndex, a.SuppliedPrevLogTerm, a.SuppliedEntriesIndexBeg, a.SuppliedEntriesIndexEnd, a.SuppliedLeaderCommitIndex, a.SuppliedLeaderCommitIndexEntryTerm)
}

func (a *RaftLogEntry) Equal(b *RaftLogEntry) bool {
	return a.Term == b.Term && a.Index == b.Index
}

func (s *TubeNode) dispatchAwaitingLeaderTickets() {
	//vv("%v: top dispatchAwaitingLeaderTickets(); len(s.ticketsAwaitingLeader)=%v", s.name, len(s.ticketsAwaitingLeader))

	// any tickets waiting for a leader?
	for id, tkt := range sorted(s.ticketsAwaitingLeader) {
		delete(s.ticketsAwaitingLeader, id)
		if s.role == LEADER {
			//vv("%v dispatchAwaitingLeaderTickets: leader sees ticket waiting for leader, calling replicateTicket: '%v'", s.me(), tkt.TicketID)

			// we might have stashed this as a follower,
			// then become leader. the redirectToLeader()
			// method stashes them when it doesn't know a leader.
			//s.replicateTicket(tkt)
			s.commandSpecificLocalActionsThenReplicateTicket(tkt, "dispatchAwaitingLeaderTickets")
		} else {
			//vv("%v dispatchAwaitingLeaderTickets: follower sees ticket waiting for leader, calling redirectToLeader: '%v'", s.me(), tkt.TicketID)
			s.redirectToLeader(tkt)
		}
	}
}
func (s *TubeNode) answerToQuestionTicket(answer, question *Ticket) {

	if question.Insp == nil {
		//vv("%v using answer.Insp", s.me())
		question.Insp = answer.Insp
	}

	switch question.Op {
	case SESS_NEW:
		// good, replaces nil with filled in session.
		//vv("%v answerToQuestionTicket is setting question.NewSessReply(%v) = answer.NewSessReq(%v)", s.name, question.NewSessReply, answer.NewSessReq)
		question.NewSessReply = answer.NewSessReq

	case MEMBERSHIP_SET_UPDATE, MEMBERSHIP_BOOTSTRAP,
		ADD_SHADOW_NON_VOTING, REMOVE_SHADOW_NON_VOTING,
		USER_DEFINED_FSM_OP:
		// below now, to be universal
		// question.MC = answer.MC

	case CAS:
		question.CASwapped = answer.CASwapped
		question.CASRejectedBecauseCurVal = answer.CASRejectedBecauseCurVal

	case READ_KEYRANGE, READ_PREFIX_RANGE, SHOW_KEYS:
		question.KeyValRangeScan = answer.KeyValRangeScan
	}
	if question.Op != WRITE || answer.Err != nil {
		// otherwise client code cannot read what we got!
		// On leasing error the Val has the lease holder
		// which is useful for simulating elections.
		question.Val = answer.Val
	}
	question.MC = answer.MC
	question.answer = answer
	question.DupDetected = answer.DupDetected
	question.LogIndex = answer.LogIndex
	question.Term = answer.Term
	question.LeaseRequestDur = answer.LeaseRequestDur
	question.Leasor = answer.Leasor
	question.LeaseEpoch = answer.LeaseEpoch
	question.LeaseAutoDel = answer.LeaseAutoDel
	question.VersionRead = answer.VersionRead
	question.LeaseWriteRaftLogIndex = answer.LeaseWriteRaftLogIndex
	question.LeaseUntilTm = answer.LeaseUntilTm
	question.Vtype = answer.Vtype
	question.RaftLogEntryTm = answer.RaftLogEntryTm
	question.HighestSerialSeenFromClient = answer.HighestSerialSeenFromClient

	question.Err = answer.Err
	//question.StateSnapshot = answer.StateSnapshot

	question.AsOfLogIndex = answer.AsOfLogIndex
	question.LeaderLocalReadGoodUntil = answer.LeaderLocalReadGoodUntil
	question.LeaderLocalReadAtTm = answer.LeaderLocalReadAtTm
	question.LeaderLocalReadHLC = answer.LeaderLocalReadHLC

	question.Stage += ":answerToQuestionTicket_append_answer" + answer.Stage
}

// pre-votes are volatile and need no disk state saving.
// In our enhancement, responders are stateless too,
// with respect to the pre-vote. This avoids livelock
// in split-vote scenarios in 9 node clusters, cf 050 safety_test.
//
// Summary of new approach: cut the leader election timeout
// in half. When the first half expires, we are here at
// beginPreVote(). We run a stateless pre-vote.
// If that succeeds, wait for another half-election timeout
// before beginElection(), allowing that to be terminated
// by AE (leader seen). The latency is the same, but split
// pre-votes (contended by many) are gracefully handled
// by the standard Raft random election timeout mechanism.
// It would require two nearly identical timeouts in a row
// from two different nodes to delay such an election,
// a very rare case.
// The livelock on pre-voting we saw is hopefully avoided.
func (s *TubeNode) beginPreVote() {

	if s.amShadowReplica() {
		//vv("%v in beginPreVote, amShadowReplica ture: do not start a pre-vote since I am a shadow replica.", s.me())
		// we use the election timer to maintain
		// connectivity to the cluster, but the <-s.electionTimeoutCh
		// handles resets so we don't need do do that again here.
		return
	}
	if s.state != nil && s.state.MC != nil {
		_, weOK := s.state.MC.PeerNames.Get2(s.name)
		if !weOK {
			//vv("%v in beginPreVote, mongo ignore: do not start a pre-vote since I am not in MC", s.me())
			return
		}
	}

	//vv("%v \n-------->>>    begin Pre Vote() s.countElections = %v <<<--------", s.me(), s.countElections)

	preVoteTerm := s.state.CurrentTerm + 1

	if s.role == CANDIDATE {
		// Was a candidate in a previous term's election that likely failed.
		// Step down to follower before starting a new pre-vote round.
		// The pre-vote is for s.state.CurrentTerm + 1.
		// If this node was CANDIDATE in s.state.CurrentTerm, that's fine.
		// No need to change s.state.CurrentTerm here.
		//vv("%v was CANDIDATE, becoming FOLLOWER before starting pre-vote for term %v", s.me(), preVoteTerm)
		s.role = FOLLOWER
		// Do NOT reset VotedFor here as that pertains to
		// s.state.CurrentTerm's actual election.
	}

	now := time.Now()
	phase1going := s.preVoteTerm > 0 && now.Before(s.preVotePhase1Deadline)
	phase2going := s.preVoteOkLeaderElecTerm > 0 && now.Before(s.preVoteOkLeaderElecDeadline)
	noPhases := !phase1going && !phase2going

	newerPreTerm := // higher term always gets through and aborts current elec
		preVoteTerm > s.preVoteTerm &&
			preVoteTerm > s.preVoteOkLeaderElecTerm

	if noPhases || newerPreTerm {
		// let it through to start a new pre-vote.
		// the settings below will stop any extant phase 1/2.
		//s.ay("%v noPhases=%v || newerPreTerm=%v", s.me(), noPhases, newerPreTerm)
	} else {
		// phase 1 or phase 2 already going at same or higher term.
		if phase1going {
			//s.ay("%v aborting beginPreVote: pre-vote 1st half already going; s.preVoteTerm = '%v'", s.me(), s.preVoteTerm)
		} else {
			//s.ay("%v aborting beginPreVote: random dur 2nd half going; s.preVoteOkLeaderElecDeadline = '%v'", s.me(), nice(s.preVoteOkLeaderElecDeadline))
		}
		return
	}
	//vv("%v beginPreVote; new preVoteTerm=%v; about to nil out s.leaderSendsHeartbeatsCh", s.me(), preVoteTerm)
	s.leaderSendsHeartbeatsCh = nil
	//no: s.state.CurrentTerm++, do not disrupt existing cluster
	//no: s.role = CANDIDATE, stay FOLLOWER

	dur := s.resetElectionTimeout("in beginPrevote, start phase 1")

	// election phase 1, try to get pre-vote
	s.preVoteTerm = preVoteTerm
	s.preVotePhase1Began = now
	s.preVotePhase1Deadline = now.Add(dur)

	// set in phase 2, after have pre-vote, wait one random election dur timeout.
	s.preVoteOkLeaderElecTimeoutCh = nil
	s.preVoteOkLeaderElecDeadline = time.Time{}
	s.preVoteOkLeaderElecTerm = 0

	// per section 4.2.2, only vote for self if we are in current MC.
	// But we still need to hold elections for liveness.
	weAreInMC := s.weAreMemberOfCurrentMC()
	if weAreInMC {
		s.preVotes = map[string]bool{s.PeerID: true}
		s.yesPreVotes = 1
		s.noPreVotes = 0
	} else {
		s.preVotes = map[string]bool{}
		s.yesPreVotes = 0
		s.noPreVotes = 0
	}

	// send RequestVote to all with IsPreVote true.
	rv := &RequestVote{
		ClusterID:           s.ClusterID,
		FromPeerID:          s.PeerID,
		FromPeerName:        s.name,
		FromPeerServiceName: s.PeerServiceName,
		CandidatesTerm:      preVoteTerm, // s.state.CurrentTerm + 1, set above.
		CandidateID:         s.PeerID,
		LastLogIndex:        s.lastLogIndex(),
		LastLogTerm:         s.lastLogTerm(),
		IsPreVote:           true,
		MC:                  s.state.MC,
		SenderHLC:           s.hlc.CreateSendOrLocalEvent(),
	}

	// we only vote for ourselves if we are in current MC
	if weAreInMC {
		// are we a singleton and so done?
		if s.clusterSize() <= 1 {
			//vv("%v we are singleton, skipping request votes", s.me())
			selfVote := &Vote{
				ClusterID:           s.ClusterID,
				FromPeerID:          s.PeerID,
				FromPeerName:        s.name,
				FromPeerServiceName: s.PeerServiceName,
				FromPeerCurrentTerm: s.state.CurrentTerm,
				FromPeerCurrentLLI:  s.lastLogIndex(),
				FromPeerCurrentLLT:  s.lastLogTerm(),
				CandidateID:         s.PeerID,
				CandidatesTerm:      preVoteTerm,
				VoteGranted:         true,
				IsPreVote:           true,
				MC:                  s.state.MC,
				SenderHLC:           s.hlc.CreateSendOrLocalEvent(),
			}
			if selfVote.MC == nil {
				panic("fix this")
			}
			s.tallyPreVote(selfVote)
			return
		}
	}

	rvFrag := s.newFrag()
	bts, err := rv.MarshalMsg(nil)
	panicOn(err)
	rvFrag.Payload = bts
	rvFrag.FragOp = RequestPreVoteMsg
	rvFrag.FragSubject = "RequestPreVote"

	// recycle frag only after last send.
	// Otherwise the circuit will recycle/zero out our frag!
	// In beginPreVote() here, so use cktReplica not cktall.
	left := len(s.cktReplica) - 1
	var delme []*cktPlus
	for peer, cktP := range sorted(s.cktReplica) {
		_ = peer
		//vv("%v requesting pre-vote from %v; cktP='%v'", s.me(), cktP.PeerName, cktP)
		ckt := cktP.ckt
		err = s.SendOneWay(ckt, rvFrag, -1, left)
		_ = err // don't panic on halting.
		if err != nil {
			alwaysPrintf("%v non nil error '%v' on Vote to '%v'", s.me(), err, rpc.AliasDecode(ckt.RemotePeerID))
			if refused(err) {
				//vv("restart the connection rather that loop on a bad one...")
				delme = append(delme, cktP)
			}
		}
		left--
	}
	if len(delme) > 0 {
		for _, cktP := range delme {
			s.deleteFromCktAll(cktP)
			delete(s.cktReplica, cktP.PeerID)
		}
		s.adjustCktReplicaForNewMembership()
	}
	// end beginPreVote
}

// remote peer has started an election.
func (s *TubeNode) handleRequestVote(reqVote *RequestVote, ckt0 *rpc.Circuit) {

	//vv("%v \n-------->>>    handle Request Vote( IsPreVote = %v )  <<<--------\n reqVote = %v\n votedStatus='%v'", s.me(), reqVote.IsPreVote, reqVote, s.state.votedStatus())

	s.hlc.ReceiveMessageWithHLC(reqVote.SenderHLC)

	// should NEVER be IsPreVote here, as we kept them
	// totally separate from regular votes to preserve
	// the ability to turn off the pre-vote if we want;
	// so handleRequestPreVote should be getting them.
	if reqVote.IsPreVote {
		panic("all pre-votes should be going to handleRequestPreVote()")
	}

	// adopt their MC? no! we must wait for ae from leader, else chaos.

	vote := &Vote{
		ClusterID:           s.ClusterID,
		FromPeerID:          s.PeerID,
		FromPeerName:        s.name,
		FromPeerServiceName: s.PeerServiceName,
		FromPeerCurrentTerm: s.state.CurrentTerm,
		FromPeerCurrentLLI:  s.lastLogIndex(),
		FromPeerCurrentLLT:  s.lastLogTerm(),

		CandidateID: reqVote.FromPeerID, // allow double check
		// leave false unless below explicitly granted.
		//VoteGranted: false,

		CandidatesTerm: reqVote.CandidatesTerm,

		// why make them wait another round trip
		// for the most important info, send it!
		PeerLogTermsRLE: s.wal.getTermsRLE(),
		MC:              s.state.MC,
		SenderHLC:       s.hlc.CreateSendOrLocalEvent(),
	}

	// save on fsyncs
	needStateSave := true
	// short-circuit once decision made.
	done := false
	denyBecauseHaveStableLeader := false
	_ = denyBecauseHaveStableLeader

	if s.state.MC == nil {
		// without an MC, we should not be voting at all.
		// We either need a manually configured initial
		// bootstrap config that is manually set on all
		// nodes at start time, or we need an MC from
		// a prior leader.
		vote.Reason = fmt.Sprintf("%v we have nil MC, so declining to vote in election; reqVote '%v', leave VoteGranted: false", s.me(), reqVote)
		done = true
	}

	if !done {
		// reply false if term < currentTerm, section 3.3
		if reqVote.CandidatesTerm < s.state.CurrentTerm {
			//leave VoteGranted: false
			//vv("%v reqVote term(%v) too small, < %v current, deny vote. nreqVote =\n%v", s.me(), reqVote.Term, s.state.CurrentTerm, reqVote)
			done = true
			needStateSave = false
			vote.Reason = fmt.Sprintf("%v term too small in reqVote '%v', leave VoteGranted: false", s.me(), reqVote)
		} else {
			// INVAR: reqVote.Term >= s.state.CurrentTerm

			if recentLeader, denyTm := s.haveStickyLeader(reqVote); recentLeader {
				// in handleRequestVote here.
				//vv("%v handleRequestVote: sticky leader optimization being used", s.me())
				done = true
				denyBecauseHaveStableLeader = true
				vote.VoteGranted = false
				// better not to mess with these, me thinks. seems okay though.
				// might be important to prevent voting in
				// same term 2 different candidates that use the same term?
				// leave out for now; might be confusing to have higher
				// HaveVotedTerm?
				//s.state.HaveVoted = true
				//s.state.HaveVotedTerm = reqVote.CandidatesTerm
				needStateSave = true
				vote.Reason = fmt.Sprintf("%v deny vote to '%v' because have sticky-leader (%v). AE sets: s.lastLegitAppendEntries(%v).After(denyAfterIfLeaderSeen(%v)); reqVote='%v'", s.me(), reqVote.FromPeerID, rpc.AliasDecode(s.leaderID), s.lastLegitAppendEntries, denyTm, reqVote)
				//vv("%v handleRequestVote: has sticky leader: '%v'; on reqVote='%v'", s.me(), vote.Reason, reqVote)

			} else {
				//vv("%v handleRequestVote: no sticky leader", s.me())
				// still INVAR: reqVote.Term >= s.state.CurrentTerm

				if reqVote.CandidatesTerm > s.state.CurrentTerm {
					//vv("%v in handleRequestVote, will become follower as reqVote.Term(%v) > current term(%v)", s.me(), reqVote.Term, s.state.CurrentTerm)
					// avoid saving state 2x by using false
					s.becomeFollower(reqVote.CandidatesTerm, reqVote.MC, SKIP_SAVE)
					// save state below
					needStateSave = true
				} else {
					// INVAR: reqVote.Term == s.state.CurrentTerm
					// if we we are a candidate, we must reject.

					//vv("%v do not vote if already voted", s.me())
					// if voted is nil or candidateId, check
					//  for candidates log being up to date.
					voted := s.state.HaveVoted

					// This kind of complex optimization tries to
					// handle lost votes and duplicated vote requests and actually
					// vote a second time for the same peerID...as
					// we voted for the first time, if in the same term.

					// so: if voted for candidate, check log up to date
					//     if voted for anyone else, vote false.
					//     if not voted yet, check log up to date

					votedForOther := voted && s.state.VotedFor != reqVote.FromPeerID
					if votedForOther {
						//vv("%v votedForOther s.state.VotedFor = '%v'", s.me(), s.state.VotedFor)
						done = true
						needStateSave = false
						vote.Reason = fmt.Sprintf("%v votedForOther s.state.VotedFor = '%v'", s.me(), rpc.AliasDecode(s.state.VotedFor))
					}
				}
			}
		}
	}
	if !done {
		//vv("%v reqVote: checking for up-to-date logs", s.me())
		// INVAR: reqVote.Term >= s.state.CurrentTerm &&
		//        (s.state.VotedFor == "" ||
		//         s.state.VotedFor == reqVote.FromPeerID) <<< must vote again for same candidate as before if they ask again.

		// If votedFor is null or candidateID,
		// grant vote if candidate's log is at
		// least as up-to-date as receiver's log (3.4, 3.6)
		if s.logUpToDateAsOurs(reqVote) {
			//vv("%v voting for %v", s.me(), rpc.AliasDecode(reqVote.FromPeerID))
			s.state.VotedFor = reqVote.FromPeerID
			s.state.VotedForName = reqVote.FromPeerName
			s.state.HaveVoted = true
			s.state.HaveVotedTerm = reqVote.CandidatesTerm
			vote.VoteGranted = true
			needStateSave = true
			vote.Reason = fmt.Sprintf("%v voting for %v (%v), satisfied logUpToDateAsOurs check", s.me(), alias(reqVote.FromPeerID), reqVote.FromPeerName)
			//vv("%v", vote.Reason)

			// "If election timeout elapses without receiving
			// AppendEntries RPC from current leader or **granting
			// vote to candidate**: convert to candidate."
			// -- https://thesquareplanet.com/blog/students-guide-to-raft/
			// quoting figure 2 of the Raft extended paper.
			s.resetElectionTimeout("handleRequestVote, granted vote to candidate")

		} else {
			//vv("%v voting against old log %v", s.me(), rpc.AliasDecode(reqVote.FromPeerID))
			// arg! this is NOT voting though! it is just rejecting a vote.

			//s.state.HaveVoted = true
			//needStateSave = true
			vote.VoteGranted = false

			// page 27
			// Raft servers must persist enough information
			// to stable storage to survive server restarts
			// safely. In particular, each server persists
			// its current term and vote; this is necessary
			// to prevent the server from voting twice in the
			// same term or replacing log entries from a newer
			// leader with those from a deposed leader. Each
			// server also persists new log entries before they
			// are counted towards the entries' commitment;
			// this prevents committed entries from being lost
			// or "uncommitted" when servers restart.
		}
	}
	if needStateSave {
		s.saver.save(s.state)
	}

	// I don't think this is what Fig 3.1 actually implies.
	//s.updateLogInfo(reqVote, vote)

	vote.CandidatesTerm = s.state.CurrentTerm
	voteFrag := s.newFrag()
	bts, err := vote.MarshalMsg(nil)
	panicOn(err)
	voteFrag.Payload = bts
	voteFrag.FragOp = VoteMsg
	voteFrag.FragSubject = "Vote"

	// handleRequestVote, per Ch 4, must ignore configuration,
	// so use ckt not cktReplica.
	cktP, ok := s.cktall[reqVote.FromPeerID]
	if !ok {
		alwaysPrintf("%v don't know how to contact '%v' in handleRequestVote(). Assuming they died.", s.me(), reqVote.FromPeerID)
		return
	}
	ckt := cktP.ckt
	err = s.SendOneWay(ckt, voteFrag, -1, 0)
	_ = err // don't panic on halting.
	if err != nil {
		alwaysPrintf("%v non nil error '%v' on Vote to '%v'", s.me(), err, ckt.RemotePeerID)
	}
	// end of handleRequestVote()
}

// A pre-vote survey must affirm a vote can be
// won before a node can become a
// candidate. (We also optimize the pre-vote
// itself by doing it in two-phases, our own
// custom optimization on top of the Raft disseration.)
// Pre-vote is a very import addition to core Raft.
// Not only does pre-vote reduced leader churn, it
// _also_ creates a read-lease allowing fast local reads
// from the leader under certain additional circumstances.
// (Namely that the leader has heard at least a
// heartbeat ack recently from a majority. Thus
// leader knows they are still the leader in their
// current term, and thus the their current term is the
// the most recent).
//
// With this method, handleRequestPreVote(), is called, some
// remote peer has started a pre-vote survey, asking
// what our vote would be. Their election timeout
// has fired, indicating they have not heard from
// their leader for too long. They are wondering
// what is going on, and do they need to stand up
// and become leader? This could be due to the
// leader actually dying/going offline, or due
// to a network partition, or simple message loss.
//
// The net result of the pre-vote mechanism is
// to maintain cluster stability in the face of
// temporary communication problems
// by preventing a zombie peer (returning from the dead;
// i.e. returning from being partitioned)
// from asking for an election it will never win.
// They won't win the pre-vote if the leader is
// actually still in power,
// because the zombie's logs are typically far behind.
// Without pre-voting, their term number
// can advance far ahead while they were isolated
// and constantly running their own failing elections.
//
// That is the main hazard that pre-voting avoids--
// that while partitioned (and without pre-votes)
// the zombie's local Term will increment up
// to a high number all alone.
// In contrast, with pre-voting, they always lose,
// the pre-vote because the zombie cannot contact
// a majority. Thus their term does not race ahead;
// in fact their term will not change at all since
// the partition, as they will never
// be allowed to progress to candidate status.
// Hence when they return from the dead
// (when the network partition is healed),
// they can join without disrupting the
// current leadership with an un-winnable
// election request.
func (s *TubeNode) handleRequestPreVote(reqPreVote *RequestVote, ckt0 *rpc.Circuit) {
	//vv("%v \n-------->>>    handle Request PreVote()  <<<--------\n reqPreVote = %v", s.me(), reqPreVote)

	// adopt their MC? wait for ae from leader; we
	// don't want to adopt a stray zombie's MC by mistake.

	s.hlc.ReceiveMessageWithHLC(reqPreVote.SenderHLC)

	// short-circuit once decision made.
	done := false

	// etcd ~/go/src/github.com/etcd-io/raft/node.go:195
	// gives a condition for voting yes wrt having hearing
	// from the leader in the past election timeout interval.
	// "[forgetting a leader] is useful with PreVote+CheckQuorum,
	// where followers will normally not
	// grant pre-votes if they've heard from the leader in the past election
	// timeout interval. [discussion continues but elided here]
	// Me: This seems to void the important sticky-leader
	// optimization, so I'm ignoring etcd's tack here.

	preVote := &Vote{
		ClusterID:           s.ClusterID,
		FromPeerID:          s.PeerID,
		FromPeerName:        s.name,
		FromPeerServiceName: s.PeerServiceName,
		FromPeerCurrentTerm: s.state.CurrentTerm,
		FromPeerCurrentLLI:  s.lastLogIndex(),
		FromPeerCurrentLLT:  s.lastLogTerm(),

		CandidateID: reqPreVote.FromPeerID, // allow double check
		// leave false unless below explicitly granted.
		IsPreVote:      true,
		CandidatesTerm: reqPreVote.CandidatesTerm,
		MC:             s.state.MC,
	}
	if s.state.MC == nil {
		// without an MC, we should not be voting at all.
		// We either need a manually configured initial
		// bootstrap config that is manually set on all
		// nodes at start time, or we need an MC from
		// a prior leader.
		preVote.Reason = fmt.Sprintf("%v we have nil MC, so declining to pre-vote. reqPreVote '%v', leave VoteGranted: false", s.me(), reqPreVote)
		done = true
		preVote.VoteGranted = false
	}

	// sticky-leader... should we have to be FOLLOWER? or any?
	if !done {
		if recentLeader, denyTm := s.haveStickyLeader(reqPreVote); recentLeader {
			// in handleRequestPreVote here.
			preVote.VoteGranted = false
			done = true
			preVote.Reason = fmt.Sprintf("%v deny pre-vote to '%v' because have sticky-leader (%v). AE sets: s.lastLegitAppendEntries(%v).After(denyAfterIfLeaderSeen(%v)); reqPreVote='%v'", s.me(), rpc.AliasDecode(reqPreVote.FromPeerID), rpc.AliasDecode(s.leaderID), s.lastLegitAppendEntries, denyTm, reqPreVote)
			//vv("%v have sticky-leader, denying pre-vote", s.me())
		}
	}

	// Me: pre-vote should never change any
	// of the Raft state in any receivers. It is only a probe.
	// Responders should answer accurately with respect
	// to their CurrentTerm and role, however; how they "would vote".
	// So we changed this next check to include <= not just <
	// because on equality, we already have a candidacy (voted for self),
	// or leader for this current term.

	if !done {
		if reqPreVote.CandidatesTerm <= s.state.CurrentTerm {
			preVote.VoteGranted = false
			//vv("%v term too small, deny pre-vote. reqPreVote=\n %v", s.me(), reqPreVote)
			done = true
			preVote.Reason = fmt.Sprintf("%v deny pre-vote because reqPreVote.CandidatesTerm(%v) <= s.state.CurrentTerm(%v)", s.me(), reqPreVote.CandidatesTerm, s.state.CurrentTerm)
		}
	}

	if !done {
		//vv("%v reqPreVote: checking for up-to-date logs", s.me())
		// INVAR: no sticky leader.
		// INVAR: we have not pre-voted yet.
		// INVAR: reqPreVote.Term >= s.state.CurrentTerm
		//
		// There is a possible optimization for lost pre-votes
		// where we will vote again for the same pre-vote request
		// and term as before, but its enough trouble getting
		// the basic right to start with, so leave that out for now.
		//        (s.state.VotedFor == "" ||
		//         s.state.VotedFor == reqPreVote.FromPeerID)

		// If votedFor is null or candidateID,
		// grant vote if candidate's log is at
		// least as up-to-date as receiver's log (3.4, 3.6)
		if s.logUpToDateAsOurs(reqPreVote) {
			//vv("%v pre-voting for %v", s.me(), reqPreVote.FromPeerID)

			//s.preVotedFor = reqPreVote.FromPeerID
			preVote.VoteGranted = true
			//s.havePreVoted = true

			preVote.Reason = fmt.Sprintf("%v yes pre-vote because their log is as up to date as ours. reqPreVote=%v", s.me(), reqPreVote)
		} else {
			//vv("%v voting against old log %v", s.me(), reqPreVote.FromPeerID)

			preVote.VoteGranted = false
			//s.havePreVoted = true

			preVote.Reason = fmt.Sprintf("%v deny pre-vote to old log. reqPreVote='%v'", s.me(), reqPreVote)
		}
	}

	// pre-votes are entirely volatile and
	// never need to save to disk.

	// Big concern: how does the partitioned re-joiner
	// decrement their Term after losing a pre-vote?
	// Answer: I think the idea is that they never got to
	// increment it in the first place(!) So it
	// can stay monotonically increasing.

	preVoteFrag := s.newFrag()
	bts, err := preVote.MarshalMsg(nil)
	panicOn(err)
	preVoteFrag.Payload = bts
	preVoteFrag.FragOp = PreVoteMsg
	preVoteFrag.FragSubject = "PreVote"

	// have to use ckt not cktReplica per Ch 4
	// where we must ignore configuration.
	cktP, ok := s.cktall[reqPreVote.FromPeerID]
	if !ok {
		alwaysPrintf("%v don't know how to contact '%v' (%v) to respond to pre-vote in handleRequestPreVote. Assuming they died. s.cktall = '%v'", s.me(), reqPreVote.FromPeerID, reqPreVote.FromPeerName, s.cktall2string())
		return
	}
	ckt := cktP.ckt
	err = s.SendOneWay(ckt, preVoteFrag, -1, 0)
	_ = err // don't panic on halting.
	if err != nil {
		alwaysPrintf("%v non nil error '%v' on Vote to '%v'", s.me(), err, ckt.RemotePeerID)
	}
	// end of handleRequestPreVote()
}

func (s *TubeNode) cktall2string() (r string) {
	r = "cktall{\n"
	for k, v := range s.cktall {
		r += fmt.Sprintf("%v -> %v\n", k, v)
	}
	return r + "}"
}

func (s *TubeNode) insaneConfig() bool {

	crazyHB := 5 * time.Millisecond
	if s.cfg.HeartbeatDur < crazyHB {
		alwaysPrintf("%v insane config: cfg.HeartbeatDur(%v) must be greater than %v", s.me(), s.cfg.HeartbeatDur, crazyHB)
		return true
	}
	crazyElect := 50 * time.Millisecond
	if s.cfg.MinElectionDur < crazyElect {
		alwaysPrintf("%v insane config: cfg.MinElectionDur(%v) must be greater than %v", s.me(), s.cfg.MinElectionDur, crazyElect)
		return true
	}

	hbMin := s.cfg.HeartbeatDur * 5
	if s.cfg.MinElectionDur < hbMin {
		alwaysPrintf("%v insane config: cfg.MinElectionDur(%v) must be greater than 5*HeartbeatDur(%v) which means MinElectionDur must be >= %v", s.me(), s.cfg.MinElectionDur, hbMin, s.cfg.HeartbeatDur, hbMin)
		return true
	}

	window := s.cfg.MinElectionDur - s.cfg.ClockDriftBound
	if window < 5*time.Millisecond {
		alwaysPrintf("%v insane config: cfg.MinElectionDur(%v) - cfg.ClockDriftBound(%v) = window (%v) is less than 5 msec", s.me(), s.cfg.MinElectionDur, s.cfg.ClockDriftBound, window)
		return true
	}

	return false
}

// reset all pre-vote state.
// The preVoteTerm is set to 0.
func (s *TubeNode) resetPreVoteState(beginElec, haveLeader bool) {
	//vv("%v resetPreVoteState called; s.preVoteOkLeaderElecDeadline was in '%v'", s.me(), s.preVoteOkLeaderElecDeadline.Sub(time.Now())) // , stack())

	// if !beginElec && !haveLeader {
	// 	if !s.preVoteOkLeaderElecDeadline.IsZero() {
	// 		panic(fmt.Sprintf("%v resetPreVoteState when 2nd-half going?!", s.me()))
	// 	}
	// }

	// reset phase one
	s.preVoteTerm = 0
	s.preVotes = nil
	s.yesPreVotes = 0
	s.noPreVotes = 0

	// reset phase two
	s.preVoteOkLeaderElecTimeoutCh = nil
	s.preVoteOkLeaderElecDeadline = time.Time{}
	s.preVoteOkLeaderElecTerm = 0

	// reset diagnostics
	s.preVotePhase1Began = time.Time{}
	s.preVotePhase1Deadline = time.Time{}
	s.preVotePhase1EndedAt = time.Time{}

	s.preVotePhase2Began = time.Time{}
	s.preVotePhase2EndsBy = time.Time{}
	s.preVotePhase2EndedAt = time.Time{}

}

// used for READ, READ_KEYRANGE, READ_PREFIX_RANGE, and SHOW_KEYS now.
func (s *TubeNode) leaderServedLocalRead(tkt *Ticket, isWriteCheckLease bool) bool {
	if s.role != LEADER {
		panic("must only be called on leader")
	}

	// Note: if we return true and an txt.Err instead
	// of an actual read, be sure to call
	// s.respondToClientTicketApplied to send the error response.

	// an inlined version of this is already below.
	// TODO: can we consolidate?
	//if s.leaderDoneEarlyOnSessionStuff(tkt) {
	//	return
	//}

	s.totalReadCount++

	canServe, untilTm := s.leaderCanServeReadsLocally()
	if !canServe {
		//vv("%v could not serve read locally", s.me())
		return false
	}
	//vv("%v about to serve read locally", s.me())

	var ste *SessionTableEntry

	// fast local reads also return tkt.MC, just
	// like replicated tickets.
	if s.state != nil && s.state.MC != nil {
		tkt.MC = s.state.MC.Clone()
	}

	if tkt.SessionID != "" {
		var ok bool
		ste, ok = s.state.SessTable[tkt.SessionID]
		if !ok {

			tkt.Err = fmt.Errorf("%v leader error: unknown Ticket.SessionID='%v' (SessionSerial='%v'). Must call CreateNewSession first to generate/register a new SessionID. In leaderServedLocalRead().", s.name, tkt.SessionID, tkt.SessionSerial)
			//vv("%v tkt.Err = '%v'", s.me(), tkt.Err)

			//vv("%v tkt.Err = '%v'", s.name, tkt.Err)
			tkt.Stage += ":unkown_SessionID_leaderServedLocalRead"
			// actually DO think we need to do this here! otherwise
			// tup reader can just hang!
			s.respondToClientTicketApplied(tkt) // or s.replyToForwardedTicketWithError(tkt)?
			return true                         // done early

		} else {
			// session exists
			s.refreshSession(time.Now(), ste)

			// cleanup old state sessions
			defer s.cleanupAcked(ste, tkt.MinSessSerialWaiting)

			priorTkt, already := ste.Serial2Ticket.Get2(tkt.SessionSerial)
			if already {
				// dedup: return the previous read, to preserve
				// linearizability (linz).
				//
				// As Ongaro in the Raft disseratation, page 71,
				// section 6.3 "Implementing linearizable semantics",
				// says:
				// "The session tracks the latest serial
				// number processed for the client, along
				// with the associated response. If a
				// server receives a command whose serial
				// number has already been executed, it
				// responds immediately without re-executing
				// the request.
				// Given this filtering of duplicate requests,
				// Raft provides linearizability. The Raft
				// log provides a serial order in which
				// commands are applied on every server.
				// Commands take effect instantaneously and
				// exactly once according to their first
				// appearance in the Raft log, since any
				// subsequent appearances are filtered out
				// by the state machines as described above.

				tkt.Err = priorTkt.Err
				tkt.AsOfLogIndex = priorTkt.AsOfLogIndex
				// READ and SHOW_KEYS: (and WRITE)
				tkt.Val = priorTkt.Val
				tkt.Vtype = priorTkt.Vtype
				tkt.LeaseRequestDur = priorTkt.LeaseRequestDur
				tkt.Leasor = priorTkt.Leasor
				tkt.LeaseEpoch = priorTkt.LeaseEpoch
				tkt.LeaseAutoDel = priorTkt.LeaseAutoDel
				tkt.VersionRead = priorTkt.VersionRead
				tkt.LeaseWriteRaftLogIndex = priorTkt.LeaseWriteRaftLogIndex
				tkt.LeaseUntilTm = priorTkt.LeaseUntilTm

				// READ_KEYRANGE, READ_PREFIX_RANGE, SHOW_KEYS:
				tkt.KeyValRangeScan = priorTkt.KeyValRangeScan

				tkt.DupDetected = true
				tkt.LogIndex = priorTkt.LogIndex
				tkt.Term = priorTkt.Term
				// is above already: tkt.AsOfLogIndex = priorTkt.AsOfLogIndex

				return true
			}

			// Note that we are only dealing with reads here in
			// leaderServedLocalRead(). And writes to check for
			// failed leases if isWriteCheckLease.

			// the problem with killing a session
			// is that to a new leader, the local-reads served by the
			// previous leader look like gaps/dropped serial
			// numbers, even though they were just
			// local reads. For performance purposes,
			// we want local reads to be fast and not
			// tell followers about them. So now we
			// skip this early-error-out return and avoid
			// killing the session so the new leader
			// can continue it.
			//
			// If a read is dropped its no big deal. If a
			// write was dropped before replication,
			// well it just never made
			// it to through the raft log, so client needs
			// to re-try before incrementing their serial
			// number if they care. If the reply to a
			// successful write was dropped, the client retry
			// with the same serial number is idempotent,
			// so no problem; it will likely succeed on
			// the client's 2nd attempt.
		}
	}

	// the local read lease is in-force,
	// so we can do the read locally.
	switch tkt.Op {
	case READ:
		s.doReadKey(tkt)
	case SHOW_KEYS:
		s.doShowKeys(tkt)
	case READ_KEYRANGE:
		s.doReadKeyRange(tkt)
	case READ_PREFIX_RANGE:
		s.doReadPrefixRange(tkt)
	case WRITE:
		if !isWriteCheckLease {
			panicf("unhandled tkt.Op case in leaderServedLocalRead; tkt=%v", tkt)
		}
		if s.state.kvstoreWouldWriteLease(tkt, s.cfg.ClockDriftBound) {
			return false
		}
		// otherwise the read info for the current lease
		// that prevents the write is filled in on tkt now.
		// Since the read is good and it says the write to lease
		// would fail, we want to return the lease metainfo
		// (the failed write turns into a fast local read to avoid another
		// network roundtrip).

	default:
		panicf("unhandled tkt.Op case in leaderServedLocalRead; tkt=%v", tkt)
	}
	tkt.AsOfLogIndex = s.state.LastApplied
	tkt.LeaderLocalReadGoodUntil = untilTm
	hlc := s.hlc.CreateSendOrLocalEvent()
	tkt.LeaderLocalReadAtTm = hlc.ToTime()
	tkt.LeaderLocalReadHLC = hlc

	s.localReadCount++

	if ste != nil {
		if tkt.SessionSerial > ste.HighestSerialSeenFromClient {
			ste.HighestSerialSeenFromClient = tkt.SessionSerial
		}
		tktClone := tkt.clone()
		ste.ticketID2tkt[tktClone.TicketID] = tktClone
		ste.Serial2Ticket.Set(tktClone.SessionSerial, tktClone)
	}

	if tkt.FromID == s.PeerID {
		s.FinishTicket(tkt, true) // true b/c here always leader
	} else {
		s.respondToClientTicketApplied(tkt)
	}
	return true
}

func (s *TubeNode) doWrite(tkt *Ticket) {
	s.state.kvstoreWrite(tkt, s.cfg.ClockDriftBound)
}

func (s *TubeNode) doCAS(tkt *Ticket) {
	if s.state.KVstore == nil {
		if tkt.OldVersionCAS > 0 {
			tkt.Err = ErrKeyNotFound
			return
		}
		if len(tkt.OldVal) > 0 {
			tkt.Err = ErrKeyNotFound
			return
		} else {
			// request to start a key from scratch
			s.state.kvstoreWrite(tkt, s.cfg.ClockDriftBound)
			tkt.CASwapped = (tkt.Err == nil)
			return
		}
	}
	table, ok := s.state.KVstore.m[tkt.Table]
	if !ok {
		if tkt.OldVersionCAS > 0 {
			tkt.Err = ErrKeyNotFound
			return
		}
		if len(tkt.OldVal) == 0 {
			// request to start a key from scratch
			s.state.kvstoreWrite(tkt, s.cfg.ClockDriftBound)
			tkt.CASwapped = (tkt.Err == nil)
			return
		}
		tkt.Err = ErrKeyNotFound
		return
	}
	leaf, _, found := table.Tree.Find(art.Exact, art.Key(tkt.Key))
	if !found {
		if tkt.OldVersionCAS > 0 {
			tkt.Err = ErrKeyNotFound
			return
		}
		if len(tkt.OldVal) == 0 {
			// request to start a key from scratch
			s.state.kvstoreWrite(tkt, s.cfg.ClockDriftBound)
			tkt.CASwapped = (tkt.Err == nil)
			return
		}
		tkt.Err = ErrKeyNotFound
		return
	}
	if tkt.OldVersionCAS > 0 && tkt.OldVersionCAS != leaf.Version {
		tkt.Err = fmt.Errorf("CAS rejected on OldVersionCAS='%v' vs current Version='%v'", tkt.OldVersionCAS, leaf.Version)
		tkt.CASwapped = false
		return
	}

	curVal := leaf.Value
	//vv("%v cas: have curVal = '%v' for key='%v' from table '%v'; tkt.OldVal='%v'; will swap = %v (new val = '%v')", s.name, string(curVal), tkt.Key, tkt.Table, string(tkt.OldVal), bytes.Equal(curVal, tkt.OldVal), string(tkt.Val))

	if !bytes.Equal(curVal, tkt.OldVal) { // compare
		tkt.CASRejectedBecauseCurVal = append([]byte{}, curVal...)
		tkt.CASwapped = false
		return
	}
	// kvstoreWrite takes care of leasing rejections
	s.state.kvstoreWrite(tkt, s.cfg.ClockDriftBound)
	tkt.CASwapped = (tkt.Err == nil)
}

func (s *TubeNode) doReadKey(tkt *Ticket) {
	if s.state.KVstore == nil {
		tkt.Err = ErrKeyNotFound
		return
	}
	var leaf *art.Leaf
	leaf, tkt.Err = s.state.KVStoreReadLeaf(tkt.Table, tkt.Key)
	if leaf != nil {
		tkt.Val = append([]byte{}, leaf.Value...)
		tkt.Vtype = leaf.Vtype
		tkt.Leasor = leaf.Leasor
		tkt.LeaseUntilTm = leaf.LeaseUntilTm
		tkt.LeaseEpoch = leaf.LeaseEpoch
		tkt.LeaseAutoDel = leaf.AutoDelete
		tkt.LeaseWriteRaftLogIndex = leaf.WriteRaftLogIndex
		tkt.VersionRead = leaf.Version
	}
}

func (s *TubeNode) doReadKeyRange(tkt *Ticket) {
	if s.state.KVstore == nil {
		tkt.Err = ErrKeyNotFound
		return
	}
	tkt.KeyValRangeScan, tkt.Err =
		s.state.kvstoreRangeScan(tkt.Table, tkt.Key, tkt.KeyEndx, tkt.ScanDescend)
}

func (s *TubeNode) doReadPrefixRange(tkt *Ticket) {
	if s.state.KVstore == nil {
		tkt.Err = ErrKeyNotFound
		return
	}
	tkt.KeyValRangeScan, tkt.Err =
		s.state.kvstorePrefixScan(tkt.Table, tkt.Key, tkt.ScanDescend)
}

func (s *TubeNode) setConfigDefaultsIfZero() {
	// reserve negatives for future use.
	if s.cfg.HeartbeatDur < 0 {
		panic("negative HeartbeatDur not allowed")
	}
	if s.cfg.MinElectionDur < 0 {
		panic("negative MinElectionDur not allowed")
	}

	if s.cfg.HeartbeatDur == 0 {
		s.cfg.HeartbeatDur = time.Millisecond * 50
	}
	if s.cfg.MinElectionDur == 0 {
		s.cfg.MinElectionDur = 10 * s.cfg.HeartbeatDur
	}
	if s.cfg.BatchAccumulateDur == 0 {
		s.cfg.BatchAccumulateDur = time.Millisecond * 100
	}
	//vv("%v end setConfigDefaultsIfZero(). s.cfg.HeartbeatDur=%v; s.cfg.MinElectionDur=%v", s.me(), s.cfg.HeartbeatDur, s.cfg.MinElectionDur)
}

// leaderCanServeReadsLocally is a popular and
// desirable performance optization for reading
// the current system state. Writes still
// require network delays to replicate to
// a quorum, of course. Nonetheless,
// not needing to wait a network roundtrips
// for reads ("local" reads) on the leader is a big win.
//
// Here is why it works:
//
// We assume pre-voting is used, as it always
// is in Tube, and the noop0 has been committed
// in the leader's current term, as it always
// will be in Tube. I mention this only because
// these are two optional optimizations to the core Raft
// algorithm (although suggested by the Raft
// dissertation, they are not in Chapter 3,
// the description of core Raft),
// and so local reads would not be a safe
// optimization in a more minimal core Raft implementation.
//
// Pre-Vote is discussed page 137 section 9.6
// of the Raft dissertation. Committing a no-op
// at the start of each term is discussed
// on page 72 section 6.4 on processing read-only
// queries more efficiently.
//
// The leader can serve local reads when
// they have received back a heartbeat from a majority
// of nodes that the leader *sent* within
// the last ElectionTimeoutDur period. This
// is safe, and maintains linearizability,
// because the pre-voting and recent, acknowledged communication
// from the leader have together effected a
// quorum read-lease for the
// remainder of the smallest time left on any
// follower's election timeout. The pre-vote
// rules guarantee a quorum will deny votes to anyone
// seeking to stand for election during this time.
//
// Thus we know there will be no other leader during this
// period; and no higher term. In fact the
// system would become unavaible
// for this read lease period if the leader crashed. The flip
// side/benefit of that is this is that we, the
// only possible leader, can serve reads without network
// roundtrips to guarantee "the most recent"
// committed value. It is available to the
// leader locally, and they can reply to client
// read requests immediately.
//
// We check each followers election timeout,
// taking the smallest if one node happens to be
// configured differently.
//
// Of course, only committed values can
// be served in this way, since non-committed
// values may be overwritten later. To
// maintain this, we only read from the
// applied state. This is what s.state.KVstore
// represents.
//
// To address use-after-check issues, we
// return in untilTm the last valid time
// at which the client can know that our
// read was not stale according to this
// local peer/leader's clock.
func (s *TubeNode) leaderCanServeReadsLocally() (canServe bool, untilTm time.Time) {
	//vv("%v top leaderCanServeReadsLocally", s.me())
	//defer func() {
	//	vv("%v end leaderCanServeReadsLocally; canServe=%v; untilTm=%v (%v from now)", s.me(), canServe, nice(untilTm), untilTm.Sub(time.Now()))
	//}()
	if s.role != LEADER {
		panic("should only be called on leader")
		return
	}
	if s.clusterSize() <= 1 {
		// single node cluster should be able to serve reads locally.
		//vv("%v leaderCanServeReadsLocally ClusterSize <= 1", s.me())
		window := (s.cfg.MinElectionDur / 2) - s.cfg.ClockDriftBound
		if window <= 5*time.Millisecond {
			panicf("window must be >= 5 msec duration, not: %v; s.cfg.MinElectionDur / 2=%v", window, s.cfg.MinElectionDur/2)
		}
		untilTm = time.Now().Add(window)
		return true, untilTm
	}
	// in leaderCanServeReadsLocally() here.
	if !s.initialNoop0HasCommitted {
		// a new leader must consider themselves
		// "stale" wrt reads until noop0 commits.
		// See Figure 3.7 of the Raft dissertation.
		//vv("%v leaderCanServeReadsLocally: noop0 has not committed", s.name)
		return
	}
	need := s.quorum() - 1 // not including leader self
	if len(s.peers) < need {
		//vv("%v leaderCanServeReadsLocally: insufficient quorum", s.name)
		return
	}

	// not really a part of local-only reads
	// section 6.4 "Processing read-only queries more efficiently"
	// but not sure where to implement it yet...
	s.readIndexOptim = s.state.CommitIndex

	// Require at least 80% of the read lease to still
	// be left (since election >= 10 * heartbeat).
	// Really this should never make
	// a difference. The read lease _should_ be renewed on
	// every heartbeat, and if we go past this
	// the follower probably just went down
	// or got blocked in application code (their
	// comm link is still up, or they wouldn't
	// be in our follower list). Since they are
	// a bit of a wildcard at that point,
	// we don't want to use them in our quorum.
	slop := 2 * s.cfg.HeartbeatDur

	good := 0
	now := time.Now()
	oldestSentFullPing := now
	minWindow := time.Hour
	for _, info := range s.peers {
		if info.PeerServiceName != TUBE_REPLICA {
			continue
		}
		if info.MinElectionTimeoutDur == 0 ||
			info.LastHeardAnything.IsZero() {
			// have not heard from yet, do not freak out.
			continue
		}
		// We hope all peers share an election timeout,
		// otherwise the elections will be biased;
		// but we don't assume it and use what they report using.
		window := info.MinElectionTimeoutDur - slop
		if window < 0 {
			panic(fmt.Sprintf("%v: bad mis-configuration of cluster: info.MinElectionTimeoutDur='%v' from peerName '%v': must be much > slop = '%v' = 2* s.cfg.HeartbeatDur = 2* %v; info='%#v'", s.me(), info.MinElectionTimeoutDur, info.PeerName, slop, s.cfg.HeartbeatDur, info))
		}
		readLeaseWindowBeg := now.Add(-window)
		if info.LastFullPing.Sent.After(readLeaseWindowBeg) {

			if window < minWindow {
				minWindow = window
			}
			if info.LastFullPing.Sent.Before(oldestSentFullPing) {
				oldestSentFullPing = info.LastFullPing.Sent
			}
			good++
			if good >= need {
				untilTm = oldestSentFullPing.Add(minWindow)
				return true, untilTm
			}
		} else {
			hbs := time.Since(info.LastFullPing.Sent) / s.cfg.HeartbeatDur
			_ = hbs
			//vv("%v warning: peer %v has not responded to heartbeat in '%v' (%v heartbeats)", s.me(), info.PeerID, time.Since(info.LastFullPing.Sent), int(hbs))
		}
	}
	//vv("%v warning: leader could not get local read-lease from a quorum %v of our cluster %v", s.me(), need, s.clusterSize())
	return
}

func (s *TubeNode) cloneWaitingAtLeaderToMap() (r map[string]*Ticket) {
	r = make(map[string]*Ticket)
	for _, v := range s.WaitingAtLeader.all() {
		r[v.TicketID] = v.clone()
	}
	return
}

func (s *TubeNode) cloneWaitingAtFollowToMap() (r map[string]*Ticket) {
	r = make(map[string]*Ticket)
	for _, v := range s.WaitingAtFollow.all() {
		r[v.TicketID] = v.clone()
	}
	return
}

func (s *TubeNode) cloneWaitingAtLeader() (r *imap) {
	r = newImap()
	for _, tkt := range s.WaitingAtLeader.all() {
		r.set(tkt.TicketID, tkt.clone())
	}
	return
}

func (s *TubeNode) cloneWaitingAtFollow() (r *imap) {
	r = newImap()
	for _, tkt := range s.WaitingAtFollow.all() {
		r.set(tkt.TicketID, tkt.clone())
	}
	return
}

func (s *TubeNode) choiceString(choices []string) (r string) {
	r = "choices{"
	for i, c := range choices {
		if i == 0 {
			r += "\n"
		}
		r += c + "\n"
	}
	r += "}"
	return
}

// the TubeSim version lets us track the choices
// that handleAppendEntries() makes.
func (s *TubeNode) choice(format string, a ...interface{}) {
	if s.isRegularTest() { // simae_test won't get here anyway, but good form.
		a = append([]any{fileLine(2)}, a...)
		last := fmt.Sprintf("AE CHOICE(%v): "+format, a...)
		s.testAEchoices = append(s.testAEchoices, last)
		//vv("%v: %v", s.name, last)
	}
	// allow compiler to get rid of these calls if possible.
	//if s != nil && false { // s.choiceLoud {
	//tsPrintf("CHOICE: "+format, a...)
	//}
}

// The sticky leader optimization is
// critical for stability with configuration change,
// otherwise leavers (not in C_new) can be very disruptive.
// Section 4.2.3 Disruptive Servers, pages 40-41
//
// "Unfortunately, the Pre-Vote phase does
// not solve the problem of disruptive servers:
// there are situations where the disruptive server’s log
// is sufficiently up-to-date, but starting an election
// would still be disruptive. Perhaps surprisingly,
// these can happen even before the configuration
// change completes. For example, Figure 4.7 shows
// a server that is being removed from a cluster.
// Once the leader creates the Cnew log entry, the
// server being removed could be disruptive. The
// Pre-Vote check does not help in this case, since
// the server being removed has a log that is more
// up-to-date than a majority of either cluster.
// (Though the Pre-Vote phase does not solve the
// problem of disruptive servers, it does turn out to
// be a useful idea for improving the robustness of
// leader election in general; see Chapter 9.)"
//
// "Because of this scenario, we now believe that
// no solution based on comparing logs alone (such
// as the Pre-Vote check) will be sufficient to
// tell if an election will be disruptive. We
// cannot require a server to check the logs of
// every server in Cnew before starting an election,
// since Raft must always be able to tolerate faults.
// We also did not want to assume that a leader
// will reliably replicate entries fast enough
// to move past the scenario shown in Figure 4.7
// quickly; that might have worked in practice,
// but it depends on stronger assumptions that
// we prefer to avoid about the performance of
// finding where logs diverge and the
// performance of replicating log entries."
//
// "The sticky leader optimization is page 43, section 4.2.3
// of the Raft dissertation.
// "Raft’s solution uses heartbeats to determine
// when a valid leader exists. In Raft, a leader is
// considered active if it is able to maintain
// heartbeats to its followers (otherwise, another
// server will start an election). Thus, servers
// should not be able to disrupt a leader whose
// cluster is receiving heartbeats. We modify
// the RequestVote RPC to achieve this: if a server
// receives a RequestVote request within the
// minimum election timeout of hearing from a
// current leader, it does not update its term
// or grant its vote. It can either drop the
// request, reply with a vote denial, or delay
// the request; the result is essentially the
// same. This does not affect normal elections,
// where each server waits at least a minimum
// election timeout before starting an election.
// However, it helps avoid disruptions from
// servers not in Cnew: while a leader is able
// to get heartbeats to its cluster, it will
// not be deposed by larger term numbers.
// This change conflicts with the leadership
// transfer mechanism as described in Chapter 3,
// in which a server legitimately starts an
// election without waiting an election timeout.
// In that case, RequestVote messages should
// be processed by other servers even when
// they believe a current cluster leader exists.
// Those RequestVote requests can include
// a special flag to indicate this behavior
// ('I have permission to disrupt the
// leader--it told me to!')."
func (s *TubeNode) haveStickyLeader(reqVote *RequestVote) (recentLeader bool, denyTm time.Time) {

	// Leader stickiness - prevent flip-flopping
	// between leaders.
	// https://web.archive.org/web/20160304020315if_/http://openlife.cc/system/files/4-modifications-for-Raft-consensus.pdf
	// "Four modifications for the Raft consensus algorithm"
	// Henrik Ingo
	// henrik.ingo@openlife.cc
	// September 2015
	// also https://groups.google.com/g/raft-dev/c/2OGYyUmjRFY
	// Same rule as in pre-vote.

	if s.role == LEADER {
		// I certainly think I'm sticky. I'm alive!
		recentLeader = true
		return
	}

	if s.leaderID != "" {
		return // no leader at all
	}

	if reqVote.LeadershipTransferFrom != "" &&
		reqVote.LeadershipTransferFrom == s.leaderID {
		return // over-ride, say no sticky leader
	}

	//clockDriftBound := s.clockDriftBound // time.Millisecond * 500 // even less chance of granting vote...
	// page 42 says minElectionTimeout
	denyAfterIfLeaderSeen := time.Now().Add(-s.minElectionTimeoutDur() - s.cfg.ClockDriftBound)
	if s.lastLegitAppendEntries.After(denyAfterIfLeaderSeen) {
		denyTm = denyAfterIfLeaderSeen
		recentLeader = true
		return
	}
	return

}

// old: used to take ckt *rpc.Circuit as argument.
func (s *TubeNode) verifyCluster(remotePeerID string) {
	//vv("top verifyCluster. verifyPeersNeeded = %v; verifyPeersSeen=%p", s.verifyPeersNeeded, s.verifyPeersSeen)

	if s.verifyPeersNeeded == 0 ||
		s.verifyPeersSeen == nil {

		// either prod or test 802 and we don't want to crash below.
		return
	}

	s.verifyPeersSeen.Set(rpc.AliasDecode(remotePeerID), time.Now())

	peersSeen := s.verifyPeersSeen.Len()
	//vv("%v verifyCluster() peersSeen = %v; s.verifyPeersNeeded = %v", s.me(), peersSeen, s.verifyPeersNeeded)

	if peersSeen >= s.verifyPeersNeeded {
		//vv("%v verifyCluster: requirements have been met. (peersSeen(%v) >= s.verifyPeersNeeded(%v))", s.me(), peersSeen, s.verifyPeersNeeded)
		s.verifyPeersNeededSeen.Close()
		// avoid alot of extra logs, only need to know it once.
		s.verifyPeersNeeded = 0 // early exit above.
	}
}

// CallPeerTraffic must have matching Args[ClusterID].
// Other internal peer traffic is fine only
// if they do not have the ClusterID at all.
func (s *TubeNode) notOurClusterAndNotCircuitStart(frag *rpc.Fragment) (drop bool, dropReason string) {

	ok := false
	fragCluster := ""
	if len(frag.Args) > 0 {
		fragCluster, ok = frag.Args["ClusterID"]
	}
	if frag.Typ != rpc.CallPeerTraffic {
		// okay to not be set on internal ckt stuff at first.
		if !ok || fragCluster == "" {
			if frag.Typ == rpc.CallPeerStartCircuit {
				//vv("warning: allowing frag w/o ClusterID b/c CallPeerStartCircuit")
			} else {
				//vv("warning: allowing frag w/o ClusterID b/c not CallPeerTraffic or empty ClusterID: %v", frag)
			}
			drop = false
			return
		}
		// if set, it had better be correct.
		if fragCluster != s.ClusterID {
			dropReason = fmt.Sprintf("[1]our ClusterID (%v) != frag.Args[ClusterID](%v): frag = %v", s.ClusterID, fragCluster, frag)
			drop = true
			return
		}
		return // ok
	}
	// INVAR:  frag.Typ == CallPeerTraffic
	if !ok {
		dropReason = fmt.Sprintf("[2]frag missing Args[ClusterID]: %v", frag)
		drop = true
		return
	}
	if fragCluster != s.ClusterID {
		dropReason = fmt.Sprintf("[3]our ClusterID (%v) != frag.Args[ClusterID](%v): frag = %v", s.ClusterID, fragCluster, frag)
		drop = true
		return
	}
	return // okay, do not drop.
}

// centralize rejecting ancient/too far
// in the future/wrong cluster garbage.
// Note this will also disable/deafen a node whose
// clock is massively wrong, on either
// sender or reader side.
func (s *TubeNode) insaneTooOldOrNew(frag *rpc.Fragment) (drop bool, dropReason string) {
	now := time.Now()
	// discards legit traffic in 050 safety_test.
	//window := s.cfg.MinElectionDur * 10
	window := 10 * time.Minute // s.cfg.MinElectionDur * 10
	lower := now.Add(-window)
	upper := now.Add(window)
	if frag.Created.Before(lower) {
		dropReason = fmt.Sprintf("frag too old(now=%v): %v", now, frag)
		drop = true
		return
	}
	if frag.Created.After(upper) {
		dropReason = fmt.Sprintf("frag too new(now=%v): %v", now, frag)
		drop = true
		return
	}
	return
}

func (s *TubeNode) newFrag() (frag *rpc.Fragment) {
	frag = s.MyPeer.NewFragment()
	frag.Created = time.Now()
	frag.FromPeerName = s.name
	frag.SetUserArg("ClusterID", s.ClusterID)
	//vv("newFrag, Serial = %v", frag.Serial)
	return
}

// print logging only from
func (s *TubeNode) ay(format string, a ...interface{}) {
	return

	rpc.TsPrintfMut.Lock()
	printf("\n%s %s say: ", fileLine(2), ts())
	printf(format+"\n", a...)
	rpc.TsPrintfMut.Unlock()
}

func noEpoch(term int64) int64 {
	return term & 0x00000000ffffffff
}

func justEpoch(term int64) int32 {
	return int32(term >> 32)
}

// helper called by GetPeerListFrom, so is an
// outside/external called func; therefore avoid accessing
// internals e.g. s.leaderID here.
// but also now internal, because handleLocalModifyMembership()
// calls it too... ugh. must be a useful routine :)
func (s *TubeNode) getCircuitToLeader(ctx context.Context, leaderURL string, firstFrag *rpc.Fragment, insideCaller bool) (ckt *rpc.Circuit, onlyPossibleAddr string, sentOnNewCkt bool, err error) {
	//vv("%v top getCircuitToLeader('%v')", s.me(), leaderURL)

	//if strings.Contains(leaderURL, "100.114.32.72") {
	//vv("top getCircuitToLeader() stack = '%v'", stack())
	//}

	// do we already have a ckt to requested leaderURL? if so, use it.

	netAddr, serviceName, leaderPeerID, _, err1 := rpc.ParsePeerURL(leaderURL)
	panicOn(err1)
	if err1 != nil {
		err = fmt.Errorf("getCircuitToLeader error: bad leaderURL('%v') supplied, could not parse: '%v'", leaderURL, err1)
		return
	}
	if serviceName != "" && serviceName != string(TUBE_REPLICA) {
		panicf("sanity check failed, serviceName('%v') != '%v' in leaderURL = '%v'", serviceName, TUBE_REPLICA, leaderURL)
	}
	// INVAR: serviceName == "" || serviceName == string(TUBE_REPLICA),
	// from the leaderURL parse (nothing to do with us and our s.PeerServiceName)

	// remember we are external here. This is a Mutexmap so
	// should be safe; and RemotePeer.IncomingCkt is
	// goroutine safe.
	if s.MyPeer == nil {
		err = fmt.Errorf("getCircuitToLeader error: no MyPeer available")
		return
	}
	var remotePeer *rpc.RemotePeer
	ok := true
	if s.MyPeer.Remotes == nil || leaderPeerID == "" {
		ok = false
	} else {
		remotePeer, ok = s.MyPeer.Remotes.Get(leaderPeerID)
	}
	if ok {
		ckt = remotePeer.IncomingCkt
		//vv("already have ckt to leaderURL '%v' -> leaderPeerID: '%v'; remotePeer = '%#v'", leaderURL, ckt, remotePeer)
		if ckt == nil {
			panic(fmt.Sprintf("no ckt avail??? leaderURL = '%v'; remotePeer = '%#v'", leaderURL, remotePeer))
		}
	} else {
		//vv("%v no prior ckt to leaderPeerID='%v'; leaderURL='%v'; s.MyPeer.Remotes = '%v'; netAddr='%v'", s.name, leaderPeerID, leaderURL, s.MyPeer.Remotes, netAddr)

		var peerServiceNameVersion string
		// here we are in getCircuitToLeader()
		// got error from ckt2.go:
		// client peer error on StartRemotePeer: remoteAddr should be 'tcp://100.89.245.101:7001' (that we are connected to), rather than the 'tcp://100.126.101.8:7006' which was requested. Otherwise your request will fail.
		// but may not be a problem in here, just retry on tuberm.
		cliRemote := s.MyPeer.U.RemoteAddr()
		if cliRemote != "" {
			//vv("substitute in '%v' in place of '%v'", cliRemote, netAddr)
			// we are on a client, and only one remote is possible.
			// that seems preferable to the guess that is leaderURL
			//netAddr = cliRemote
		}
		// retry loop to attempt onlyPossibleAddr if we get that error.
		for try := 0; try < 2; try++ {
			ckt, _, _, onlyPossibleAddr, err = s.MyPeer.PreferExtantRemotePeerGetCircuit(ctx, "tube-ckt", firstFrag, string(TUBE_REPLICA), peerServiceNameVersion, netAddr, 0, nil, waitForAckTrue)
			// can get errors if we removed the leader and then
			// got our ckt redirected to new leader, in a logical race.

			// can get errors on connection not working, don't freak.
			// e.g. error requesting CallPeerStartCircuit from remote: 'error in SendMessage: net.Conn not found
			if err == nil {
				break
			}
			break // not working for tuberm:
			if onlyPossibleAddr != "" {
				//vv("%v substitute onlyPossibleAddr(%v) into netAddr(%v)", s.me(), onlyPossibleAddr, netAddr)
				netAddr = onlyPossibleAddr
				// retry once
			}
		}
		//vv("%v getCircuitToLeader(netAddr='%v') back from PreferExtantRemotePeerGetCircuit: err='%v'", s.name, netAddr, err)
		if err != nil {
			err = fmt.Errorf("getCircuitToLeader error: myPeer.NewCircuitToPeerURL to leaderURL '%v' (netAddr='%v') (onlyPossibleAddr='%v') gave err = '%v'; ", leaderURL, netAddr, onlyPossibleAddr, err)
			return
		}
		// must manually tell the service goro
		// about the new ckt in this case.

		// this will deadlock right? since we are
		// called by main goro? is supposed to
		// be external but is also internal now...
		// so as of rpc v1.28.24, s.MyPeer.NewCircuitCh
		// is 100 bufferred now...
		select {
		case s.MyPeer.NewCircuitCh <- ckt:
			sentOnNewCkt = true
			//vv("%v manually did: s.MyPeer.NewCircuitCh <- ckt; ckt='%v'", s.name, ckt)
		case <-s.Halt.ReqStop.Chan:
			err = ErrShutDown
			return
		default:
			// let sentOnNewCkt false handle it now.
			// if caller is handleLocalModifyMembership(), they
			// will pass it up to main goro and it will in
			// turn call into handleNewCircuit(); which we
			// could do ourselves if we knew for sure we
			// were internal here...
			// don't return yet, so we service clients/observers below.
		}
	}
	switch s.PeerServiceName {
	case TUBE_CLIENT, TUBE_OBS_MEMBERS:
		// hung here on tuberm, naturally enough, because
		// we are occupying the mainline goro.
		// made setLeaderCktChan buffered.
		// but: buffering makes 707 intermit red without a leader! hmm.

		if insideCaller {
			// this is all that <-s.setLeaderCktChan does anyway.
			s.leaderID = ckt.RemotePeerID
			s.leaderName = ckt.RemotePeerName
			s.leaderURL = ckt.RemoteServerURL("")
		} else {
			select {
			case s.setLeaderCktChan <- ckt:
			case <-s.Halt.ReqStop.Chan:
				err = ErrShutDown
				return
			}
		}
	}
	return
}

// client nodes do not get heartbeats anymore,
// so have to be told where to find the leader/cluster
// initially.
func (s *TubeNode) UseLeaderURL(ctx context.Context, leaderURL string) (onlyPossibleAddr string, err error) {
	_, onlyPossibleAddr, _, err = s.getCircuitToLeader(ctx, leaderURL, nil, false)
	return
}

// do inspection, get membership list. used by tup and tests.
// NOTE: we were getting dropped messages complaints
// (from hdr.go) when they got too big (> 1MB) because
// we included all the sesions (and maybe clients?)--so
// exclude those. A dropped reply makes tubels appear as if
// a replica node is down even though it is not. We
// should send on the package but without the payload?
func (s *TubeNode) GetPeerListFrom(ctx context.Context, leaderURL, leaderName string) (mc *MemberConfig, insp *Inspection, actualLeaderURL, actualLeaderName string, onlyPossibleAddr string, ckt *rpc.Circuit, err error) {

	_, _, peerID, _, err := rpc.ParsePeerURL(leaderURL)
	panicOn(err)
	if leaderName == s.name ||
		leaderURL == s.URL ||
		peerID == s.PeerID {

		panic("ugh. self-circuit? TODO figure out self-circuit or what?")
	}

	ckt, onlyPossibleAddr, _, err = s.getCircuitToLeader(ctx, leaderURL, nil, false)

	if err != nil {
		//vv("%v GetPeerListFrom got error from getCircuitToLeader('%v') err='%v'; s.electionTimeoutCh='%p', s.nextElection in '%v'", s.me(), leaderURL, err, s.electionTimeoutCh, time.Until(s.nextElection))
		return nil, nil, "", "", onlyPossibleAddr, nil, err
	}

	//actualLeaderName = ckt.RemotePeerName

	itkt := s.newRemoteInspectionTicket(ckt)
	select {
	case s.requestRemoteInspectCh <- itkt:
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		//vv("%v shutdown...", s.me())
		err = ErrShutDown
		return
	}

	select {
	case <-itkt.readyCh:
		insp = itkt.insp
		mc = itkt.insp.MC
		// members = make(map[string]string)
		// for name, url := range itkt.insp.CktReplicaByName {
		// 	members[name] = url
		// }

		actualLeaderURL = itkt.insp.CurrentLeaderURL
		// correct the leaderName if we got it wrong:
		//vv("%v orig leaderName (ckt.RemotePeerName) = '%v'; itkt.insp.CurrentLeaderName='%v'", s.name, leaderName, itkt.insp.CurrentLeaderName)
		if itkt.insp.CurrentLeaderName != "" {
			actualLeaderName = itkt.insp.CurrentLeaderName
		}
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		//vv("%v shutdown...", s.me())
		err = ErrShutDown
	}

	return
} // end GetPeerListFrom

type inspectionTicket struct {
	insp    *Inspection
	reqFrag *rpc.Fragment
	ckt     *rpc.Circuit
	fcid    string
	readyCh chan struct{}
}

func (s *TubeNode) newRemoteInspectionTicket(ckt *rpc.Circuit) *inspectionTicket {
	frag := s.newFrag()
	frag.FragOp = PeerListReq
	frag.FragSubject = "PeerListReq"
	fcid := rpc.NewCallID("")
	frag.SetUserArg("fragCallID", fcid)

	return &inspectionTicket{
		reqFrag: frag,
		ckt:     ckt,
		fcid:    fcid,
		readyCh: make(chan struct{}),
	}
}

func (s *TubeNode) BaseServerHostPort() (hp string) {
	hp = s.rpcServerAddr.Network() + "://" + s.rpcServerAddr.String()
	//return s.rpcServerAddr.String()
	//vv("hp = '%v'", hp)
	return
}

func (s *TubeNode) AddPeerIDToCluster(ctx context.Context, forceChange, nonVoting bool, targetPeerName, targetPeerID, targetPeerServiceName, baseServerHostPort, leaderURL string, errWriteDur time.Duration) (inspection *Inspection, leaderState *RaftState, err error) {

	const addNotRemove = true
	targetPeerServiceNameVersion := "" // placeholder/versioning of service code
	return s.SingleUpdateClusterMemberConfig(ctx, forceChange, nonVoting, targetPeerName, targetPeerID, targetPeerServiceName, targetPeerServiceNameVersion, baseServerHostPort, leaderURL, addNotRemove, errWriteDur)
}

func (s *TubeNode) RemovePeerIDFromCluster(ctx context.Context, forceChange, nonVoting bool, targetPeerName, targetPeerID, targetPeerServiceName, baseServerHostPort, leaderURL string, errWriteDur time.Duration) (inspection *Inspection, leaderState *RaftState, err error) {

	const addNotRemove = false
	targetPeerServiceNameVersion := ""
	return s.SingleUpdateClusterMemberConfig(ctx, forceChange, nonVoting, targetPeerName, targetPeerID, targetPeerServiceName, targetPeerServiceNameVersion, baseServerHostPort, leaderURL, addNotRemove, errWriteDur)
}

// SingleUpdateClusterMemberConfig is an
// external routine (must not touch state
// directly, communiate with the Start goro
// by channels) to submit a Ticket
// to join (or leave) a cluster. Both we (and the
// cluster) must already be running.
//
// THIS IS THE SINGLE NODE (AT A TIME) CHANGE.
//
// This a part of the setup process always now,
// to ensure it well tested.
//
// results in call to handleLocalModifyMembership inside.
func (s *TubeNode) SingleUpdateClusterMemberConfig(ctx context.Context, forceChange, nonVoting bool, targetPeerName, targetPeerID, targetPeerServiceName, targetPeerServiceNameVersion, baseServerHostPort, leaderURL string, addNotRemove bool, errWriteDur time.Duration) (inspection *Inspection, leaderState *RaftState, err error) {

	//vv("%v top SingleUpdateClusterMemberConfig; leaderURL='%v'", s.me(), leaderURL)

	if targetPeerServiceName != TUBE_REPLICA {
		err = fmt.Errorf("error in SingleUpdateClusterMemberConfig: targetPeerServiceName is not TUBE_REPLICA but '%v'", targetPeerServiceName)
		return
	}

	if strings.Contains(targetPeerID, "/") {
		err = fmt.Errorf("error in SingleUpdateClusterMemberConfig: target is url not peerID: '%v'", targetPeerID)
		panic(err)
		return
	}
	if targetPeerName == "" {
		err = fmt.Errorf("error in SingleUpdateClusterMemberConfig: targetPeerName cannot be empty")
		panic(err)
		return
	}
	// allow without PeerID -- only the name matters
	// so targetPeerID == "" is allowed.

	if leaderURL == "" {
		err = fmt.Errorf("error in SingleUpdateClusterMemberConfig: leaderURL cannot be empty")
		panic(err)
		return
	}

	insp := s.Inspect()
	weAreLeader := (insp.Role == LEADER) &&
		(leaderURL == insp.ResponderPeerURL)
	//vv("%v weAreLeader = '%v'", s.me(), weAreLeader)

	if weAreLeader {
		if targetPeerID == s.PeerID {
			if addNotRemove {
				alwaysPrintf("%v oddness in SingleUpdateClusterMemberConfig(add=%v): we (%v) are already leader in cluster(%v), skipping add of self to self led cluster", s.name, addNotRemove, rpc.AliasDecode(insp.State.PeerID), rpc.AliasDecode(insp.State.ClusterID))
				return
			}
			// else: we are allowed to remove self, even if leader.
		}
	}

	desc := "SingleUpdateClusterMemberConfig() "
	if addNotRemove {
		if nonVoting {
			desc += "ADD NON-VOTING "
		} else {
			desc += "ADD "
		}
	} else {
		if nonVoting {
			desc += "REMOVE NON-VOTING "
		} else {
			desc += "REMOVE "
		}
	}
	desc += fmt.Sprintf("%v", targetPeerName)
	if forceChange {
		desc += " (FORCED)"
	}
	tkt := s.NewTicket(desc, "", "", nil, s.PeerID, s.name, MEMBERSHIP_SET_UPDATE, -1, ctx)
	desc += fmt.Sprintf(", tkt4=%v targetPeerID='%v'", tkt.TicketID[:4], targetPeerID)
	tkt.Desc = desc
	tkt.GuessLeaderURL = leaderURL
	tkt.ForceChangeMC = forceChange

	ctxDeadline, haveCtxDeadline := ctx.Deadline()
	if errWriteDur > 0 {
		tkt.WaitLeaderDeadline = time.Now().Add(errWriteDur)
		if haveCtxDeadline && ctxDeadline.Before(tkt.WaitLeaderDeadline) {
			tkt.WaitLeaderDeadline = ctxDeadline
		}
	} else if haveCtxDeadline {
		tkt.WaitLeaderDeadline = ctxDeadline
	}

	if addNotRemove {
		tkt.AddPeerName = targetPeerName
		tkt.AddPeerID = targetPeerID
		tkt.AddPeerServiceName = targetPeerServiceName
		tkt.AddPeerServiceNameVersion = targetPeerServiceNameVersion
		tkt.AddPeerBaseServerHostPort = baseServerHostPort
	} else {
		tkt.RemovePeerName = targetPeerName
		tkt.RemovePeerID = targetPeerID
		tkt.RemovePeerServiceName = targetPeerServiceName
		tkt.RemovePeerServiceNameVersion = targetPeerServiceNameVersion
		tkt.RemovePeerBaseServerHostPort = baseServerHostPort
	}

	var timeout <-chan time.Time
	if errWriteDur > 0 {
		timeout = time.After(errWriteDur)
	}

	sendCh := s.singleUpdateMembershipReqCh
	if nonVoting {
		if addNotRemove {
			tkt.Op = ADD_SHADOW_NON_VOTING
		} else {
			tkt.Op = REMOVE_SHADOW_NON_VOTING
		}
		sendCh = s.writeReqCh
	}

	//vv("%v SingleUpdateClusterMemberConfig sending on s.singleUpdateMembershipReqCh <- tkt='%v'", s.me(), tkt)
	select {
	case <-timeout:
		err = ErrTimeOut
		return
	case sendCh <- tkt:
		// results in a call to handleLocalModifyMembership,
		// which will redirect to leader if we are not leader.
		//vv("%v sent tkt on s.singleUpdateMembershipReqCh; tkt='%v'", s.me(), tkt.Short()) // 402 seen
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		//vv("%v shutdown...", s.me())
		err = ErrShutDown
		return
	}

	if errWriteDur > 0 {
		timeout = time.After(errWriteDur)
	}
	select { // 402 hung with full grid. prod hung too. tuberm too. tubeadd too.
	case <-tkt.Done.Chan:
		//vv("%v update membership tkt.DoneChan closed; tkt.LogIndex=%v; Desc='%v'; tkt.Err = '%v'", s.me(), tkt.LogIndex, tkt.Desc, tkt.Err) // racy, leave off mostly.
		if tkt.Err != nil {
			err = tkt.Err
		} else if tkt.Insp == nil {
			alwaysPrintf("%v SingleUpdateClusterMemberConfig() got tkt.Done closed but tkt.Insp == nil", s.me())
			panic("tkt.Insp should have been filled in")
		}
		// try to fill in even if tkt.Err, if we can:
		// old:
		if tkt.Insp == nil {
			//vv("%v oh noes. tkt.Insp is nil", s.name)
		} else {
			//vv("%v good: tkt.Insp is NOT nil", s.name) // seen on node_5 in 401
		}
		inspection = tkt.Insp // green 401 expects from leader not follower
		leaderState = tkt.StateSnapshot
	case <-timeout:
		err = ErrTimeOut
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		//vv("%v shutdown...", s.me())
		err = ErrShutDown
	}

	return
}

type fragCkt struct {
	frag *rpc.Fragment
	ckt  *rpc.Circuit
}

// Add server RPC: Note hat
// we (may) have to maintain the set of all
// memberlist config changes separately from the log,
// because the log might compress them away,
// and yet we might still need to know them
// in order to fail back to the prior config, if
// the new one is not committed.
// TODO: just ensure during log compaction that
// the last memberlist is preserved.

// Figure 4.1 Add/Remove server RPC to add one node into the cluster.
//
// 1. If not leader reply "NOT LEADER" [done]. (section 6.2)
//
// 2. (Add new server only): Catch up new server
//    for a fixed number of rounds.
//    Reply TIMEOUT if server does not make progress
//    for an election timeout OR if the last round
//    takes longer than an election timeout (section 4.2.1).
//
// 3. Wait until previous config in log is committed (4.1)
// 4. Append _new_ config to log, commit it using
//    a majority of the _new_ config. (4.1)
// 5. Reply OK to requestor.
// 6. (Remove server only): If we were removed, step down from
//    leader to follower.
//    (section 4.2.2)
// [6 is done now] at tube.go:6381; in commitWhatWeCan().
//
// When to respect membership list:
//
// page 36
// ...each server adopts C_new as soon as that entry
// exists in its log, and the leader knows it's safe
// to allow further configuration changes as soon as
// the C_new entry has been committed. Unfortunately,
// this decision does imply that a log entry for a
// configuration change can be removed (if leadership
// changes); in this case, a server must be prepared
// to fall back to the previous configuration in its log.
//
// [me: done. implemented in
// updateMembershipDueToVaporizedMemConfig()
// at tube.go:9422 which is called after any
// destructive overwriteEntries happens to the wal].
//
// In Raft, it is the caller's configuration that
// is used in reaching consensus, both for voting
// (RequestVote) [who to ask for votes]
// and for log replication (sending AE) [who to send AE to].
//
// a) A server accepts AppendEntries requests from a
// leader that is not part of the server’s latest
// configuration. Otherwise, a new server could
// never be added to the cluster (it would never
// accept any log entries preceding the
// configuration entry that adds the server).
//
// b) A server also grants its vote to a candidate
// that is not part of the server's latest configuration
// (if the candidate has a sufficiently up-to-date
// log and a current term). This vote may occasionally
// be needed to keep the cluster available. For
// example, consider adding a fourth server to a
// three-server cluster. If one server were to fail,
// the new server’s vote would be needed to form a
// majority and elect a leader.
//
// Thus, servers process incoming RPC requests
// without consulting their current configurations.

// handle frag.FragOp == PeerListReq
func (s *TubeNode) peerListRequestHandler(frag *rpc.Fragment, ckt *rpc.Circuit) error {

	//vv("%v top peerListRequestHandler; frag='%v'", s.me(), frag)

	// since we are inside on the main goro, we cannot
	// call Inspect, since we are on the same goro that
	// has to service it (deadlocks), so just call inspectHandler
	// directly.
	insp := newInspection()
	insp.Minimal = true
	s.inspectHandler(insp)

	//vv("%v back from s.Inpsect, insp = '%v'", s.me(), insp)
	if insp == nil {
		// shutting down
		//vv("%v insp was nil, return ErrShutDown", s.name)
		return ErrShutDown
	}

	// trim down the details to keep remote
	// message size not overly large. e.g.
	// we probably don't need the full
	// ticket history. It is marked msg:"-"
	// too, but just in case.
	insp.Tkthist = nil

	frag1 := s.newFrag()
	frag1.FragOp = PeerListReply
	frag1.FragSubject = "PeerListReply"
	fcid, ok := frag.GetUserArg("fragCallID")
	if ok {
		frag1.SetUserArg("fragCallID", fcid)
	}
	bts, err := insp.MarshalMsg(nil)
	panicOn(err)
	frag1.Payload = bts
	if len(bts) > rpc.UserMaxPayload { // 1_200_000
		// I think this happens because we are including
		// all the dead clients from membership attempts
		// whose processes are very short lived! The
		// inspection thus gets too large (3MB).
		fmt.Printf("problem insp is: '%v'", insp.String())
		panicf("problem! our peer list reply (%v) is over maxMessage(%v)", len(bts), rpc.UserMaxPayload)
	}

	frag1.SetUserArg("leader", insp.CurrentLeaderID)
	frag1.SetUserArg("leaderName", insp.CurrentLeaderName)
	frag1.SetUserArg("leaderURL", insp.CurrentLeaderURL)

	var cktP *cktPlus
	if ckt == nil {
		// in peerListRequestHandler here, client could be calling.
		cktP, ok = s.cktall[frag.FromPeerID]
		if !ok {
			//vv("%v don't know how to contact '%v'", s.me(), frag.FromPeerID)
			panic("what??")
		}
		ckt = cktP.ckt
	}
	//vv("%v about to send frag1 = '%v' back on ckt = '%v'", s.name, frag1, ckt)
	err = s.SendOneWay(ckt, frag1, -1, 0)

	_ = err // don't panic on halting or inability to send.
	if err != nil {
		alwaysPrintf("%v non nil error '%v' on PeerListReply to '%v'", s.me(), err, ckt.RemotePeerID)
		// might be:
		// failed to connect to server: 'dial tcp 100.109.45.81:61580: connect: connection refused'
		//return ErrShutDown
	}
	//vv("%v replied with PeerListReply to '%v'", s.name, ckt.RemoteCircuitURL())
	return nil
}

// when the request comes back to us; one we made.
func (s *TubeNode) peerListReplyHandler(frag *rpc.Fragment) error {

	insp := &Inspection{}
	_, err := insp.UnmarshalMsg(frag.Payload)
	panicOn(err)
	//vv("%v see frag.PeerListReplyHandler 1: frag = '%v' -> insp='%v'", s.me(), frag, insp)

	fcid, ok := frag.GetUserArg("fragCallID")
	if !ok {
		panic(fmt.Sprintf("%v fragCallID must be set on the PeerListReply, frag lacks it: '%v'", s.name, frag))
	}
	itkt, ok2 := s.remoteInspectRegistryMap[fcid]
	if !ok2 {
		panic(fmt.Sprintf("%v s.remoteInspectRegistryMap[fcid='%v'] not found, reply without prior request? like bug!", s.name, fcid))
	}
	delete(s.remoteInspectRegistryMap, fcid)
	itkt.insp = insp
	close(itkt.readyCh)
	return nil
}

func (s *TubeNode) LeaderCatchesUpPreReplicaNode() {

}

func (s *TubeNode) showCktall() (r string) {
	i := 0
	r = fmt.Sprintf("s.cktall is len %v:\n", len(s.cktall))
	for peerID, cp := range sorted(s.cktall) {
		r += fmt.Sprintf("   s.cktall[%02d] key(peerID): '%v' (%v) -> val '%#v'\n", i, peerID, rpc.AliasDecode(peerID), cp)
		i++
	}
	return
}

func (s *TubeNode) showCktReplica() (r string) {
	i := 0
	r = fmt.Sprintf("s.cktReplica is len %v:\n", len(s.cktReplica))
	for peerID, cp := range sorted(s.cktReplica) {
		r += fmt.Sprintf("   s.cktReplica[%02d] key(peerID): '%v' (%v) -> val '%#v'\n", i, peerID, rpc.AliasDecode(peerID), cp)
		i++
	}
	return
}

func (s *TubeNode) showMC() (r string) {
	return fmt.Sprintf("s.state.MC is %v", s.state.MC)
}

type PeerDetail struct {
	Name   string `zid:"0"`
	URL    string `zid:"1"`
	PeerID string `zid:"2"`
	Addr   string `zid:"3"`

	PeerServiceName        string `zid:"4"`
	PeerServiceNameVersion string `zid:"5"`

	NonVoting bool   `zid:"6"`
	PID       int64  `zid:"7"`
	Hostname  string `zid:"8"` // for quick reference

	gcAfterHeartbeatCount int
}

func (s *PeerDetail) Clone() *PeerDetail {
	return &PeerDetail{
		Name:                   s.Name,
		URL:                    s.URL,
		PeerID:                 s.PeerID,
		Addr:                   s.Addr,
		PeerServiceName:        s.PeerServiceName,
		PeerServiceNameVersion: s.PeerServiceNameVersion,
		NonVoting:              s.NonVoting,
		PID:                    s.PID,
		Hostname:               s.Hostname,
	}
}

func (s *PeerDetail) String() string {
	return fmt.Sprintf(`PeerDetail{
                  Name: %v
                   URL: %v
                PeerID: %v
                  Addr: %v
       PeerServiceName: %v
PeerServiceNameVersion: %v
             NonVoting: %v
                   PID: %v
              Hostname: %v
}`, s.Name, s.URL, s.PeerID, s.Addr,
		s.PeerServiceName, s.PeerServiceNameVersion,
		s.NonVoting, s.PID, s.Hostname)
}

// MemberConfig gets stored and saved
// to disk in RaftState under
// state.HistoryMemberConfig;
// state.MC;
// state.CommittedMemberConfig.
type MemberConfig struct {
	PeerNames *Omap[string, *PeerDetail] `msg:"-"`

	SerzPeerDetails []*PeerDetail `zid:"0"`

	RaftLogIndex int64 `zid:"1"`

	// 0 if not a bootup, else number of boot nodes.
	BootCount int `zid:"2"`

	// whose view is PeerNames from?
	OriginPeerID                 string `zid:"3"`
	OriginPeerName               string `zid:"4"`
	OriginPeerServiceName        string `zid:"5"`
	OriginPeerServiceNameVersion string `zid:"6"`

	CreateTm  time.Time `zid:"7"`
	CreateWho string    `zid:"8"`

	// provenance: track flow of information/chain of custody
	// to figure out where we are getting it wrong/backwards/flipped.
	Prov []string `zid:"9"`

	// ConfigVersion gives a total order on versions from the
	// MongoRaftReconfig algorithm/paper
	// uses a (version, term) pair where
	// term is compared first. Term is
	// the same as the Raft Term.
	// ConfigVersion is analagous to
	// the raft log index, but only for
	// MC configurations.
	// See VersionGT/E funcs below.
	//
	// The version here is the Version within
	// the term, representing a new configuration.
	//
	// [1] https://arxiv.org/abs/2102.11960
	// "Design and Analysis of a Logless Dynamic
	//  Reconfiguration Protocol"
	// William Schultz, Siyuan Zhou, Ian Dardik, Stavros Tripakis
	// last revised 20 Nov 2021 (this version, v3).
	// [2] https://will62794.github.io/distributed-systems/consensus/2025/08/25/logless-raft.html
	// ConfigVersion is initially 1.
	ConfigVersion int64 `zid:"10"` // logless analog to Raft log index.
	ConfigTerm    int64 `zid:"11"` // same as Raft Term. initially 0.

	// Is every version that the leader
	// broadcasts a committed version? if the
	// ConfigTerm == leader.Term, then we
	// know it is intended to be associated with
	// this term, but we don't know if the
	// leader has actually gotten quorum on
	// it yet.
	// leader sets to let followers know that
	// the MC has been loglessly committed.
	IsCommitted          bool  `zid:"12"`
	CommitIndex          int64 `zid:"13"`
	CommitIndexEntryTerm int64 `zid:"14"`
}

func peerNamesUnion(peerNamesA, peerNamesB *Omap[string, *PeerDetail]) (r *Omap[string, *PeerDetail]) {
	r = NewOmap[string, *PeerDetail]()
	for _, okv := range peerNamesA.Cached() {
		r.Set(okv.key, okv.val)
	}
	for _, okv := range peerNamesB.Cached() {
		v2, ok2 := r.Get2(okv.key)
		if ok2 {
			// okv.key is in both A and B.
			if strings.Contains(okv.val.URL, "pending") &&
				!strings.Contains(v2.URL, "pending") {
				// prefer the non-pending detail
				continue // leave ovk.val as is
			}
		}
		// overwrite any key also in A.
		r.Set(okv.key, okv.val)
	}
	return
}

func (mc *MemberConfig) majority() int {
	tot := mc.PeerNames.Len()
	if tot == 0 {
		return 0
	}
	return (tot / 2) + 1
}

// i and j reflect the notation in the paper.
func (i *MemberConfig) VersionGT(j *MemberConfig) bool {
	if i.ConfigTerm > j.ConfigTerm {
		return true
	}
	if i.ConfigTerm == j.ConfigTerm {
		return i.ConfigVersion > j.ConfigVersion
	}
	return false
}

func (i *MemberConfig) VersionGTE(j *MemberConfig) bool {
	if i.ConfigTerm > j.ConfigTerm {
		return true
	}
	if i.ConfigTerm == j.ConfigTerm {
		return i.ConfigVersion >= j.ConfigVersion
	}
	return false
}

func (i *MemberConfig) VersionEqual(j *MemberConfig) bool {
	return i.ConfigTerm == j.ConfigTerm &&
		i.ConfigVersion == j.ConfigVersion
}

func (i *MemberConfig) TermsEqual(j *MemberConfig) bool {
	return i.ConfigTerm == j.ConfigTerm
}

func (s *MemberConfig) PreSaveHook() {
	s.SerzPeerDetails = s.SerzPeerDetails[:0]
	if s.PeerNames == nil {
		return
	}
	for _, kv := range s.PeerNames.Cached() {
		s.SerzPeerDetails = append(s.SerzPeerDetails, kv.val)
	}
}

func (s *MemberConfig) PostLoadHook() {
	s.PeerNames = NewOmap[string, *PeerDetail]()
	for _, d := range s.SerzPeerDetails {
		s.PeerNames.Set(d.Name, d)
	}
	s.SerzPeerDetails = s.SerzPeerDetails[:0]
}

// give the optional origin of this MemberConfig in s.
func (s *TubeNode) NewMemberConfig(loc string) *MemberConfig {
	r := &MemberConfig{
		PeerNames: NewOmap[string, *PeerDetail](),
		//ConfigVersion: 0,
		//IsCommitted: false,
	}
	if s == nil {
		panic("s cannot be nil")
	}
	if s.wal == nil {
		r.RaftLogIndex = -1
		r.ConfigTerm = 0
	} else {
		idx, term := s.wal.LastLogIndexAndTerm()
		r.RaftLogIndex = idx
		r.ConfigTerm = term
	}
	if s.state != nil {
		r.ConfigTerm = s.state.CurrentTerm
		r.CommitIndex = s.state.CommitIndex
		r.CommitIndexEntryTerm = s.state.CommitIndexEntryTerm
	}

	r.OriginPeerID = s.PeerID
	r.OriginPeerName = s.name
	r.OriginPeerServiceName = s.PeerServiceName
	r.OriginPeerServiceNameVersion = s.PeerServiceNameVersion
	r.addProv(loc, s.name, r.RaftLogIndex)

	r.CreateTm = time.Now()
	r.CreateWho = s.name

	return r
}

func (s *MemberConfig) addProv(loc string, who string, raftLogIndex int64) {
	s.RaftLogIndex = raftLogIndex
	s.OriginPeerName = who
	// since we just updated s.RaftLogIndex/IsCommitted for replicateTicket:
	s.Prov = append(s.Prov, fmt.Sprintf("[idx %02d] %v: %v", raftLogIndex, who, loc))
}

func (s *MemberConfig) check(who *TubeNode) (now time.Time) {
	now = time.Now()
	if s.CreateTm.After(now) {
		panic(fmt.Sprintf("how possible to have update from future? CreateTm: '%v' (%v in future) now=%v? from who='%v' vs me='%v'", nice(s.CreateTm), s.CreateTm.Sub(now), nice(now), s.CreateWho, who.name))
	}
	return
}

func (s *MemberConfig) Len() int {
	return s.PeerNames.Len()
}

// prefer setNameDetail to raw change to s.PeerNames,
// so we always get an updated s.MemberConfigID and
// can know when we need to update on the remote.
func (s *MemberConfig) setNameDetail(name string, detail *PeerDetail, who *TubeNode) {

	// track down bug of auto-cli wrong URL entry
	//if detail.URL == "simnet://auto-cli-from-srv_node_0-to-srv_node_1" {
	//	panic("where?")
	//}
	//if strings.Contains(detail.URL, "auto-cli-from") {
	//	panic("where2?")
	//}

	// sanity check, where is future update from??
	s.check(who)

	s.PeerNames.Set(name, detail)
	// call addProv after if desired; e.g. r.addProv("setNameDetails")
}

func (s *MemberConfig) delName(name string, who *TubeNode) {
	s.PeerNames.Delkey(name)

	// sanity check, where is future update from??
	now := s.check(who)

	s.CreateTm = now
	s.CreateWho = who.name
	// call addProv after if desired; e.g. r.addProv("delName")
}

func (s *MemberConfig) Vers() (r string) {
	return fmt.Sprintf("MC{term:%v, vers:%v}", s.ConfigTerm, s.ConfigVersion)
}

func (s *MemberConfig) String() (r string) {
	return s.Short()
	//return s.LongString()
}
func (s *MemberConfig) LongString() (r string) {
	// cannot have MemberConfigID here b/c is hash of this string.
	r = fmt.Sprintf(`MemberConfig{
    CreateTm: %v,
   CreateWho: %v,
    RaftLogIndex: %v,
     ConfigVersion: %v,
      ConfigTerm: %v,
       BootCount: %v, // isBoot pseudo-config: %v
      [%v]PeerNames:
`, nice(s.CreateTm), s.CreateWho,
		s.RaftLogIndex,
		s.ConfigVersion, s.ConfigTerm,
		s.BootCount, s.BootCount > 0, s.PeerNames.Len())

	for peerName, det := range s.PeerNames.All() {
		r += fmt.Sprintf("%v -> %v\n", peerName, det)
	}
	r += "}\n" + s.ProvString()
	return
}

func (s *MemberConfig) ProvString() (r string) {
	r += fmt.Sprintf("Prov: (len %v)\n", len(s.Prov))
	for k, prov := range s.Prov {
		r += fmt.Sprintf("   [%02d] %v\n", k, prov)
	}
	return
}
func (s *MemberConfig) ContentString() (r string) {
	// cannot have MemberConfigID here b/c is hash of this string.
	// leave out
	//    CreateTm: %v,
	//   CreateWho: %v,
	// so we have a content-defined hash.

	r = fmt.Sprintf(`MemberConfig{
   ConfigTerm: %v,
   ConfigVersion: %v,

    RaftLogIndex: %v,
       BootCount: %v, // isBoot pseudo-config: %v
      [%v]PeerNames:
`, s.ConfigTerm, s.ConfigVersion,
		s.RaftLogIndex,
		s.BootCount, s.BootCount > 0, s.PeerNames.Len())

	for peerName, det := range s.PeerNames.All() {
		r += fmt.Sprintf("%v -> %v\n", peerName, det)
	}
	r += "}\n"
	return
}

func (s *MemberConfig) ShortProv() (r string) {
	return s.short(true)
}
func (s *MemberConfig) Short() (r string) {
	return s.short(false)
}
func (s *MemberConfig) short(withProv bool) (r string) {
	if s == nil {
		return "(nil MemberConfig)"
	}
	com := "_"
	if s.IsCommitted {
		com = "*"
	}
	r = fmt.Sprintf(`[%v term:%v; vers=%v; idx:%v; ci:%v]{`, com, s.ConfigTerm, s.ConfigVersion, s.RaftLogIndex, s.CommitIndex)
	i := 0
	if s.PeerNames != nil {
		for peerName := range s.PeerNames.All() {
			if i > 0 {
				r += ","
			}
			r += peerName
			i++
		}
	}
	r += "}\n"
	if withProv {
		r += s.ProvString()
	}
	return
}

func (s *MemberConfig) CloneForUpdate(who *TubeNode) (r *MemberConfig) {
	r = s.Clone()
	r.CreateTm = time.Now()
	r.CreateWho = who.name
	return
}
func (s *MemberConfig) Clone() (r *MemberConfig) {
	if s == nil {
		return nil
	}
	r = &MemberConfig{}
	*r = *s
	r.PeerNames = NewOmap[string, *PeerDetail]()

	for peerName, det := range s.PeerNames.All() {
		r.PeerNames.Set(peerName, det.Clone())
	}
	r.Prov = nil
	for _, prov := range s.Prov {
		r.Prov = append(r.Prov, prov)
	}
	return
}

// return a-b union b-a
func (a *MemberConfig) Diff(b *MemberConfig) (diff map[string]bool) {
	diff = make(map[string]bool)
	for peerName := range a.PeerNames.All() {
		if _, ok := b.PeerNames.Get2(peerName); !ok {
			diff[peerName] = true
		}
	}
	for peerName := range b.PeerNames.All() {
		if _, ok := a.PeerNames.Get2(peerName); !ok {
			diff[peerName] = true
		}
	}
	return
}

// internal routine,
// called when <-s.singleUpdateMembershipReqCh
//
// only return an error if you want to shut down the peer.
//
// If we are leader we simply call s.changeMembership(tkt);
// if not we forward the request to the leader.
func (s *TubeNode) handleLocalModifyMembership(tkt *Ticket) (onlyPossibleAddr string, sentOnNewCkt bool, ckt *rpc.Circuit, err error) {
	//vv("%v: top handleLocalModifyMembership(tkt='%v')", s.me(), tkt.Short())
	//defer func() {
	//vv("%v: end of handleLocalModifyMembership(tkt='%v')", s.me(), tkt.Short())
	//}()
	switch tkt.Op {
	case MEMBERSHIP_SET_UPDATE, MEMBERSHIP_BOOTSTRAP:
	default:
		panic(fmt.Sprintf("should not call handleLocalModifyMembership with a non-membership update! tkt='%v'", tkt))
	}
	// actually not required that client know the cluster...
	// if tkt.MemberConfig == nil {
	// 	panic(fmt.Sprintf("tkt.MemberConfig must be set on a membership update ticket? tkt='%v'", tkt))
	// }

	var baseServerHostPort string
	_ = baseServerHostPort
	// tubeadd wants to add with just a name, change from AddPeerID to AddPeerName
	if tkt.AddPeerName == "" && tkt.RemovePeerName == "" {
		panic(fmt.Sprintf("MEMBERSHIP_SET_UPDATE must set AddPeerName or RemotePeerName. Neither set in: '%v'", tkt))
	}
	if tkt.AddPeerName != "" && tkt.RemovePeerName != "" {
		panic(fmt.Sprintf("MEMBERSHIP_SET_UPDATE must set ether AddPeerName or RemotePeerName; not both: '%v'", tkt))
	}
	if tkt.AddPeerName != "" {
		if tkt.AddPeerServiceName != TUBE_REPLICA {
			panic(fmt.Sprintf("MEMBERSHIP_SET_UPDATE AddPeerServiceName must be TUBE_REPLICA: tkt='%v'", tkt))
		}
		baseServerHostPort = tkt.AddPeerBaseServerHostPort
	} else {
		if tkt.RemovePeerServiceName != TUBE_REPLICA {
			panic(fmt.Sprintf("MEMBERSHIP_SET_UPDATE RemovePeerServiceName must be TUBE_REPLICA: tkt='%v'", tkt))
		}
		baseServerHostPort = tkt.RemovePeerBaseServerHostPort
	}
	prior, already := s.singleUpdateMembershipRegistryMap[tkt.TicketID]
	if already {
		panic(fmt.Sprintf("conflict! cannot re-use TicketID '%v'/must delete after use. prior='%#v'", tkt.TicketID, prior))
	}
	s.singleUpdateMembershipRegistryMap[tkt.TicketID] = tkt

	if s.role != LEADER {
		var leaderPeerID string
		if tkt.GuessLeaderURL != "" {
			_, _, leaderPeerID, _, err = rpc.ParsePeerURL(tkt.GuessLeaderURL)
			panicOn(err)
			if leaderPeerID == s.PeerID {
				// hmm circuit to self?? but we are not leader.
				// try someone else?
				if s.leaderID != "" && s.leaderID != s.PeerID {
					// try s.leaderID
				} else {
					// error out for now
					tkt.Err = fmt.Errorf("%v: tkt.GuessLeaderURL '%v' pointed to oursevles, but we are not leader.", s.name, tkt.GuessLeaderURL)
					tkt.Stage += ":err_bad_guess_leaderURL"
					s.replyToForwardedTicketWithError(tkt)
					return
				}
			} else {
				// try to let redirectToLeader() below succeed.
				s.leaderID = leaderPeerID
			}
			//vv("%v just set s.leaderID = '%v'", s.me(), s.leaderID)

			cktP, ok := s.cktall[s.leaderID]
			_ = cktP
			if !ok {
				var firstFrag *rpc.Fragment
				firstFrag = s.newFrag()
				firstFrag.FragOp = CircuitSetupHasBaseServerAddr
				firstFrag.FragSubject = "CircuitSetupHasBaseServerAddr"
				firstFrag.SetUserArg("baseServerHostPort", baseServerHostPort)

				// returned now: var ckt *rpc.Circuit
				//vv("%v handleLocalModifyMembership calling getCircuitToLeader", s.me())
				ckt, onlyPossibleAddr, sentOnNewCkt, err = s.getCircuitToLeader(s.MyPeer.Ctx, tkt.GuessLeaderURL, firstFrag, true)

				if err != nil {
					alwaysPrintf("%v don't know how to contact tkt.GuessLeaderURL='%v' to redirect to leader; err='%v'; for tkt '%v'. Assuming they died. s.cktall = '%#v'", s.me(), tkt.GuessLeaderURL, err, tkt, s.cktall)
					//vv("%v could not contact tkt.GuessLeaderURL '%v'; cktall = '%#v'", s.me(), tkt.GuessLeaderURL, s.cktall)

					// immediately return error to caller? tubecli.go
					// does not have re-try loop at the moment.
					tkt.Err = fmt.Errorf("no ckt for leaderURL '%v': err='%v'", tkt.GuessLeaderURL, err)
					tkt.Stage += ":err_no_cktall_for_leader"
					// we could be local, or we could be remote
					// to the original caller, right? this handles
					// both by checking tkt.FromID and either way
					// it closes tkt.Done at the end.
					s.replyToForwardedTicketWithError(tkt)

					return onlyPossibleAddr, sentOnNewCkt, ckt, nil
				}
				//vv("%v good got circuit to leader: '%v'", s.me(), ckt)
				// should not be needed, right? <-s.incomingCkt will make if need be.
				//cktP := s.addToCktall(ckt)
				_ = cktP
				// must set this so that redirectToLeader can succeed now.
				//vv("%v setting s.leaderName: '%v' -> '%v'; and s.leaderID from '%v' -> '%v'; vs our PeerID='%v'", s.name, s.leaderID, ckt.RemotePeerID, s.leaderName, ckt.RemotePeerName, s.PeerID)
				if ckt.RemotePeerID == s.PeerID {
					// this was because in hdr.go we had shifted to
					// var chacha8rand *mathrand2.ChaCha8 = newZeroSeededChaCha8() // now faketime only
					// instead of (now only !faketime:)
					// var chacha8rand *mathrand2.ChaCha8 = newCryrandSeededChaCha8()
					// but! still seen in etc prod runs,
					// so return an error rather than panic.

					//panic(fmt.Sprintf("%v: Wat?! why does this circuit list OUR peerID '%v' as the REMOTE PeerID?? ckt='%v'", s.name, s.PeerID, ckt))

					tkt.Err = fmt.Errorf("no ckt was to self for leaderURL '%v': err='%v'", tkt.GuessLeaderURL, err)
					tkt.Stage += ":err_ckt_was_to_self"
					s.replyToForwardedTicketWithError(tkt)
					return
				}
				s.leaderID = ckt.RemotePeerID
				s.leaderName = ckt.RemotePeerName
				s.leaderURL = ckt.RemoteServerURL("")
			}
			// INVAR: cktP connects to leader
		}
	}
	//vv("%v middle of handleLocalModifyMembership with tkt='%v'", s.me(), tkt.Short())
	if s.redirectToLeader(tkt) {
		//vv("%v handleLocalModifyMembership redirected to leader", s.me())
		if tkt.Err != nil {
			return
		}

		//vv("%v: handleLocalModifyMembership redirected to leader, adding to WaitingAtFollow, tkt='%v'", s.me(), tkt)
		//vv("%v am not leader but '%v'", s.name, s.role)
		s.WaitingAtFollow.set(tkt.TicketID, tkt)
		tkt.Stage += ":handleLocalModifyMembership_WaitingAtFollow"
		return
	}
	// INVAR: we are leader.
	//vv("%v I am leader with request to change membership: tkt='%v'", s.me(), tkt.Short())
	s.changeMembership(tkt)

	// when finished, let regular replication do this, not us!
	// so comment out: tkt.Done.Close().
	// otherwise 403 membership_test prematurely returns and goes red.

	//vv("end of handleLocalModifyMembership(); err = '%v'", err)
	return onlyPossibleAddr, sentOnNewCkt, ckt, nil // non-nil error shuts down the peer.
}

type FirstRaftLogEntryBootstrap struct {
	NewConfig *MemberConfig

	// should we hold an election (reset our
	// leader election timer) after we
	// write the first entry into the raft log?
	// Used by 040 election_test, for example.
	DoElection bool
	Err        string
	Done       chan struct{}
}

func (s *TubeNode) NewFirstRaftLogEntryBootstrap() *FirstRaftLogEntryBootstrap {
	b := &FirstRaftLogEntryBootstrap{
		NewConfig: s.NewMemberConfig("NewFirstRaftLogEntryBootstrap"),
		Done:      make(chan struct{}),
	}
	b.NewConfig.RaftLogIndex = 1
	return b
}

func (s *FirstRaftLogEntryBootstrap) Clone() (r *FirstRaftLogEntryBootstrap) {
	r = &FirstRaftLogEntryBootstrap{
		Err:        s.Err,
		DoElection: s.DoElection,
		NewConfig:  s.NewConfig.Clone(),
		Done:       make(chan struct{}),
	}
	return
}

func (s *TubeNode) setupFirstRaftLogEntryBootstrapLog(boot *FirstRaftLogEntryBootstrap) error {
	//vv("%v top setupFirstRaftLogEntryBootstrapLog()", s.name)
	n := len(s.wal.raftLog)
	if n > 0 {
		panic(fmt.Sprintf("setupFirstRaftLogEntryBootstrapLog() must have empty wal as PRE condition. s.wal.nodisk=%v; s.wal.path: '%v'", s.wal.nodisk, s.wal.path))
	}
	nNode := s.clusterSize()
	if nNode != boot.NewConfig.PeerNames.Len() {
		panic(fmt.Sprintf("setupFirstRaftLogEntryBootstrapLog() have nNode=%v but len(boot.NewConfig.PeerNames) = %v", nNode, boot.NewConfig.PeerNames.Len()))
	}
	if s.state.CurrentTerm != 0 {
		panic(fmt.Sprintf("setupFirstRaftLogEntryBootstrapLog() must see s.state.CurrentTerm == 0, not %v", s.state.CurrentTerm))
	}

	desc := fmt.Sprintf("%v bootstrap cluster leader('%v') with log entry 1 containing %v placeholder boot.blank nodes: '%v'", fileLine(1), s.name, boot.NewConfig.PeerNames.Len(), boot.NewConfig.PeerNames)
	tkt := s.NewTicket(desc, "", "", nil, s.PeerID, s.name, MEMBERSHIP_BOOTSTRAP, -1, s.MyPeer.Ctx)
	tkt.MC = boot.NewConfig.Clone() // here! is setting tkt.MC
	tkt.MC.RaftLogIndex = 1
	//tkt.MC.ConfigVersion = 1

	entry := &RaftLogEntry{
		Tm:                 time.Now(),
		LeaderName:         s.name,
		Term:               1, // s.state.CurrentTerm,
		Index:              1, // idx,
		Ticket:             tkt,
		CurrentCommitIndex: 1, // s.state.CommitIndex,
		node:               s,
	}
	es := []*RaftLogEntry{entry}

	// Use isLeader false even though we are the first leader, as
	// true always panics overwriteEntries; b/c more important
	// to assert about non-bootstrap scenarios in AppendEntries().
	isLeader := false

	var curCommitIndex int64 = 1
	var keepCount int64 = 0
	// in setupFirstRaftLogEntryBootstrapLog() here.
	err := s.wal.overwriteEntries(keepCount, es, isLeader, curCommitIndex, 0, &s.state.CompactionDiscardedLast, s)
	panicOn(err)
	if true { // TODO restore: s.cfg.isTest {
		s.wal.assertConsistentWalAndIndex(s.state.CommitIndex)
	}

	//vv("%v wrote to wal first log entry, txt = '%v'", s.name, tkt)

	s.state.MC = tkt.MC.Clone()

	// avoid (at least leader) to replay the bootstrap
	// entry on startup, back to its old self PeerID, which
	// will always fail of course.
	// We will have everyone notice MEMBERSHIP_BOOTSTRAP
	// and not try to reply to that client too.
	s.state.LastApplied = 1
	s.state.LastAppliedTerm = 1
	s.saver.save(s.state)
	s.wroteBootstrapFirstLogEntry = time.Now()

	close(boot.Done)
	return nil
}

// called by handleLocalModifyMembership() on leader,
// our main membership config change routine above.
// Also called by commandSpecificLocalActionsThenReplicateTicket()
// during remote submit or resubmit.
// Our watchdog's seen() also calls it.
//
// Any errors should be set on tkt.Err.
// We'll ignore any tkt that have finishTicketCalled already true.
// Any new config installed (and not stalled) will start
// with IsCommitted false.
func (s *TubeNode) changeMembership(tkt *Ticket) {
	//vv("%v top of changeMembership(); tkt.Desc='%v'", s.me(), tkt.Desc)

	if tkt.finishTicketCalled {
		//vv("%v tkt.finishTicketCalled so exit changeMembership early; tkt.Desc='%v'", s.me(), tkt.Desc)
		return
	}

	switch tkt.Op {
	case MEMBERSHIP_SET_UPDATE, MEMBERSHIP_BOOTSTRAP:
	default:
		panic(fmt.Sprintf("should not call changeMembership with a non-membership update! tkt='%v'", tkt))
	}

	if s.redirectToLeader(tkt) {
		if tkt.Err != nil {
			//vv("%v bail after redirectToLeader error in changeMembership(); tkt.Desc='%v' tkt.Err='%v'", s.me(), tkt.Desc, tkt.Err)
			return // bail out, error happened.
		}
		//vv("%v: changeMembership redirected to leader, adding to Waiting, writeReqCh: '%v'", s.me(), tkt)
		//vv("%v am not leader but '%v'", s.name, s.role)
		s.WaitingAtFollow.set(tkt.TicketID, tkt)
		tkt.Stage += ":changeMembership_WaitingAtFollow"
		return
	}
	// INVAR: we are leader.
	if s.role != LEADER {
		panic("changeMembership should only be called on leader")
	}
	//vv("%v: changeMembership top (we are leader). tkt='%v'", s.me(), tkt.Short())

	// too early to do an Inspection! ticket is
	// not committed yet so we get stale MC
	// info in the Inspection; makes 403 red if we set
	// it in a defer here. (so don't!)

	// mongo not stalling made for alot of logical races and pre-maturely
	// failed membershipChanges. Much nicer to stall them too. (402 for instance).

	// Figure 4.1
	// 3. Wait until previous config in log is committed.

	// do we ourselves have pending membership change just in memory?
	havePriorChange := len(s.stalledMembershipConfigChangeTkt) > 0
	if havePriorChange {

		first := s.stalledMembershipConfigChangeTkt[0]
		if tkt == first {
			// resubmit of stalled ticket by cktPlus.seen()
			s.stalledMembershipConfigChangeTkt = s.stalledMembershipConfigChangeTkt[1:]
			havePriorChange = false // don't de-queue below.
		} else {
			//vv("%v overload! must stall this 2nd (or greater) config change. tkt has '%v'", s.me(), tkt.Short())
			tkt.Stage += ":stalledAsHavePreviousConfigChangeUncommitted"
			//tkt.localHistIndex = len(s.tkthist)
			//s.tkthist = append(s.tkthist, tkt)
			//s.tkthistQ.add(tkt)

			// en-queue at end
			s.stalledMembershipConfigChangeTkt =
				append(s.stalledMembershipConfigChangeTkt, tkt)
		}
	}
	//vv("%v do first in/first out for our memory. havePriorChange=%v", s.name, havePriorChange)
	if havePriorChange {

		//vv("%v changeMembership(): havePriorChange=true, so swapping in old for latest, to handle membership changes first-in-first-applied. old stalled tkt we will apply now = '%v'; latest tkt we must stall = '%v'", s.me(), s.stalledMembershipConfigChangeTkt[0].TicketID, tkt.TicketID)

		// de-queue from front
		tkt = s.stalledMembershipConfigChangeTkt[0]
		s.stalledMembershipConfigChangeTkt = s.stalledMembershipConfigChangeTkt[1:]
		// execute membership changes first-come, first-served.
	}
	// INVAR: okay to replicate tkt, compute its membership change:

	if s.state.MC == nil {
		s.state.MC = s.NewMemberConfig("changeMembership")
		// starts with IsCommitted == false
	} else {
		if false { // now we allow one-step transfer from shadow to MC.
			// keep MC and ShadowReplicas disjoint, so reject
			// an addition of regular replica to MC
			// if peer is already a shadow.
			if tkt.AddPeerName != "" &&
				s.state.ShadowReplicas != nil &&
				s.state.ShadowReplicas.PeerNames != nil {
				// only MEMBERSHIP_SET_UPDATE or MEMBERSHIP_BOOTSTRAP here
				// (never ADD_SHADOW_NON_VOTING/REMOVE_SHADOW_NON_VOTING)
				_, alreadyShadow := s.state.ShadowReplicas.PeerNames.Get2(tkt.AddPeerName)
				if alreadyShadow {
					tkt.Err = fmt.Errorf("'%v' is already a shadow replica, cannot add as regular regular peer, as these sets must be disjoint. rejecting '%v'; error at leader '%v' in changeMembership().", tkt.AddPeerName, tkt.AddPeerName, s.name)
					s.respondToClientTicketApplied(tkt)
					s.FinishTicket(tkt, true)
					//vv("%v changeMembership alreadyShadow so tkt.Err='%v'", s.me(), tkt.Err)
					return
				}
			}
		} // end if false
	}
	curConfig := s.state.MC

	// our Rule: only increment the ConfigVersion when
	// the set of _names_ has changed. The details of
	// how to communicate with that name are fluid and
	// can be changed without getting a new version
	// (as circuits die and are re-established).
	// In fact we actively try below to give the most up to date
	// contact info before we adopt and broadcast
	// the newConfig.
	newConfig := curConfig.CloneForUpdate(s)
	newConfig.ConfigVersion++
	newConfig.IsCommitted = false

	// addProv does the equivalent of these for us:
	// newConfig.RaftLogIndex = s.lastLogIndex()
	// newConfig.IsCommitted = false
	// newConfig.OriginPeerName = s.name
	newConfig.addProv(fmt.Sprintf("changeMembership tkt={%v}", tkt.addRmString()), s.name, s.lastLogIndex())

	var mongoNeedSeen []*cktPlus
	// update the locations/peerID of
	// members if we can, we adjust the newConfig for that.
	// and at the same time, we
	// calculate inputs for the MongoRaftReconfig pre-conditions,
	// and stall the tkt if we are not ready for it.

	// This was assuming the leader is always in the
	// current MC. However, pages 39-40, section 4.2.2
	// of the Raft dissertation and especially Fig 4.6 on
	// "Removing the current leader" point out that
	// this may not always be the case to keep liveness:
	//
	// "[T]he leader waits until Cnew is committed
	// to step down. This is the first point when the
	// new configuration can definitely operate without
	// participation from the removed leader: it will
	// always be possible for the members of Cnew to
	// elect a new leader from among themselves. After
	// the removed leader steps down, a server in Cnew
	// will time out and win an election. This small
	// availability gap should be tolerable, since
	// similar availability gaps arise when leaders fail.
	//
	// page 40 continues: "This approach
	// leads to two implications about
	// decision-making that are not particularly harmful
	// but may be surprising. First, there will be a
	// period of time (while it is committing Cnew) when
	// a leader can manage a cluster that does not
	// include itself;
	//
	// "*** it replicates log entries but does not
	// count itself in majorities. ***
	//
	// "Second, a server that is not part of its own
	// latest configuration should still start new
	// elections, as it might still be needed until
	// the Cnew entry is committed (as in Figure 4.6).
	//
	// "*** It does not count its own vote in elections
	// unless it is part of its latest configuration. ***"
	//
	// [me: wow! suprising indeed!]

	inCurrentConfigCount := 1 // for self (note we are never in cktAllByName)
	inCurTermCount := 1       // for self (we always have our current MC).
	if !s.weAreMemberOfCurrentMC() {
		// per section 4.2.2, do not count ourselves if
		// we are being removed, even though we are leader.
		inCurrentConfigCount = 0
		inCurTermCount = 0
	}
	curTerm := s.state.CurrentTerm
	for peerName, detail := range curConfig.PeerNames.All() {
		_ = detail
		cktP0, ok := s.cktAllByName[peerName]
		if ok && cktP0.ckt != nil {
			if peerName == s.name {
				// sanity check our inCurrentConfigCount logic.
				panic("impossible, we never add self to cktAllByName")
			}
			needStall := false
			if cktP0.MC == nil {
				needStall = true
				//vv("%v mongo warning: no MC available (so we cannot add to inCurrentConfigCount) for cktP0.PeerName='%v', looking for curConfig='%v'; cktP0=%p; stalling tkt='%v'", s.me(), cktP0.PeerName, curConfig.Short(), cktP0, tkt.Short())

			} else {
				if cktP0.MC.VersionEqual(curConfig) {
					inCurrentConfigCount++
				} else {
					//vv("%v mongo ignore for quorum check peer is out of date: '%v' (%v) vs ours MC='%v'. stalling tkt='%v'", s.me(), peerName, cktP0.MC.Short(), curConfig.Short(), tkt.Short())
					// I think this means we need to stall to let this peer
					// get the latest config on the next AE heartbeat. yes: 403 green mongo.
					// Arg: this probably will stop us from
					// executing an MC change if one member is down.
					// So we probably need to not stall unless
					// we really have lack a quorum.

					// only stall on mongoLeaderCanReconfig() error below.
					mongoNeedSeen = append(mongoNeedSeen, cktP0)
				}
			}
			if needStall {
				s.stalledMembershipConfigChangeTkt = append(s.stalledMembershipConfigChangeTkt, tkt)
				//vv("%v mongo stall in changeMembership, so return nil without closing tkt.Done. Ticket has '%v'", s.me(), tkt.Short())
				cktP0.stalledOnSeenTkt = append(cktP0.stalledOnSeenTkt, tkt)
				return
			}
			if cktP0.CurrentTerm == curTerm {
				inCurTermCount++
			}
			ckt := cktP0.ckt
			det := &PeerDetail{
				Name:                   peerName,
				PeerID:                 cktP0.PeerID,
				PeerServiceName:        ckt.RemoteServiceName,
				PeerServiceNameVersion: ckt.RemotePeerServiceNameVersion,
				NonVoting:              detail.NonVoting,
			}
			s.setAddrURL(det, cktP0)
			newConfig.setNameDetail(peerName, det, s)
			s.state.Known.PeerNames.Set(peerName, det)
		}
	}
	if s.weAreMemberOfCurrentMC() {
		//vv("weAreMemberOfCurrentMC true: %v", s.name)
		// update/add self
		det := &PeerDetail{
			Name:   s.name,
			URL:    s.URL,
			PeerID: s.PeerID,
			//Addr:                   s.MyPeer.NetAddr,
			Addr:                   s.MyPeer.BaseServerAddr,
			PeerServiceName:        s.MyPeer.PeerServiceName,
			PeerServiceNameVersion: s.MyPeer.PeerServiceNameVersion,
			//NonVoting:              s.NonVoting,
		}
		newConfig.setNameDetail(s.name, det, s)
		s.state.Known.PeerNames.Set(s.name, det)
	}

	// DONE: we have updated newConfig set of curConfig members
	// to the latest available Addr/PeerID/URL, but have
	// not added or removed the change target peer.

	switch {
	case tkt.AddPeerName != "":

		_, already := curConfig.PeerNames.Get2(tkt.AddPeerName)
		if already {
			break // just proceed below

			tkt.Err = fmt.Errorf("changeMembership error: nothing to do, as AddPeerName '%v'/'%v' is already in curConfig: '%v'", tkt.AddPeerName, tkt.AddPeerID, curConfig)
			//vv("%v changeMembership returning error early: tkt.Err = '%v'", s.me(), tkt.Err)
			s.replyToForwardedTicketWithError(tkt)
			return
		}

		cktP, ok := s.cktAllByName[tkt.AddPeerName]
		if ok && cktP.ckt != nil {
			peerID := tkt.AddPeerID
			if peerID == "" {
				peerID = cktP.PeerID
			} else {
				if cktP.PeerID != "" && tkt.AddPeerID != cktP.PeerID {
					panic(fmt.Sprintf("difference in opinion about peerID for '%v'; which is correct? cktP.PeerID='%v', but tkt.AddPeerID = '%v'. is one of them stale?", tkt.AddPeerName, cktP.PeerID, tkt.AddPeerID))
				}
			}
			det := &PeerDetail{
				Name:                   tkt.AddPeerName,
				PeerID:                 peerID,
				PeerServiceName:        cktP.ckt.RemoteServiceName,
				PeerServiceNameVersion: cktP.ckt.RemotePeerServiceNameVersion,
			}
			s.setAddrURL(det, cktP)
			newConfig.setNameDetail(tkt.AddPeerName, det, s)
			s.state.Known.PeerNames.Set(tkt.AddPeerName, det)

		} else {

			target := tkt.AddPeerName
			// can be adding self back from shadow to MC
			if target == s.name {
				// update/add self
				det := &PeerDetail{
					Name:   s.name,
					URL:    s.URL,
					PeerID: s.PeerID,
					//Addr:                   s.MyPeer.NetAddr,
					Addr:                   s.MyPeer.BaseServerAddr,
					PeerServiceName:        s.MyPeer.PeerServiceName,
					PeerServiceNameVersion: s.MyPeer.PeerServiceNameVersion,
					//NonVoting:              s.NonVoting,
				}
				newConfig.setNameDetail(s.name, det, s)
				s.state.Known.PeerNames.Set(s.name, det)
			} else {
				//vv("%v no existing connection to tkt.AddPeerName='%v', try to spin one up in background", s.me(), target)
				cktP := s.newCktPlus(target, TUBE_REPLICA)
				cktP.PeerBaseServerAddr = tkt.AddPeerBaseServerHostPort
				vv("%v calling startWatchdog in changeMembership on cktP=%p for '%v'", s.me(), cktP, cktP.PeerName)
				cktP.startWatchdog()

				det := &PeerDetail{
					Name: target,
					URL:  "pending",
				}
				newConfig.setNameDetail(target, det, s)

				// INVAR: target != s.name, from above.
				s.cktAllByName[target] = cktP
				// should s.cktall get it too? for now, no. We
				// only put pending into cktAllByName and set cktall
				// once an actual circuit arrives.

				s.connectInBackgroundIfNoCircuitTo(tkt.AddPeerName, "changeMembership") // call 2
				//tkt.Err = fmt.Errorf("changeMembership error: AddPeerName '%v' not found in cktall: '%v'", tkt.AddPeerName, s.cktall)

			}
		}
	case tkt.RemovePeerName != "":
		gonerDetail, already := curConfig.PeerNames.Get2(tkt.RemovePeerName)
		if !already {
			alwaysPrintf("%v remove peer '%v' is a noop since it is already gone", s.me(), tkt.RemovePeerName)

			s.respondToClientTicketApplied(tkt)
			s.FinishTicket(tkt, true)
			return
			break
			// just proceed below

			tkt.Err = fmt.Errorf("changeMembership error: nothing to do, as RemovePeerID '%v' is already NOT in curConfig: '%v'", tkt.RemovePeerName, curConfig)
			//panic(tkt.Err)
			//vv("%v error: removing peer not in membership: '%v'", s.me(), tkt.RemovePeerName)
			break
		}
		// INVAR: already == true, gonerDetail != nil.
		const removedNodesBecomeShadows = true
		if removedNodesBecomeShadows {
			// the s.setMC() below will add to ShadowReplicas,
			// in preference to being a temporary observer;
			// once all the other checks below have passed.
		} else {
			gonerDetail.gcAfterHeartbeatCount = 3
			s.state.Observers.PeerNames.Set(gonerDetail.Name, gonerDetail)
		}
		newConfig.delName(tkt.RemovePeerName, s)

		// don't kill our connection to the departed,
		// or we will won't be able to
		// tell them about their exclusion from the new membership!
	default:
		panic(fmt.Sprintf("one of AddPeerName or RemovePeerName must have been set. bad tkt = '%v'", tkt))
	}
	if tkt.Err != nil {
		//vv("%v erroring out in changeMembership b/c tkt.Err='%v'", s.me(), tkt.Err)
		s.replyToForwardedTicketWithError(tkt)
		return
	}
	// INVAR: new config differs from cur config by
	// exactly one (or zero; no change in) peers.

	//vv("%v good, in changeMembership, cur config = '%v'", s.me(), curConfig)
	//vv("vs new config = '%v'", newConfig)

	// double check that
	memDiff := s.membershipDiffOldNew(curConfig, newConfig)
	numDiffs := len(memDiff)
	_ = numDiffs
	//vv("cur vs new config; diff='%#v'\n\n newConfig = \n'%v'", memDiff, newConfig)
	tkt.MC = newConfig
	//vv("%v we set (%p)tkt.MemberConfig = newConfig(%p)", s.me(), tkt, newConfig)

	// too long list of repeats... every time we try it gets longer!
	//tkt.Desc += fmt.Sprintf("; (new)tkt.MC = '%v'", tkt.MC.Short())

	// mongo-raft-reconfig safety checks that current config has
	// been "loglessly committed", which is the equivalent of
	// Raft-log-committed in the cluster (on a quorum). This
	// is based on the MC from AE acks and pre/vote tallies
	// as submitted via cktPlus.seen(mc) when we see those messages.

	// Although we cache in s.state.MC.IsCommitted,
	// we don't want to skip the quorum overlap
	// check between the existing and the new,
	// so we don't use that cache here.

	err := s.mongoLeaderCanReconfig(curConfig, newConfig, inCurrentConfigCount, inCurTermCount)

	if err != nil && !tkt.ForceChangeMC {
		//vv("%v mongoLeaderCanReconfig gave err = '%v'", s.me(), err)

		if time.Since(tkt.T0) > 10*time.Second {
			tkt.Err = fmt.Errorf("%v timeout MC change attempts after 10 seconds; error from mongoLeaderCanReconfig: %v", s.me(), err)
			s.replyToForwardedTicketWithError(tkt)
			return
		}
		s.stalledMembershipConfigChangeTkt = append(s.stalledMembershipConfigChangeTkt, tkt)
		//vv("%v mongo stall in changeMembership due to err='%v' tkt='%v'", s.me(), err, tkt.Short())
		for _, cktP0 := range mongoNeedSeen {
			cktP0.stalledOnSeenTkt = append(cktP0.stalledOnSeenTkt, tkt)
		}
		return
	}
	if err == nil {
		tkt.Stage += ":mongo_logless_commit_observed_in_changeMembership"
		// the "logless commit" of the existing has been observed:
		// a quorum of servers meet the RECONFIG
		// conditions Q1,Q2,P1 of Algorithm 1 (page 6)
		// of the Mongo paper; any older configs have been
		// deactivated and a new/other leader cannot revive them.
		//vv("%v mongo logless commit observed", s.me())

		// We could do this to document the fact... but they are
		// about to get replaced by newConfig which is not committed anyway.
		//curConfig.IsCommitted = true
		//s.state.MC.IsCommitted = true
	}
	if err != nil && tkt.ForceChangeMC {
		alwaysPrintf("%v: -f force change is overriding mongo safe commit rules, thus ignoring: '%v'", s.me(), err)
		tkt.Stage += ":force_change_overrides_mongo_logless_commit"
	}

	// inside changeMembership
	// side effect: removal from MC will add to ShadowReplicas
	s.setMC(newConfig, fmt.Sprintf("changeMembership to newConfig '%v'", newConfig.Short()))

	// WTF why full snapshot on each member change???
	//tkt.StateSnapshot = s.getStateSnapshot()

	if tkt.Insp == nil {
		//vv("%v changeMembership mongo logless commit adding tkt.Insp", s.me())
		s.addInspectionToTicket(tkt)
	}
	// there is no replicateTicket needed for
	// mongo style dynamic reconfig, because we
	// don't depend on the wal/oplog and we don't
	// start the configuration change until
	// the current config is already committed
	// and all old configs are deactivated.

	// this does s.FinishTicket(tkt) only if WaitingAtLeader
	s.respondToClientTicketApplied(tkt)
	//vv("%v mongo changeMembership done, back from respondToClientTicketApplied with tkt='%v'", s.me(), tkt)
	s.FinishTicket(tkt, true)
	//vv("%v mongo FinishTicket done for tkt4=%v", s.me(), tkt.TicketID[:4])

	// 4. Append _new_ config to log, commit it using
	//    a majority of the _new_ config.
	// 5. Reply OK to requestor.
	// 6. If we were removed, step down from
	//    leader to follower. (section 4.2.2)
	//s.lastLeaderActiveStepDown = now
	//s.becomeFollower(s.state.CurrentTerm, nil, SAVE_STATE)
}

func (a *MemberConfig) SamePeers(b *MemberConfig) bool {
	na := a.PeerNames.Len()
	nb := b.PeerNames.Len()
	if na != nb {
		return false
	}
	for p := range a.PeerNames.All() {
		_, ok := b.PeerNames.Get2(p)
		if !ok {
			return false
		}
	}
	return true
}

// tkt has Err set. At end close the tkt.Done
// can/should we replace use of this with
// respondToClientTicketApplied(tkt)?
func (s *TubeNode) replyToForwardedTicketWithError(tkt *Ticket) {
	if tkt.Done != nil {
		defer tkt.Done.Close()
	}

	if s.PeerID == tkt.FromID {
		//vv("%v leader local changeMembership tkt had err ('%v'), returning now. tkt= '%v'", s.name, tkt.Err, tkt)
		return
	}
	// INVAR: forwarded ticket, forward back the error.

	cktP, ok := s.cktall[tkt.FromID]
	if !ok {
		//vv("%v don't know how to contact '%v' to return error on changeMembership; for tkt '%v'. Assuming they died. s.cktall = '%#v'", s.me(), tkt.FromID, tkt, s.cktall)
		return
	}

	s.addInspectionToTicket(tkt)

	fragErr := s.newFrag()
	fragErr.FragOp = LeaderToClientTicketAppliedMsg
	fragErr.FragSubject = "LeaderToClientTicketAppliedMsg"
	bts, err := tkt.MarshalMsg(nil)
	panicOn(err)
	fragErr.Payload = bts

	fragErr.SetUserArg("leader", s.leaderID)
	fragErr.SetUserArg("leaderName", s.leaderName)
	err = s.SendOneWay(cktP.ckt, fragErr, -1, 0)
	_ = err // don't panic on halting.
	if err != nil {
		alwaysPrintf("%v non nil error '%v' on LeaderToClientTicketAppliedMsg to '%v'", s.me(), err, cktP.ckt.RemotePeerID)
	}
}

// calls s.adjustCktReplicaForNewMembership() at the end.
func (s *TubeNode) setMC(members *MemberConfig, caller string) (amInLatest, ignoredMemberConfigBecauseSuperceded bool) {

	defer func() {
		if s != nil && s.state != nil && s.state.MC != nil {
			_, amInLatest = s.state.MC.PeerNames.Get2(s.name)
		}
	}()

	if members == nil {
		panic("members cannot be nil")
	}
	if members.RaftLogIndex == 0 {
		// the log may just legit be empty. Don't freak out.
	}
	if s.state.CommitIndex > 0 && s.state.CommitIndex > members.RaftLogIndex {
		members.RaftLogIndex = s.state.CommitIndex
	}

	// are we plyaing back the log but have newer config
	// already from AE giving us the latest?
	if s.state.MC != nil &&
		s.state.MC.VersionGTE(members) {
		//s.state.MC.RaftLogIndex > members.RaftLogIndex {
		//vv("%v ignoring replay of old membership change: '%v', as we already have: '%v'", s.me(), members.Short(), s.state.MC.Short())
		ignoredMemberConfigBecauseSuperceded = true
		return
	}

	//vv("%v setMC() about to set s.state.MC, going from\n '%v'\n ->\n %v", s.me(), s.state.MC.Short(), members.Short())

	upd := members.CloneForUpdate(s)
	descprov := fmt.Sprintf("setMC: %v", caller)
	upd.addProv(descprov, s.name, upd.RaftLogIndex)

	// put removed followers into shadows automatically?
	if s.state.MC != nil {
		s.putRemovedReplicasIntoShadows(members)
	}

	s.state.MC = upd // in setMC itself, do not call setMC recursviely.
	s.saver.save(s.state)

	s.adjustCktReplicaForNewMembership()

	//vv("%v after adjustCktReplicaForNewMembership(); after setting s.state.MC = members(%v); caller='%v'", s.me(), members.Short(), fileLine(2)) // 403 only seen on node_0 leader
	return
}

// and transfer out of shadows if adding to MC.
// problem is: these don't get replicated to other
// nodes, so these loose the shadow list if they take over.
func (s *TubeNode) putRemovedReplicasIntoShadows(newMC *MemberConfig) {
	if s.state == nil || s.state.MC == nil {
		return
	}
	diff := s.membershipDiffOldNew(s.state.MC, newMC)
	for name, action := range diff {
		if action == "removed" {
			// use the detail from s.state.MC for shadows
			detail, ok := s.state.MC.PeerNames.Get2(name)
			if ok {
				s.state.ShadowReplicas.PeerNames.Set(name, detail)
				s.clearFromObservers(name)
			} else {
				panicf("should be impossible; why no detail if we just saw the name in diff='%v'", name)
			}
		} else {
			// adding name to MC, so take out of Shadows automatically.
			s.state.ShadowReplicas.PeerNames.Delkey(name)
		}
	}
}

func showCktsDiff(diff map[string]string) (r string) {
	if len(diff) == 0 {
		return
	}
	r = "map["
	for k, v := range diff {
		r += fmt.Sprintf("%v:%v, ", k, alias(v))
	}
	return
}

func cktsDiff(update, orig map[string]*cktPlus) (diff map[string]string) {
	diff = make(map[string]string)
	for p := range update {
		_, ok := orig[p]
		if !ok {
			diff[p] = "added"
		}
	}
	for p := range orig {
		_, ok := update[p]
		if !ok {
			diff[p] = "removed"
		}
	}
	return
}

// ===================
// external state; service
// for hermes: cluster membership
// of the hermes cluster. The
// state being maintained by
// the tube (Raft) mechanism.
// ===================
type ExternalCluster struct {
	Member *Omap[string, string] `msg:"-"`

	// nil almost always; only used
	// briefly during serialization to/from disk.
	Serz map[string]string `zid:"0"`
}

func NewExternalCluster(m map[string]string) (r *ExternalCluster) {
	r = &ExternalCluster{
		Member: NewOmap[string, string](),
	}
	for k, v := range m {
		r.Member.Set(k, v)
	}
	return
}

func (s *ExternalCluster) String() (r string) {
	r = "ExternalCluster{\n"
	i := 0
	for k, v := range s.Member.All() {
		r += fmt.Sprintf(` %02d Member[ "%v" ]: "%v"
`, i, k, v)
		i++
	}
	r += "}"
	return
}

func (s *ExternalCluster) PreSaveHook() {
	if s == nil || s.Member == nil {
		// empty set; happens in auto gen_test
		return
	}
	s.Serz = make(map[string]string)
	for _, kv := range s.Member.Cached() {
		s.Serz[kv.key] = kv.val
	}
}

func (s *ExternalCluster) PostLoadHook() {
	s.Member = NewOmap[string, string]()
	for k, v := range s.Serz {
		s.Member.Set(k, v)
	}
	s.Serz = nil
}

func (s *ExternalCluster) SerzBytes() []byte {
	bts, err := s.MarshalMsg(nil)
	panicOn(err)
	return bts
}

func showExternalCluster(by []byte) (r string) {
	if !bytes.Contains(by, []byte("_zid00_")) {
		return string(by)
	}
	v := &ExternalCluster{}
	_, err := v.UnmarshalMsg(by)
	if err != nil {
		// likely not ExternalCluster
		return string(by)
	}
	return v.String()
}

// # Log Compaction and Configuration History
//
// This is another excellent question, and the
// answer again makes the simpler algorithm more
// manageable. You absolutely **_do not_** need
// to keep the full history of member
// configuration changes in your log.
//
// The key is that the cluster's configuration is part
// of the server's **state machine**. When you perform
// log compaction, you create a snapshot that includes
// two key pieces of information:
//
// 1. The complete state of the user's data (the
// state machine).
// 2. The last cluster configuration committed at
// or before the snapshot's log index.
//
// Let's say your log has a history of configurations:
// $C_1$, $C_2$, $C_3$, and $C_4$. $C_4$ was
// committed at log index 8000.
//
// When your server performs a log compaction and
// creates a snapshot at index 8500, that snapshot file
// will contain your application's data *and* the fact
// that the active configuration is $C_4$.
//
// Once that snapshot is saved, the server can safely
// discard all log entries before index 8500,
// _including_ the old configuration entries
// for $C_1$, $C_2$, and $C_3$. The snapshot itself
// now serves as the historical record,
// establishing $C_4$ as the baseline
// configuration for anyone who needs to
// install it.
//
// BUT! PAR (at first glance) MAKES THIS UNTENABLE
// (See below for a solution, doing PAR during Pre-Vote).
//
// Q: My problem with this is trying to
// implement Protocol Aware Recovery (PAR)
// on top of Raft. The PAR paper points out
// that once we have a realistic storage
// fault model, it no longer is safe for
// the leader to assume it can replicate
// its log, as that log might be damaged
// and so would propagate the damage rather
// than repair it. Can the simpler reconfig
// algorithm still be saved?
//
// Gemini 2.5 pro answer:
//
// A: No, the simpler reconfiguration algorithm
// cannot be saved in its original form when
// faced with the realistic storage fault
// model introduced by Protocol-Aware
// Recovery (PAR).
//
// The algorithm's core mechanics are
// fundamentally incompatible with a system
// where the leader's log cannot be trusted.
// Integrating PAR's principles to make it
// safe would alter it so significantly
// that it would no longer be "simple."
//
// ### The Fundamental Conflict: Leader
// Authority vs. Storage Faults
//
// The conflict is a direct clash between
// the foundational assumptions of each protocol.

// * **Raft's Simple Algorithm** operates on
// the principle of **absolute leader authority**.
// The leader's log is the single source of
// truth. If a follower's log diverges, the
// leader force-replicates its own log to
// "correct" the follower. This is the mechanism
// that handles everything, including
// configuration rollbacks.
//
// * **Protocol-Aware Recovery (PAR)** operates
// on the principle of **leader self-doubt**.
// It recognizes that realistic storage faults
// (bit rot, torn writes, phantom reads) mean
// the leader's log could be corrupt. Blindly
// replicating a leader's log could propagate
// corruption rather than fix it.
//
// The simple reconfiguration algorithm's
// elegance comes entirely from the assumption
// that the leader is infallible. Once you
// remove that assumption, the algorithm
// becomes dangerously unsafe.
//
// ### How PAR Breaks the Simple Algorithm's Mechanics
//
// Introducing the possibility of a corrupt
// leader log breaks the simple algorithm in
// several critical ways:
//
// 1. **Propagating Corrupt Configurations:** If
// a leader has a damaged log entry for a new
// configuration, $C_{new}$, it will replicate
// this corrupted data to its followers. The
// cluster could unknowingly commit to an
// invalid membership list, leading to
// catastrophic failures.
//
// 2.  **Unsafe Rollbacks:** This is the most
// dangerous failure. Imagine a valid,
// uncommitted $C_{new}$ exists on a majority
// of servers. A new leader is elected, but
// its own log is damaged and is *missing* this
// $C_{new}$ entry. Believing its own log
// is correct, it will incorrectly force
// all followers to roll back the valid
// configuration change, moving the cluster
// to a stale state based on corrupted information.
//
// 3. **Invalid Snapshots:** As we discussed,
// a log snapshot must contain the latest
// configuration. If this part of the snapshot
// file is corrupted on the leader, it will
// send this poison pill to any new or slow
// follower, instantly corrupting their
// view of the cluster's membership.
//
// ### What "Saving" the Algorithm Would Look Like
//
// To make the algorithm safe in a PAR world,
// you would have to abandon its simplicity
// and integrate cross-server validation
// at its core. This would fundamentally change it.
//
// **Checksums and Validation:** Every log entry,
// especially a critical configuration entry,
// would need a checksum. Followers wouldn't
// just acknowledge an entry; they'd have
// to validate its integrity first.
//
// **Leader Verification:** Before committing
// a configuration change, a leader would need
// confirmation from a quorum that their copies
// are not corrupt.
//
// **Inverted Recovery:** A newly elected
// leader could no longer assume its log is
// correct. Before forcing a configuration
// rollback, it would have to query other
// servers. If a quorum of followers has
// a log entry that the leader is missing,
// the leader must assume **it is the one
// with the damaged log** and repair
// itself *from its followers*.
//
// This "saved" algorithm is no longer the
// simple reconfiguration algorithm. It's
// a new, more complex, and more robust
// protocol that treats the leader not
// as an infallible monarch, but as a
// coordinator that is just as susceptible
// to faults as any other node.
//
// But, see below for details:
//
// If PAR is done during the Pre-vote,
// does that then make the simpler reconfig
// algorithm viable again? Short answer:
// yes.
//
// also called whenever membership changes forward,
// not just backwards now.
func (s *TubeNode) updateMembershipDueToVaporizedMemConfig(
	vaporizedMemCfgIdx int64,
	prevMemCfg *MemberConfig,
	membershipChangedForward int64,
) {

	// we should only have one or the other change (either
	// forward config change, or roll-back: but not both!)
	if membershipChangedForward > 0 && prevMemCfg != nil {
		panic(fmt.Sprintf("assert error in updateMembershipDueToVaporizedMemConfig: prevMemCfg should be nil when:  membershipChangedForward > 0 (is %v); prevMemCfg='%v'", membershipChangedForward, prevMemCfg))
	}

	if membershipChangedForward > 0 {
		//vv("%v sees membershipChangedForward = %v; starting s.clusterSize() = %v", s.me(), membershipChangedForward, s.clusterSize())

		// this calls s.adjustCktReplicaForNewMembership() inside/already.
		// It sets s.state.MC and s.state.PrevMemberConfig
		// from the last member change in the log.
		s.startCircuitsToLatestMC()

		//vv("%v sees membershipChangedForward = %v; after startCircuitsToLatestMC, s.clusterSize() is now = %v", s.me(), membershipChangedForward, s.clusterSize())
	}
	// else-- nada. we never roll back in mongo
}

// how long before we declare a follower is down
func (s *TubeNode) followerDownDur() time.Duration {
	if s.cfg.MinElectionDur == 0 {
		panic("cannot have s.cfg.MinElectionDur == 0")
	}
	return 4*s.cfg.MinElectionDur + s.cfg.ClockDriftBound
}

// (1) handle AE calls us if we receive a new
// s.state.MC so we can establish
// circuits (in the background).
//
// (2) mainline calls us on leader election timeout.
// .
// How do we decide if an update is needed?
// we look at the MC.
// a) if a peer is missing from cktAllByName, we attempt connect.
// b) if we are leader and the connection is down, we attempt reconnect.
// c) if we are leader and the connection is up BUT
// it has been too long since we've heard from them, we attemp reconnect.
// (this uses info.LastHeardAnything).
// .
// Notice that info.LastHeardAnything is slightly
// different from what the lastSeen that the watchdog maintains,
// because currently it is only updated in in handleAppendEntriesAck,
// while watchdog also monitors AE and tally pre/votes.
//
// The watchdog is on all replicas; the follower info is
// only kept on the leader. The watchdog timer should
// independently ask for a reconnect based on lastSeen.
//
// If the change is only to our own identity
// (i.e we are restarted follower and leader is
// broadcasting our new PeerID) then there is
// no need to reboot our links.
func (s *TubeNode) connectToMC(origin string) {
	if s.state.MC == nil ||
		s.state.MC.PeerNames.Len() == 0 {
		//vv("%v connectToMC() returning early; sees empty MC", s.name)
		return
	}
	//vv("%v top connectToMC(); MC='%v'; keys of s.cktAllByName = '%v'", s.name, s.state.MC, keys(s.cktAllByName))

	var backgroundConnCount int
	_ = backgroundConnCount
	//defer func() {
	//	//vv("%v end of connectToMC(); backgroundConnCount=%v", s.me(), backgroundConnCount)
	//}()

	sortedPeerNamesCached := s.state.MC.PeerNames.Cached()
	numNames := len(sortedPeerNamesCached)

	// these are disjoint, as we enforce that.
	shadows := s.state.ShadowReplicas.PeerNames.Cached()
	sortedPeerNamesCached = append(sortedPeerNamesCached, shadows...)
	numNames += len(shadows)

	// To avoid redundant loops, we (used to) only connect the
	// upper-right triangle of the sorted peer name
	// square grid matrix. By convention, we actively link
	// from i (row) to j (column) in the
	// upper right triangle, while the lower-left
	// triangle remains passive, accepting
	// but not initiating connections.
	var wantConnection []string

	// why does reaching out to more failed nodes
	// cause our leader step down to not happen??? in 052 with this:
	for j := range numNames { // node_0 has conn to node_2 at least! but makes 402 hang. why cannot node_1 reach node_0 once it comes back? b/c it does not try in upper triangle. So why cannot node_0 reach node_1 (or node_2 maybe--maybe it is that node_2 is finding node_0 but not vice-versa). but now 052 red. 402 hangs!?!
		target := sortedPeerNamesCached[j].key
		if target == s.name {
			continue // no need to connect to self.
		}
		// can have isPending
		cktP, ok := s.cktAllByName[target]
		_ = cktP
		if !ok {
			//vv("%v sees no ckt to '%v', adding to wantConnection", s.me(), target)
			wantConnection = append(wantConnection, target)

			// this would be redundant with the below
			// call to connectInBackgroundIfNoCircuitTo().
			//cktP := s.newCktPlus(target, TUBE_REPLICA)
			//cktP.startWatchdog()
			//s.cktAllByName[target] = cktP

			//vv("%v: setting up isPending cktP for target = '%v'", s.name, target)
		} else {
			// already have connection to target.
			// watchdog should take care of the rest.
		}
	}
	//}
	backgroundConnCount = len(wantConnection)
	if backgroundConnCount > 0 {
		//vv("%v: in connectToMC() wantConnection (len %v) = '%#v'", s.me(), len(wantConnection), wantConnection)
	}

	for _, peerName := range wantConnection {

		// peerID might be empty. we try via url anyway.
		// We are inside connectToMC(), called by AE.
		s.connectInBackgroundIfNoCircuitTo(peerName, origin+"|connectToMC") // call 3
	}
}

// connectInBackgroundIfNoCircuitTo is called
// from
// (1) changeMembership()
// (2) just above in connectToMC(); called by AE.
// (3) below in adjustCktReplicaForNewMembership()
// (4) leaderSendsHeartBeats
//
// Callers can leave a cktPlus{isPending:true, pendingTm:time.Time{}}
// for a given cktPlus.PeerName
// in a the s.cktAllByName map to tell us that we should
// start a background connection to that cktPlus.PeerName.
// Callers (1) and (2) do this. We will set pendingTm
// to indicate start of the attempt.
func (s *TubeNode) connectInBackgroundIfNoCircuitTo(peerName, origin string) {
	from := origin + "|" + fileLine(2)
	if s.cfg.NoBackgroundConnect {
		return // this feature is off
	}
	if peerName == s.name {
		return // already connected to self.
	}
	//vv("%v connectInBackgroundIfNoCircuitTo from='%v' peerName='%v'", s.me(), from, peerName)
	//vv("%v top connectInBackgroundIfNoCircuitTo '%v'", s.me(), peerName) // seen in 059

	var url, peerID, netAddr string
	detail, ok := s.state.MC.PeerNames.Get2(peerName)
	if ok {
		url = detail.URL
		peerID = detail.PeerID
		netAddr = detail.Addr

		if url == "" {
			panic("cannot have empty url")
		}
		if url == "boot.blank" {
			//vv("%v no way to connect to boot.blank... skipping background connect to peerName='%v'", s.me(), peerName) // 059 sees! arg. the culprit!
			// SO: we need to update the details in
			// s.state.MC.PeerNames when we get
			// an actual connection made for the first time! thought
			// we were, but apparently not.
			return
		}
	} // end have det from MC

	cktP, okPeerNameInCktAllByName := s.cktAllByName[peerName]
	if !okPeerNameInCktAllByName {
		// notice that all we have is the peerName
		cktP = s.newCktPlus(peerName, TUBE_REPLICA)
		//vv("%v calling startWatchdog in connectInBackgroundIfNoCircuitTo on cktP=%p for '%v'", s.me(), cktP, cktP.PeerName)
		cktP.startWatchdog()

		//vv("%v set up isPending cktP for peerName = '%v'; cktP='%v'", s.name, peerName, cktP) // seen 055 for node_0 only though.
		s.cktAllByName[peerName] = cktP
	} else {
		//vv("%v ok = true, will connect in background below!", s.name)
		//url = cktP.URL
		netAddr = cktP.PeerBaseServerAddr
		if netAddr == "" {
			netAddr = cktP.Addr
		}
	}

	if strings.Contains(url, "auto-cli-from-") {

		// cannot use the utility func RemoteBaseServerAddr
		// because we lack a ckt, but very similarly...
		netAddr, serviceName, peerID2, _, err := rpc.ParsePeerURL(url)
		panicOn(err)
		split := strings.Split(netAddr, "://")
		host := extractFromAutoCli(split[1])
		//vv("host='%v'; split='%#v'", host, split)
		if peerID == "" {
			peerID = peerID2
		}
		url = fmt.Sprintf("%v://%v/%v/%v", split[0], host, serviceName, peerID)
		//vv("new url = '%v'", url)
	}

	if cktP.PeerID == "" {
		cktP.PeerID = peerID
	}
	if cktP.Addr == "" {
		cktP.Addr = netAddr
	}
	//vv("%v starting background connection to '%v' via url='%v'; where netAddr='%v'", s.name, peerName, url, netAddr) // 402 not seen, auto-cli reject above keeps everything 402 out. 055 seen both old and new leader, good. 059 not seen with stashLeader false
	s.backgroundConnectToPeer(cktP, url, netAddr, from)

}

type packReconnect struct {
	submit  time.Time
	addr    string
	netAddr string
	url     string
	from    string
}

func (a *packReconnect) differs(b *packReconnect) bool {
	if a.addr != b.addr {
		return true
	}
	if a.netAddr != b.netAddr {
		return true
	}
	if a.url != b.url {
		return true
	}
	return false
}

func (s *TubeNode) backgroundConnectToPeer(cktP *cktPlus, url, netAddr, from string) {

	addr := cktP.PeerBaseServerAddr // empty in testing? set in prod?
	if addr == "" {
		if netAddr != "" {
			addr = netAddr // prod happy
		} else {
			addr = url // testing 401 happy membership_test
		}
	}

	recon := newReconnect(addr, netAddr, url, from)

	//vv("%v: top of backgroundConnectToPeer using addr='%v' (cktP='%v') url='%v'; netAddr='%v'; s.state.MC='%v'; from='%v'", s.me(), addr, cktP, url, netAddr, s.state.MC, from) // data racy, leave off mostly!

	cktP.requestReconnectMut.Lock()
	cktP.requestReconnect = recon
	prevStarted := cktP.requestReconnectLastBeganTm
	cktP.requestReconnectLastBeganTm = time.Time{}
	cktP.requestReconnectMut.Unlock()

	_ = prevStarted

	// request queued up, wake the watchdog
	// if its not busy. otherwise it will
	// get to it on its 2 sec refresh. maybe. if
	// it doesn't decide to ignore all new
	// requests after the last failed one
	// to prevent busy looping.
	select {
	case cktP.requestReconnectPulse <- true:
	default:
	}

}

func newReconnect(addr, netAddr, url, from string) *packReconnect {
	return &packReconnect{
		submit:  time.Now(),
		addr:    addr,
		netAddr: netAddr,
		url:     url,
		from:    from,
	}
}

// called by adjustCktReplicaForNewMembership().
func (s *TubeNode) serviceMembershipObservers() {
	if s.state.MC == nil {
		return
	}
	if s.state.MC.BootCount != 0 {
		// boot up config, not really together yet.
		return
	}
	var observers []*rpc.Circuit
	for name, cktP := range s.cktAllByName {
		_ = name
		if cktP.isPending() {
			continue
		}
		switch cktP.PeerServiceName {
		case TUBE_OBS_MEMBERS: // , TUBE_CLIENT:
			// clients could also automatically be observers
			// of membership changes?
		default:
			continue
		}
		alwaysPrintf("%v sees observer '%v'", s.name, name)
		observers = append(observers, cktP.ckt)
	}
	if len(observers) == 0 {
		return
	}
	left := len(observers) - 1

	frag := s.newFrag()
	frag.SetUserArg("leader", s.leaderID)
	frag.SetUserArg("leaderName", s.leaderName)
	frag.SetUserArg("leaderURL", s.leaderURL)

	frag.FragOp = ObserveMembershipChange
	frag.FragSubject = "ObserveMembershipChange"
	bts, err := s.state.MC.MarshalMsg(nil)
	panicOn(err)
	frag.Payload = bts

	for _, ckt := range observers {
		err = s.SendOneWay(ckt, frag, -1, left)
		_ = err // don't panic on halting.
		if err != nil {
			alwaysPrintf("%v non nil error '%v' on notifying membership observer to '%v'", s.me(), err, ckt.RemotePeerID)
		}
	}
}

// called by
// (1) startCircuitsToLatestMC() :6793
// (2) commitWhatWeCan() :7693
// (3) setMC() :10429
// (4) updateMembershipDueToVaporizedMemConfig() :10808
//
// given new membership, toss out non-members
// from cktReplica, and add in any new ones
// we have cktall for (except ourselves of course).
func (s *TubeNode) adjustCktReplicaForNewMembership() {
	//vv("%v top adjustCktReplicaForNewMembership()", s.name)
	if s.state.MC == nil {
		if !s.isTest() {
			panic("must have s.state.MC")
		}
		return // under test okay to have nil MC (020, 021...)
	}
	if s.state.ShadowReplicas == nil {
		panic("must have s.state.ShadowReplicas, even if empty")
	}
	s.serviceMembershipObservers()

	// toss any missing
	keepName := make(map[string]*cktPlus)
	var toss []string
	for peerID, cktP := range s.cktReplica {
		_, allowed0 := s.state.MC.PeerNames.Get2(cktP.PeerName)
		if allowed0 && cktP.PeerName != s.name {
			// (but don't add self to cktReplica)
			keepName[cktP.PeerName] = cktP
		} else {
			// also keep ShadowReplicas, as well as MC.
			_, allowed1 := s.state.ShadowReplicas.PeerNames.Get2(cktP.PeerName)
			if allowed1 && cktP.PeerName != s.name {
				// (but don't add self to cktReplica)
				keepName[cktP.PeerName] = cktP
			} else {
				// not in MC or ShadowReplicas
				toss = append(toss, peerID)
			}
		}
	}
	// We don't delete during map iteration, as we
	// might mess up the iteration. So we do it after:
	for _, peerID := range toss {
		delete(s.cktReplica, peerID)
	}
	// add any new, from either MC or ShadowReplicas
	var group *MemberConfig
	for k := range 2 {
		switch k {
		case 0:
			group = s.state.MC
		case 1:
			group = s.state.ShadowReplicas
		}
		for peerName, det := range group.PeerNames.All() {
			if peerName == s.name {
				// never want self in cktReplica
				continue
			}
			_, already := keepName[peerName]
			if already {
				continue
			}
			if det.PeerID == "" {
				// happens on boot up... have to allow.
				continue

				panic(fmt.Sprintf("peerID missing for peerName '%v' in MC='%v'", peerName, s.state.MC))
			}
			// we need to add peerName, peerID, if available, to cktReplica
			cktP, ok := s.cktall[det.PeerID]
			if ok {
				if cktP.PeerServiceName == TUBE_REPLICA {
					s.cktReplica[det.PeerID] = cktP
				} else {
					err := fmt.Errorf("%v error: problem! we were asked to add a member '%v' to cktReplica, but cannot because it is not TUBE_REPLICA; PeerServiceName='%v'", s.me(), peerName, cktP.PeerServiceName)
					panic(err)
				}
			} else {
				//vv("%v warning: drat! we want to add a member '%v' to cktReplica, but no ckt available in cktall: '%v'", s.me(), peerName, s.showCktall()) // not seen 059

				// we are inside adjustCktReplicaForNewMembership() here.
				s.connectInBackgroundIfNoCircuitTo(peerName, "adjustCktReplicaForNewMembership") // call 4
			}
		}
	}
}

// do we actually have a non-pending in cktAllByName
// for remotePeerName?
func (s *TubeNode) haveActualCktAllConnTo(remotePeerName string) (ok bool) {
	if remotePeerName == s.name {
		return true // always already connected to self
	}
	if remotePeerName == "" {
		panic("remotePeerName cannot be empty")
	}
	var cktP *cktPlus
	cktP, ok = s.cktAllByName[remotePeerName]
	if ok && cktP.isPending() {
		ok = false
	}
	return
}

func peerIDfromULR(leaderURL string) string {
	_, _, leaderPeerID, _, err := rpc.ParsePeerURL(leaderURL)
	panicOn(err)
	return leaderPeerID
}

// test specific version of setupFirstRaftLogEntryBootstrapLog circa :8920
// sets s.state.MC from boot.NewConfig.Clone()
func (s *TubeNode) testSetupFirstRaftLogEntryBootstrapLog(boot *FirstRaftLogEntryBootstrap) (err error) {
	//vv("top testSetupFirstRaftLogEntryBootstrapLog()")
	if s.wal != nil {
		n := len(s.wal.raftLog)
		if n > 0 {
			panic(fmt.Sprintf("testSetupFirstRaftLogEntryBootstrapLog() must have empty wal as PRE condition. s.wal.nodisk=%v; s.wal.path: '%v'", s.wal.nodisk, s.wal.path))
		}
	} else {
		err = s.initWalOnce()
		panicOn(err)
		if err != nil {
			return err
		}
	}

	nNode := s.clusterSize()
	if nNode != boot.NewConfig.PeerNames.Len() {
		// 401 under mongo hits this-- we are trying to
		// get the followers to have an empty MC to start.
		// comment out for now... it was just a getting started check anyway.
		//panic(fmt.Sprintf("testSetupFirstRaftLogEntryBootstrapLog() have nNode=%v but len(boot.NewConfig.PeerNames) = %v", nNode, boot.NewConfig.PeerNames.Len()))
	}
	if s.state.CurrentTerm != 0 {
		panic(fmt.Sprintf("testSetupFirstRaftLogEntryBootstrapLog() must see s.state.CurrentTerm == 0, not %v", s.state.CurrentTerm))
	}

	desc := fmt.Sprintf("bootstrap cluster leader('%v') with log entry 1 containing %v placeholder boot.blank nodes: '%v'", s.name, boot.NewConfig.PeerNames.Len(), boot.NewConfig.PeerNames)
	tkt := s.NewTicket(desc, "", "", nil, s.PeerID, s.name, MEMBERSHIP_BOOTSTRAP, -1, s.Ctx)

	tkt.MC = boot.NewConfig.Clone()
	tkt.MC.RaftLogIndex = 1

	entry := &RaftLogEntry{
		Tm:                 time.Now(),
		LeaderName:         s.name,
		Term:               1, // s.state.CurrentTerm,
		Index:              1, // idx,
		Ticket:             tkt,
		CurrentCommitIndex: 1, // s.state.CommitIndex,
		node:               s,
	}
	es := []*RaftLogEntry{entry}

	// Use isLeader false even though we are the first leader, as
	// true always panics overwriteEntries; b/c more important
	// to assert about non-bootstrap scenarios in AppendEntries().
	isLeader := false

	var curCommitIndex int64 = 1
	var keepCount int64 = 0
	// in testSetupFirstRaftLogEntryBootstrapLog() here.
	err = s.wal.overwriteEntries(keepCount, es, isLeader, curCommitIndex, 0, &s.state.CompactionDiscardedLast, s)
	panicOn(err)

	if true { // TODO restore: s.cfg.isTest {
		s.wal.assertConsistentWalAndIndex(s.state.CommitIndex)
	}

	//vv("%v wrote to wal first log entry, txt = '%v'", s.name, tkt)

	// avoid (at least leader) to replay the bootstrap
	// entry on startup, back to its old self PeerID, which
	// will always fail of course.
	// We will have everyone notice MEMBERSHIP_BOOTSTRAP
	// and not try to reply to that client too.
	s.state.LastApplied = 1
	s.state.CurrentTerm = 1
	s.state.LastAppliedTerm = 1

	// in testSetupFirstRaftLogEntryBootstrapLog
	s.state.MC = tkt.MC.Clone()
	//vv("%v in testSetupFirstRaftLogEntryBootstrapLog, just set state.MC", s.name)

	// test means noDisk anyway, but just for form's sake:
	// Plus now with mongo reconfig preferring s.saver.state/s.state
	path := s.GetPersistorPath()
	s.saver, _, err = s.cfg.NewRaftStatePersistor(path, s, false)
	panicOn(err)
	s.saver.save(s.state)

	close(boot.Done)
	return nil
}

func (s *TubeNode) initWalOnce() (err error) {
	if s.wal != nil {
		return nil
	}
	// we could append an RFC3339NanoNumericTZ0pad timestamp
	// on the log and then open the most recent one,
	// but not sure its worth it since then we have
	// to do extra work to find the log, sorting the files.

	logPath := s.cfg.DataDir + sep + "tube.wal.msgp"
	s.wal, err = s.cfg.newRaftWriteAheadLog(logPath, false)
	//vv("%v s.wal logPath = '%v'", s.name, logPath)
	panicOn(err)

	s.candidateReinitFollowerInfo()
	return
}

func (s *TubeNode) membershipDiffOldNew(prevConfig, newConfig *MemberConfig) (diff map[string]string) {

	diff = make(map[string]string)
	for newName, newDet := range newConfig.PeerNames.All() {
		oldDet, ok := prevConfig.PeerNames.Get2(newName)
		if !ok {
			diff[newName] = "added"
		} else {
			if newDet.URL != oldDet.URL {
				diff[newName] = fmt.Sprintf("updated URL from '%v' -> '%v'", oldDet.URL, newDet.URL) // "updated URL from 'tcp://100.89.245.101:7001/tube-replica/fNtXzQAwDLyASLcTo4bHRwWtag9H' -> '100.89.245.101:7001'" not great TODO can we preserve info rather than strip off the protcol and PeerID?
			} else if newDet.PeerID != oldDet.PeerID {
				diff[newName] = fmt.Sprintf("updated PeerID from '%v' -> '%v'", oldDet.PeerID, newDet.PeerID)
			}
		}
	}
	for prevName := range prevConfig.PeerNames.All() {
		_, ok := newConfig.PeerNames.Get2(prevName)
		if !ok {
			diff[prevName] = "removed"
		}
	}
	return
}

func cktallToString(cktall map[string]*cktPlus) (r string) {
	if len(cktall) == 0 {
		return "(empty cktall)"
	}
	r = fmt.Sprintf("cktall of len %v:\n", len(cktall))
	i := 0
	for peerID, cktP := range sorted(cktall) {
		r += fmt.Sprintf("  [%02d] %v: %v\n", i, peerID, cktP)
		i++
	}
	return
}

func extractFromAutoCli(name string) string {
	if !strings.Contains(name, "auto-cli-from-") {
		return name
	}
	from := name[len("auto-cli-from-"):]
	x := strings.Index(from, "-to-")
	if x < 0 {
		panic(fmt.Sprintf("mal-formed auto-cli name, did not have -to- part: '%v'", name))
	}
	return from[:x]
}

func FixAddrPrefix(addr string) string {

	u, err := url.Parse(addr)
	if err != nil {
		errs := err.Error()
		if strings.Contains(errs, "missing protocol scheme") && addr[0] == ':' {
			return "tcp://127.0.0.1" + addr
		}
	}
	_ = u
	return addr
}

// test 401 membership_test helper
func (c *TubeCluster) showClusterGrid(assertFullConnectedNodeName string, expectedConnCount int) {
	time.Sleep(time.Second)
	if c.Cfg.UseSimNet {
		if c.Snap == nil {
			c.Snap = c.Cfg.RpcCfg.GetSimnetSnapshotter()
			if c.Snap == nil {
				panic("grid is connected, why no snapshotter?")
			}
		}
		snp := c.Snap.GetSimnetSnapshot(false)
		//vv("at end, simnet = '%v'", snp.LongString())
		//vv("at end, simnet.Peer = '%v'", snp.Peer)
		//vv("at end, simnet.DNS = '%#v'", snp.DNS)
		matrix := snp.PeerMatrix()
		if assertFullConnectedNodeName != "" {
			//vv("matrix='%v'", matrix)
			connections := matrix.Sparse[assertFullConnectedNodeName]
			if len(connections) != expectedConnCount {
				panic(fmt.Sprintf("len(connections)=%v != expectedConnCount(%v)", len(connections), expectedConnCount))
			}
		}
		// node 4 on test 401 is just a client, for instance, so it won't
		// be fully connected(!). So we do not assert all should
		// be talking to all.
	}
}

type Session struct {
	ctx context.Context

	// initial request ====================
	CliName            string `zid:"0"`
	CliPeerID          string `zid:"1"`
	CliPeerServiceName string `zid:"2"`
	CliRndOnce         string `zid:"3"`
	CliURL             string `zid:"19"`

	// can be zero, but adjust LastKnownIndex below on session use.
	CliLastKnownIndex0      int64         `zid:"4"`
	SessRequestedInitialDur time.Duration `zid:"5"`
	ClusterID               string        `zid:"6"`

	// initial response ====================
	SessionAssignedIndex int64 `zid:"7"` // LogIndex of created session

	// SessionID is different from CliPeerID so
	// that clients can change their SessionID if they
	// have too. And they will, as the server node
	// will kill the session
	// if a gap in the Serial between the last applied
	// and the next proposed is detected -- to alert
	// the client that their request has been lost
	// before it could be committed to the raft log.
	// The client must then reconnect with a new session and
	// new set of incrementing serial numbers.
	SessionID string `zid:"8"` // fixed on creation

	// note that SessionIndexEndxTm is never updated on
	// the leader during session refresh. Instead it
	// is immutable after creation, and the SessionTableEntry's
	// SessionEndxTm starts at SessionIndexEndxTm but
	// is updated on session refresh. At the moment local
	// leader-served reads only extend the session locally
	// to avoid having to do a full cluster consensus
	// write on every local read (local reads are supposed
	// to be fast, that is the point of them -- if we
	// made them extend the session through consensus they
	// would bog down the system instead). The session
	// still gets extended in the current leader's local
	// memory by refreshSession(), but if there is a
	// leader change then the session extensions are lost.
	// Which does beg the question does this create
	// non-determinism since the other replicas might
	// think the session is gone? I think not, because
	// only a leader sends SESS_END via garbageCollectOldSessions().
	// So why bother putting sessions through consensus
	// in the first place, if session extension won't survive
	// leadership change anyway? Linearizability demands that raft
	// leaders deduplicate requests, and session.SessionSerial
	// is how that is done. But how does that need consensus?
	// It allows sessions during their initial timeout period
	// to survive leadership change and still deduplicate
	// requests... and writes do extend the session across
	// the cluster, since commitWhatWeCan() extends the
	// tkt.SessionID on all replicas that commit a log entry.
	// Reads increment the SessionSerial too as they
	// should not be dedupped with prior write requests.
	// On leadership change the session number gap will
	// kill the session; this will mean an extra round
	// trip for the client on leadership change, but the benefit
	// is that most reads are local, and local reads remain fast.
	SessionIndexEndxTm    time.Time `zid:"9"` // session deleted >= here
	LeaderName            string    `zid:"10"`
	LeaderPeerID          string    `zid:"11"` // changes only on restart
	LeaderPeerServiceName string    `zid:"12"`
	LeaderURL             string    `zid:"13"`
	LeaderRndOnce         string    `zid:"14"` // always different
	Errs                  string    `zid:"15"`
	// end initial response ====================

	// subsequent sess use must set.
	SessionSerial int64 `zid:"16"` // monotone inside a SessionID

	// LastKnownIndex is a session causality token to provide
	// sequential consistency even if clocks misbehave
	// (see the Raft dissertation, p75, section 6.4.1).
	//
	// Misbehavior of leases based on clocks could
	// occur due to long pauses such
	// as scheduling pauses, garbage collection pauses,
	// virtual machine migrations, or clock rate adjustments
	// for time synchronization. Even a slow network path
	// could create issues.
	//
	// LastKnownIndex is first set in the leader's replicateTicket() call
	// (see tkt.NewSessReq.SessionAssignedIndex = idx; at tube.go:5345)
	// as the raft log index assigned to the log entry
	// that creates the session.
	//
	// LastKnownIndex is copied to tkt.SessionLastKnownIndex
	// when a new Ticket is created for any session operation
	// by the client, and thus re-conveyed to the
	// Tube cluster in its role of causality token (a logical clock).
	//
	// If tkt.SessionLastKnownIndex > s.state.LastApplied
	// then an error is thrown at tube.go:14307 in
	// leaderDoneEarlyOnSessionStuff() to preserve causality
	// even if timing/clock/pause badness violates the
	// leasing time-based logic.
	LastKnownIndex int64 `zid:"17"` // adjust on each use

	MinSessSerialWaiting int64 `zid:"18"`

	readyCh chan struct{}
	cli     *TubeNode // local only, allow calling Write/Read on Session.

	// need to index by CliRndOnce, SessionID. maybe by LeaderRndOnce?
}

func (s *Session) Clone() *Session {
	cp := *s
	cp.readyCh = nil
	return &cp
}

func (s *Session) Close() error {
	//vv("TODO have Session.Close send SESS_END? not sure we can reliably close from tup as the process is terminating.")
	return nil
}

func (z *Session) String() (r string) {
	r = "&Session{\n"
	r += fmt.Sprintf("  //----- initial request  -----\n")
	r += fmt.Sprintf("              CliName: \"%v\",\n", z.CliName)
	r += fmt.Sprintf("            CliPeerID: \"%v\",\n", z.CliPeerID)
	r += fmt.Sprintf("               CliURL: \"%v\",\n", z.CliURL)
	r += fmt.Sprintf("   CliPeerServiceName: \"%v\",\n", z.CliPeerServiceName)
	r += fmt.Sprintf("           CliRndOnce: \"%v\",\n", z.CliRndOnce)
	r += fmt.Sprintf("   CliLastKnownIndex0: %v,\n", z.CliLastKnownIndex0)
	r += fmt.Sprintf("SessRequestedInitialDur: %v,\n", z.SessRequestedInitialDur)
	r += fmt.Sprintf("            ClusterID: %v,\n", z.ClusterID)

	r += fmt.Sprintf("  //----- initial response  -----\n")
	r += fmt.Sprintf(" SessionAssignedIndex: %v,\n", z.SessionAssignedIndex)
	r += fmt.Sprintf("            SessionID: \"%v\",\n", z.SessionID)
	r += fmt.Sprintf("   SessionIndexEndxTm: %v,\n", nice(z.SessionIndexEndxTm))
	r += fmt.Sprintf("           LeaderName: \"%v\",\n", z.LeaderName)
	r += fmt.Sprintf("         LeaderPeerID: \"%v\",\n", z.LeaderPeerID)
	r += fmt.Sprintf("LeaderPeerServiceName: \"%v\",\n", z.LeaderPeerServiceName)
	r += fmt.Sprintf("            LeaderURL: \"%v\",\n", z.LeaderURL)
	r += fmt.Sprintf("        LeaderRndOnce: \"%v\",\n", z.LeaderRndOnce)
	r += fmt.Sprintf("                 Errs: \"%v\",\n", z.Errs)

	r += fmt.Sprintf(" // -----  session use  -----\n")
	r += fmt.Sprintf("       LastKnownIndex: %v,\n", z.LastKnownIndex)
	r += fmt.Sprintf(" MinSessSerialWaiting: %v,\n", z.MinSessSerialWaiting)
	r += fmt.Sprintf("        SessionSerial: %v,\n", z.SessionSerial)
	r += "}\n"
	return
}

// external, client callable
func (s *TubeNode) CloseSession(ctx context.Context, sess *Session) (err error) {
	desc := fmt.Sprintf("CloseSession call from '%v' for SessionID:'%v'", s.name, sess.SessionID)
	tkt := s.NewTicket(desc, "", "", nil, s.PeerID, s.name, SESS_END, 0, ctx)
	tkt.EndSessReq_SessionID = sess.SessionID

	select {
	case s.closeSessionRequestCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}

	select { // tup linz hung here. tup hung here on simple startup. in CreateNewSession.
	case <-tkt.Done.Chan: // waits for completion
		err = tkt.Err
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		err = ErrShutDown
		return
	}
}

// external, client callable
func (s *TubeNode) CreateNewSession(ctx context.Context, leaderURL string) (r *Session, err error) {

	var ckt *rpc.Circuit
	var onlyPossibleAddr string
	if leaderURL != "" {
		ckt, onlyPossibleAddr, _, err = s.getCircuitToLeader(ctx, leaderURL, nil, false)
		if err != nil {
			return
		}
		_ = ckt
		_ = onlyPossibleAddr
		//vv("good, cli got ckt to leaderURL='%v': '%v'", leaderURL, ckt)
	}

	desc := fmt.Sprintf("createNewSession at '%v'", s.name)
	tkt := s.NewTicket(desc, "", "", nil, s.PeerID, s.name, SESS_NEW, 0, ctx)
	tkt.NewSessReq = s.newSessionRequest(ctx)

	select {
	case s.newSessionRequestCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}

	select { // tup linz hung here. tup hung here on simple startup. in CreateNewSession.
	case <-tkt.Done.Chan: // waits for completion
		err = tkt.Err
		r = tkt.NewSessReply
		if r != nil { // might be nil on error?
			// be ready for next use:
			r.LastKnownIndex = r.SessionAssignedIndex
			r.cli = s
			r.ctx = ctx // because is different from the request
		} else {
			vv("r is nil. err = %v", err)
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		err = ErrShutDown
		return
	}
	return
}

func (s *TubeNode) newSessionRequest(ctx context.Context) (r *Session) {
	r = &Session{
		ctx:                     ctx,
		CliName:                 s.name,
		CliPeerID:               s.PeerID,
		CliURL:                  s.URL,
		CliPeerServiceName:      s.PeerServiceName,
		CliRndOnce:              rpc.NewCallID(""),
		CliLastKnownIndex0:      0,
		SessRequestedInitialDur: time.Minute * 10,
		ClusterID:               s.ClusterID,
		readyCh:                 make(chan struct{}),
	}
	return
}

// internal routine, <-s.closeSessionRequestCh seen
func (s *TubeNode) handleCloseSessionRequestTicket(tkt *Ticket) {
	//vv("%v top handleCloseSessionRequestTicket", s.me())

	tkt.Stage += ":closeSessionRequestCh"

	if s.redirectToLeader(tkt) {
		if tkt.Err != nil {

			//vv("%v bail out in handleNewSessionRequestTicket; error happened '%v'", s.me(), tkt.Err)
			s.FinishTicket(tkt, false)
			return // bail out, error happened.
		}

		//vv("%v adding to WaitingAtFollow, newSessionRequestCh: '%v'", s.me(), tkt.Short())
		tkt.Stage += ":after_redirectToLeader_add_WaitingAtFollow"

		prior, already := s.WaitingAtFollow.get2(tkt.TicketID)
		if already {
			panic(fmt.Sprintf("WAF already has prior='%v'; versus tkt='%v'", prior, tkt))
		}
		s.WaitingAtFollow.set(tkt.TicketID, tkt)
		//vv("%v AFTER adding to WAF, at newSessionRequestCh: tkt='%v'", s.me(), tkt)
		return
	}
	// INVAR: on leader already, no re-direction needed.
	//s.leaderSideCloseNewSess(tkt)
	s.replicateTicket(tkt)
}

// internal routine, <-s.newSessionRequestCh seen
func (s *TubeNode) handleNewSessionRequestTicket(tkt *Ticket) {
	//vv("%v top handleNewSessionRequestTicket", s.me())

	tkt.Stage += ":newSessionRequestCh"
	//tkt.localHistIndex = len(s.tkthist)
	//s.tkthistQ.add(tkt)
	//s.tkthist = append(s.tkthist, tkt)

	if s.redirectToLeader(tkt) {
		if tkt.Err != nil {

			//vv("%v bail out in handleNewSessionRequestTicket; error happened '%v'", s.me(), tkt.Err)
			s.FinishTicket(tkt, false)
			return // bail out, error happened.
		}

		//vv("%v adding to WaitingAtFollow, newSessionRequestCh: '%v'", s.me(), tkt.Short())
		tkt.Stage += ":after_redirectToLeader_add_WaitingAtFollow"

		prior, already := s.WaitingAtFollow.get2(tkt.TicketID)
		if already {
			panic(fmt.Sprintf("WAF already has prior='%v'; versus tkt='%v'", prior, tkt))
		}
		s.WaitingAtFollow.set(tkt.TicketID, tkt)
		//vv("%v AFTER adding to WAF, at newSessionRequestCh: tkt='%v'", s.me(), tkt)
		return
	}
	// INVAR: on leader already, no re-direction needed.
	s.leaderSideCreateNewSess(tkt)
	s.replicateTicket(tkt)
}

func (s *TubeNode) leaderSideCreateNewSess(tkt *Ticket) {
	//vv("%v top leaderSideCreateNewSess(tkt='%v'", s.me(), tkt)

	var r *Session = tkt.NewSessReq
	if r == nil {
		panic(fmt.Sprintf("tkt.NewSessReq must be set for SESS_NEW tkt: '%v'", tkt))
	}

	// session creation here
	//r.SessionAssignedIndex = 0 // filled by replicateTicket
	r.SessionID = rpc.NewCallID("") // fixed on creation
	// client has set CliRndOnce: rpc.NewCallID("")
	r.SessionSerial = 0 // client must increment after each call.

	// just grant whatever is requested for now; but at least 10 sec.
	if r.SessRequestedInitialDur < 10*time.Second {
		r.SessRequestedInitialDur = 10 * time.Second
	}
	r.SessionIndexEndxTm = time.Now().Add(r.SessRequestedInitialDur)

	r.LeaderName = s.name
	r.LeaderPeerID = s.PeerID
	r.LeaderPeerServiceName = s.PeerServiceName
	r.LeaderURL = s.URL
	r.LeaderRndOnce = rpc.NewCallID("")
}

// called from commitWhatWeCan() when SESS_END applied.
func (s *TubeNode) applyEndSess(tkt *Ticket, calledOnLeader bool) {

	//vv("%v applying SESS_END '%v'", s.me(), tkt.EndSessReq_SessionID)
	tkt.Applied = true
	if tkt.EndSessReq_SessionID == "" {
		panic("should not have empty tkt.EndSessReq_SessionID in applyEndSess")
	}
	if s.state == nil || s.state.SessTable == nil {
		return
	}
	//vv("%v: delete Session via SESS_END: %v", s.name, tkt.EndSessReq_SessionID)
	ste, ok := s.state.SessTable[tkt.EndSessReq_SessionID]
	if ok {
		delete(s.state.SessTable, tkt.EndSessReq_SessionID)
		s.sessByExpiry.Delete(ste)
	}

	// have any others expired?
	if calledOnLeader {
		s.garbageCollectOldSessions()
	}
}

// called from commitWhatWeCan() when SESS_NEW applied;
// after the session is committed (present on a quorum of servers).
func (s *TubeNode) applyNewSess(tkt *Ticket, calledOnLeader bool) {

	//vv("%v applying SESS_NEW '%v'; tkt.NewSessReq.SessionID='%v'", s.me(), tkt.NewSessReq, tkt.NewSessReq.SessionID)
	sess := tkt.NewSessReq
	if s.state.SessTable == nil {
		s.state.SessTable = make(map[string]*SessionTableEntry)
	}
	ste, already := s.state.SessTable[sess.SessionID]
	if !already {
		ste = newSessionTableEntry(sess.Clone()) // only call
		s.state.SessTable[sess.SessionID] = ste
		s.sessByExpiry.tree.Insert(ste)
	} else {
		panic(fmt.Sprintf("%v: arg: already have SessionID='%v' in s.state.SessTable. This should not happen right? what about log replay?", s.me(), sess.SessionID))
	}
	tkt.Applied = true

	// collect other sessions that have expired.
	if calledOnLeader {
		s.garbageCollectOldSessions()
	}
}

func (s *TubeNode) garbageCollectOldSessions() {
	if s.role != LEADER {
		return
	}
	if s.state == nil || s.state.SessTable == nil {
		return
	}
	now := time.Now()
	//for id, ste := range s.state.SessTable {
	for it := s.sessByExpiry.tree.Min(); !it.Limit(); {
		ste := it.Item().(*SessionTableEntry)
		id := ste.SessionID

		if gte(now, ste.SessionEndxTm) {
			desc := fmt.Sprintf("%v: about to replicate SESS_END for SessionID: '%v' b/c expired at '%v' <= now='%v'", s.name, id, nice(ste.SessionEndxTm), nice(now))

			//vv("%v ste.SessionEndxTm='%v' expired. garbageCollectOldSessions replicating SESS_END.", s.me(), nice(ste.SessionEndxTm))

			// this may be a major memory leak... why is this Ticket memory not cleaned up?
			// I think every time a session is refreshed, something happens to extend memory?
			// What if we just scan for expired sessions at a regular interval?
			// and also validate any session used on a ticket and kick it back if
			// the session "would" have been deleted already.
			tkt := s.NewTicket(desc, "", "", nil, s.PeerID, s.name, SESS_END, 0, s.MyPeer.Ctx)
			tkt.EndSessReq_SessionID = id
			s.replicateTicket(tkt)

			// let applyEndSess() do the actual deletion,
			// so that it happens in the correct serial order
			// on all state machines.
		} else {
			break // since sessByExpiry is sorted, all others are later.
		}
		it = it.Next()
	}
}

// called at top of replicateTicket(); and an
// inlined version is used in leaderServedLocalRead() for reads.
func (s *TubeNode) leaderDoneEarlyOnSessionStuff(tkt *Ticket) (doneEarly, needSave bool) {
	if tkt.SessionID == "" {
		// sessions not in use
		return
	}
	switch tkt.Op {
	case READ, WRITE, CAS, DELETE_KEY, READ_KEYRANGE, READ_PREFIX_RANGE,
		MAKE_TABLE, DELETE_TABLE, RENAME_TABLE, SHOW_KEYS,
		MEMBERSHIP_SET_UPDATE:
		// check these below
	default: // NOOP, MEMBERSHIP_*, SESS_*
		// ignore all else as far as dedup goes.
		return
	}

	defer func() {
		if tkt.Err != nil {
			//vv("%v leaderDoneEarlyOnSessionStuff() has tkt.Err='%v'; doneEarly=%v", s.name, tkt.Err, doneEarllly)
		} else {
			//vv("%v leaderDoneEarlyOnSessionStuff() no tkt.Err, returning %v", s.name, doneEarly)
		}
	}()

	var ste *SessionTableEntry

	var ok bool
	ste, ok = s.state.SessTable[tkt.SessionID]
	if !ok {

		tkt.Err = fmt.Errorf("%v leader error: unknown Ticket.SessionID='%v' (SessionSerial='%v' ). Must call CreateNewSession first to generate/register a new SessionID. In leaderDoneEarlyOnSessionStuff().", s.name, tkt.SessionID, tkt.SessionSerial)
		tkt.Stage += ":unkown_SessionID_leaderDoneEarlyOnSessionStuff"

		// calls FinishTicket() if waitingAtLeader, else sends
		// reply to remote.
		s.respondToClientTicketApplied(tkt)
		doneEarly = true
		return
	}

	// chapter 6, page 75, section 6.4.1 "Using clocks to
	// reduce messaging for read-only queries":
	// "To implement this guarantee [of sequential
	// consistency in the face of clock badness],
	// servers would include the index corresponding
	// to the state machine state with each reply to clients.
	// Clients would track the latest index
	// corresponding to results they had seen, and
	// they would provide this information to servers
	// on each request. If a server received a request
	// for a client that had seen an index greater
	// than the server’s last applied log index,
	// it would not service the request (yet)."
	if tkt.SessionLastKnownIndex > s.state.LastApplied {

		tkt.Err = fmt.Errorf("%v leader error: clock mis-behavior detected and client must re-submit ticket to preserve sequential consistency. Ticket.SessionID='%v' (SessionSerial='%v'); tkt.SessionLastKnownIndex(%v) > s.state.LastApplied(%v)", s.name, tkt.SessionID, tkt.SessionSerial, tkt.SessionLastKnownIndex, s.state.LastApplied)
		tkt.Stage += ":clock_badness_preserve_sequential_consistency"

		// calls FinishTicket() if waitingAtLeader, else sends
		// reply to remote.
		s.respondToClientTicketApplied(tkt)
		doneEarly = true
		return
	}

	now := time.Now()
	if gte(now, ste.SessionEndxTm) {
		alwaysPrintf("%v: SessionID has timed out: '%v'", s.name, ste.SessionID)

		tkt.Err = fmt.Errorf("%v leader error: session killed on timeout: SessionEndxTm('%v') <= now('%v'); for tkt.SessionID '%v': a client request was dropped. dropped tkt='%v'", s.name, nice(ste.SessionEndxTm), nice(now), tkt.SessionID, tkt)
		tkt.Stage += ":session_timeout_leaderDoneEarlyOnSessionStuff"

		// kill the session
		delete(s.state.SessTable, tkt.SessionID)
		s.sessByExpiry.Delete(ste)
		needSave = true
		//s.saver.save(s.state)

		s.respondToClientTicketApplied(tkt)
		doneEarly = true
		return
	}
	// does being in Serial2Ticket mean we are already applied?
	// commitWhatWeCan() does .Serial2Ticket.set(); as
	// does leaderServedLocalRead() which by-passes the
	// wal completely; so in either case, if we find this
	// SessionSerial in Serial2Ticket, we can respond again
	// for it.

	defer s.cleanupAcked(ste, tkt.MinSessSerialWaiting)
	priorTkt, already := ste.Serial2Ticket.Get2(tkt.SessionSerial)
	if already {
		// return the previous read, to preserve
		// linearizability (linz).
		//
		// As Ongaro in the Raft disseratation, page 71,
		// section 6.3 "Implementing linearizable semantics",
		// says:
		// "The session tracks the latest serial
		// number processed for the client, along
		// with the associated response. If a
		// server receives a command whose serial
		// number has already been executed, it
		// responds immediately without re-executing
		// the request.
		// Given this filtering of duplicate requests,
		// Raft provides linearizability. The Raft
		// log provides a serial order in which
		// commands are applied on every server.
		// Commands take effect instantaneously and
		// exactly once according to their first
		// appearance in the Raft log, since any
		// subsequent appearances are filtered out
		// by the state machines as described above.

		tkt.Err = priorTkt.Err

		switch priorTkt.Op {
		case READ, READ_KEYRANGE, READ_PREFIX_RANGE, SHOW_KEYS:
			// return the previous read, to preserve linz.

			// READ and SHOW_KEYS:
			tkt.Val = priorTkt.Val
			tkt.Vtype = priorTkt.Vtype
			tkt.LeaseRequestDur = priorTkt.LeaseRequestDur
			tkt.Leasor = priorTkt.Leasor
			tkt.LeaseEpoch = priorTkt.LeaseEpoch
			tkt.LeaseAutoDel = priorTkt.LeaseAutoDel
			tkt.LeaseWriteRaftLogIndex = priorTkt.LeaseWriteRaftLogIndex
			tkt.LeaseUntilTm = priorTkt.LeaseUntilTm

			// tkt.AsOfLogIndex below, for all.
			tkt.LeaderLocalReadGoodUntil = priorTkt.LeaderLocalReadGoodUntil
			tkt.LeaderLocalReadAtTm = priorTkt.LeaderLocalReadAtTm
			tkt.LeaderLocalReadHLC = priorTkt.LeaderLocalReadHLC

			// READ_KEYRANGE, READ_PREFIX_RANGE, SHOW_KEYS:
			tkt.KeyValRangeScan = priorTkt.KeyValRangeScan
			tkt.Stage += ":prev_read_val_used_leaderDoneEarlyOnSessionStuff"
		default:
			//alwaysPrintf("%v: ignoring duplicated '%v' op; SessionSerial=%v", s.name, tkt.Op, tkt.SessionSerial)
			tkt.Stage += ":already_applied_leaderDoneEarlyOnSessionStuff"
		}
		tkt.DupDetected = true
		tkt.LogIndex = priorTkt.LogIndex
		tkt.Term = priorTkt.Term
		tkt.AsOfLogIndex = priorTkt.AsOfLogIndex
		tkt.HighestSerialSeenFromClient = ste.HighestSerialSeenFromClient

		s.respondToClientTicketApplied(tkt)
		doneEarly = true
		return
	}

	//vv("tkt.SessionSerial not in ste.Serial2Ticket: '%v'", tkt.SessionSerial)

	// The paused client scenario/fencing-token-
	// needed-argument makes it seem
	// we should be rejecting delayed/duplicates/replays of
	// smaller numbered WRITE ops; per
	// https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
	switch tkt.Op {
	case READ, READ_KEYRANGE, READ_PREFIX_RANGE, SHOW_KEYS:
		// reads are idempotent so no need to reject them.
		// only need to analyze writes...
	default:
		if tkt.SessionSerial <= ste.MaxAppliedSerial {
			// we include ste.HighestSerialSeenFromClient in the reply
			// to try and let the client recover the session.
			tkt.HighestSerialSeenFromClient = ste.HighestSerialSeenFromClient
			tkt.Err = fmt.Errorf("%v leader is rejecting non-read-only ticket with SessionSerial(%v) <= session MaxAppliedSerial(%v); in SessionID '%v'; ste.HighestSerialSeenFromClient='%v'; this client request is dropped. dropped tkt='%v'", s.name, tkt.SessionSerial, ste.MaxAppliedSerial, tkt.SessionID, ste.HighestSerialSeenFromClient, tkt)

			//vv("%v error session serial for write too old: %v\n", s.name, tkt.Err)
			tkt.Stage += ":write_lte_MaxAppliedSerial_in_ticket_leaderDoneEarlyOnSessionStuff"

			// kill the session so client must make a new one?
			//delete(s.state.SessTable, tkt.SessionID)
			//s.sessByExpiry.Delete(ste)
			//s.saver.save(s.state)

			s.respondToClientTicketApplied(tkt)
			doneEarly = true
			return
		}
	} // end only analyze writes

	if tkt.SessionSerial > ste.HighestSerialSeenFromClient {
		ste.HighestSerialSeenFromClient = tkt.SessionSerial
	}

	return
}

// purge what the client acknowledges they no longer need
func (s *TubeNode) cleanupAcked(ste *SessionTableEntry, deleteBelow int64) {
	if deleteBelow <= 0 {
		return
	}
	for ser, tkt := range ste.Serial2Ticket.All() {
		if ser < deleteBelow {
			//vv("%v cleanupAcked: deleting acked SessionSerial %v in SessionID '%v'", s.name, ser, tkt.SessionID)
			delete(ste.ticketID2tkt, tkt.TicketID)
			ste.Serial2Ticket.Delkey(ser)
		} else {
			// since we are ordered by serial,
			// all the others will be higher and we
			// can stop now.
			break
		}
	}
}

// when frag.FragOp ObserveMembershipChange is seen by
// a PeerServiceName == TUBE_OBS_MEMBERS node that
// has created a circuit to a peer to observe membership
// changes.
func (s *TubeNode) handleObserveMembershipChange(
	frag *rpc.Fragment,
	ckt *rpc.Circuit,
) {

	mc := &MemberConfig{}
	_, err := mc.UnmarshalMsg(frag.Payload)
	panicOn(err)
	//vv("%v sees handleObserveMembershipChange() mc='%v'", s.name, mc)
}

// starting is not modified, read-only.
func (s *TubeNode) addRemoveToMemberConfig(tkt *Ticket, starting *MemberConfig, atRaftLogIndex int64) (newConfig *MemberConfig) {

	newConfig = starting.CloneForUpdate(s)

	// now apply the change (add one, or remove one node) specified in tkt.
	switch {
	case tkt.AddPeerName != "":
		_, already := newConfig.PeerNames.Get2(tkt.AddPeerName)
		if already {
			// allow no-op config changes since logical races may happen.
			return // newConfig ready to go then.
			//tkt.Err = fmt.Errorf("changeMembership error: nothing to do, as AddPeerID '%v' is already in curConfig: '%v'", rpc.AliasDecode(tkt.AddPeerID), newConfig)
			//break
		}
		cktP, ok := s.cktall[tkt.AddPeerID]
		if ok {

			det := &PeerDetail{
				Name:                   tkt.AddPeerName,
				PeerID:                 cktP.PeerID, //or tkt.AddPeerID ?
				PeerServiceName:        cktP.ckt.RemoteServiceName,
				PeerServiceNameVersion: cktP.ckt.RemotePeerServiceNameVersion,
			}
			s.setAddrURL(det, cktP)

			newConfig.setNameDetail(tkt.AddPeerName, det, s)
			s.state.Known.PeerNames.Set(tkt.AddPeerName, det)

			if tkt.AddPeerID != cktP.PeerID {
				panic("which is more up to date?")
			}
		} else {
			//tkt.Err = fmt.Errorf("changeMembership error: AddPeerID '%v' not found in cktall: '%v'", rpc.AliasDecode(tkt.AddPeerID), s.cktall)
			//panic(tkt.Err)

			det := &PeerDetail{
				Name: tkt.AddPeerName,
				//URL:    tkt.AddPeerID, // wrong. hmmm "boot.blank"?
				URL:    "boot.blank",
				PeerID: tkt.AddPeerID,
				Addr:   tkt.AddPeerBaseServerHostPort,
			}
			newConfig.setNameDetail(tkt.AddPeerName, det, s)
			s.state.Known.PeerNames.Set(tkt.AddPeerName, det)
		}
	case tkt.RemovePeerName != "":
		_, already := newConfig.PeerNames.Get2(tkt.RemovePeerName)
		if !already {
			// allow logical no-ops b/c changes might be racing from
			// different clients and that should not crash us.
			return
		}
		newConfig.PeerNames.Delkey(tkt.RemovePeerName)
	}
	return
}

// helper
func (s *TubeNode) SendOneWay(ckt *rpc.Circuit, frag *rpc.Fragment, errWriteDur time.Duration, keepFragIfPositive int) (err error) {
	npay := len(frag.Payload)
	if npay > rpc.UserMaxPayload {
		panicf("npay(%v) > rpc.UserMaxPayload(%v) cannot send this large a message! frag.FragSubject='%v'; frag.FragOp='%v'", npay, rpc.UserMaxPayload, frag.FragSubject, frag.FragOp)
	}
	var anew bool
	anew, err = ckt.SendOneWay(frag, errWriteDur, keepFragIfPositive)
	_ = anew
	if err != nil {
		alwaysPrintf("%v SendOneWay: non nil error on '%v': '%v'", s.me(), frag.FragSubject, err)
		if s.wasConnRefused(err, ckt) {
			return
		}
	}
	return
}

// called by <-newCircuitCh
// called by peerJoin()
func (s *TubeNode) addToCktall(ckt *rpc.Circuit) (cktP *cktPlus, rejected bool) {
	//defer func() {
	//vv("%v addToCktall finished processing ckt.RemotePeerName='%v' RemotePeerID='%v'", s.me(), ckt.RemotePeerName, ckt.RemotePeerID)
	//}()

	//if ckt.RemotePeerName == s.name {
	// we might want to allow this to let in-process
	// tubecli clients talk to the tube/replica sub-system,
	// and helper.go creates it in its search to reach
	// "everyone". but bah! we want to use a direct
	// channel rather than a circuit in that case.
	// but bah! we cannot always know if we are leader or not.
	//panic(fmt.Sprintf("%v huh? self-loop? use a channel! ckt.RemotePeerName==s.name; ckt='%v'", s.name, ckt))
	//}

	// new replaces old can create an infinite restart loop.
	// 057 demonstrated that.
	// decide to keep one that sortes lexicographically lower
	// based on cktID.

	oldCktP, haveOld := s.cktAllByName[ckt.RemotePeerName]
	if haveOld && oldCktP.PeerServiceName == ckt.RemoteServiceName {
		if oldCktP.PeerServiceNameVersion > ckt.RemotePeerServiceNameVersion {
			alwaysPrintf("%v dropping update to RemotePeerName='%v'; PeerServiceName='%v' with version '%v' that is stale versus our current version '%v'", s.name, ckt.RemotePeerName, oldCktP.PeerServiceName, ckt.RemotePeerServiceNameVersion, oldCktP.PeerServiceNameVersion)
			rejected = true
			cktP = nil
			return
		}
	}
	// moved from <-incomingNewCkt :
	// can be logically racy, don't freak out.
	oldCktP, haveOld = s.cktall[ckt.RemotePeerID]
	if haveOld && oldCktP.ckt.CircuitID == ckt.CircuitID { // && oldCktP.ckt == ckt {
		//vv("%v ignoring redunant notice of circuitID from '%v'", s.me(), ckt.RemotePeerName) // not seen 057
		cktP = oldCktP
		rejected = true
		return
	}
	// INVAR: any oldCktP has a different ckt.CircuitID, or a different instance

	samePeerID := false
	if haveOld && oldCktP.PeerID == ckt.RemotePeerID {
		samePeerID = true
		//vv("%v oldCktP.PeerID(%v) == ckt.RemotePeerID; and CircuitID differ=%v: will keep the lower lex sorting of circuitID", s.me(), oldCktP.PeerID, oldCktP.ckt.CircuitID != ckt.CircuitID) // not seen 057
	}
	if haveOld {
		//vv("%v addToCktall have old ckt from '%v'", s.name, ckt.RemotePeerName) // not seen 057
		if samePeerID {
			if oldCktP.ckt.CircuitID != ckt.CircuitID {

				// keep the smaller circuit. arg. still problems.
				// what if we keep at most 2 circuits? the lower
				// one as the primary, the other one just not
				// killed? we can read from it but don't send on it.
				// call them "dreadOnlyParked2ndCkt". Yes, just
				// NOT closing the ckt below makes 057 happier.
				// Ugh. still recycling drat. try keep all, but only
				// sending on the latest?

				//if oldCktP.ckt.CircuitID > ckt.CircuitID {
				//vv("%v doing cleanup old ckt: have old ckt for ckt.RemotePeerID = '%v'; oldCkt = %p'", s.name, alias(ckt.RemotePeerID), oldCktP) // not seen on 057 now that we have suppressWatchdogs() called in handleAE.

				// make room for new
				s.deleteFromCktAll(oldCktP)
				//s.parkedInsert(oldCktP)
				//oldCktP.ckt.Close(nil)

				//} else {
				//	return oldCktP // and ignore the new one.
				//}
			} else {
				//vv("%v same circuit we already have, just ignore dup call to addToCktall for '%v'", s.name, oldCktP.PeerName) // not seen 057

				if ckt != oldCktP.ckt {
					panic(fmt.Sprintf("how did we get two different ckt that look the same? ckt='%v'; oldCktP.ckt = '%v'", ckt, oldCktP.ckt))
				}
				cktP = oldCktP
				rejected = true
				return
			}
		} else {
			//vv("%v addToCktall: PeerID is different for '%v', let us take the new one only.", s.me(), oldCktP.PeerName) // not seen 057
			s.deleteFromCktAll(oldCktP)
			//s.parkedInsert(oldCktP)
			//oldCktP.ckt.Close(nil)
		}
	} else {
		//vv("%v addToCktall we see completely new ckt from '%v'; RemotePeerID='%v'", s.name, ckt.RemotePeerName, ckt.RemotePeerID)
	}
	cktP = s.newCktPlus(ckt.RemotePeerName, ckt.RemoteServiceName)
	cktP.ckt = ckt
	cktP.PeerID = ckt.RemotePeerID
	cktP.PeerServiceName = ckt.RemoteServiceName
	cktP.PeerBaseServerAddr = ckt.RpbTo.BaseServerAddr
	cktP.PeerServiceNameVersion = ckt.RemotePeerServiceNameVersion
	// moved below to getting more accurate reporting: cktP.startWatchdog()

	rpc.AliasRegister(ckt.RemotePeerID, ckt.RemotePeerName)

	s.cktall[ckt.RemotePeerID] = cktP
	s.cktAllByName[cktP.PeerName] = cktP

	// avoid spurious "-" down (and spurious pending)
	// on node in watchdog report by
	// adding to ctkall and cktAllByName first (just above),
	// and only afterwards starting the watchdog.
	//vv("%v we have added cktP.PeerName='%v' to cktAllByName (and ctkall); calling startWatchdog in addToCktall on cktP=%p for '%v'", s.me(), cktP.PeerName, cktP, cktP.PeerName)

	// surely we do not want to do this for all the clients!
	if cktP.PeerServiceName == TUBE_REPLICA {
		// in fact, if we are a client ourselves, no watchdog. Only Raft replicas.
		if s.PeerServiceName == TUBE_REPLICA {
			//vv("%v cktP.PeerServiceName='%v' calling startWatchdog!", s.me(), cktP.PeerServiceName)
			cktP.startWatchdog() // ckt non-nil means isUp=true
		}
	}
	cktP.seen(nil, 0, 0, 0) // in addToCktall

	// ==========================
	// s.cktReplica and member config are updated below.

	// Not actually sure we want this, but it sure _seems_
	// like a good idea to keep things up to
	// date as circuits come in.
	// ==========================

	if cktP.PeerName == s.name {
		// probably never hit...? just be sure:
		return // never want self in cktReplica (for below)
	}
	// INVAR: cktP.PeerName != s.name

	// update cktReplica[peerID] with latest? Well,
	// only if this ckt.RemotePeerName is in our current
	// MC... and even then, not sure desired...
	if s.state == nil || s.state.MC == nil {
		return
	}
	detail, present := s.state.MC.PeerNames.Get2(ckt.RemotePeerName)
	_ = detail
	if !present {
		// this is a bit of a logical race/chicken-and-egg.
		// Our ckt comes first, then the fragment with the MC
		// that says we are in the membership. So when
		// we update MC, we also have to copy from cktall
		// to cktReplica.

		// this peer name is not in the member config; we
		// won't add it until we get an explicit command to do so.
		//vv("%v addToCktall rejecting ckt.RemotePeerName='%v' for cktReplica since not in current MC(%v)", s.name, ckt.RemotePeerName, s.state.MC.Short())
		return
	}
	// INVAR: the new ckt peer is in the current member config.
	// below: really, update CktReplica and MC.

	// update s.state.MC.PeerNames detail, otherwise
	// boot.blank persists and that is useless in recovery.
	updatedDetail := &PeerDetail{
		Name: ckt.RemotePeerName,
		//URL: setAddrURL() below sets this
		PeerID:                 ckt.RemotePeerID,
		PeerServiceName:        ckt.RemoteServiceName,
		PeerServiceNameVersion: ckt.RemotePeerServiceNameVersion,
		//Addr:                   ckt.RpbTo.BaseServerAddr,
		//Addr: ckt.RpbTo.NetAddr,
	}
	s.setAddrURL(updatedDetail, cktP)

	// could just be same; check first.
	if updatedDetail.differs(detail) {
		//vv("%v on new ckt in addToCktall(), updating detail for '%v' from '%v' -> to '%v'", s.name, ckt.RemotePeerName, detail, updatedDetail)

		s.state.MC.setNameDetail(ckt.RemotePeerName, updatedDetail, s)
		// in addToCktall here.
		// have a call to s.adjustCktReplicaForNewMembership() ?
	}
	s.state.Known.PeerNames.Set(ckt.RemotePeerName, updatedDetail)

	if ckt.RemoteServiceName == TUBE_REPLICA {
		s.cktReplica[cktP.PeerID] = cktP // cktReplica-write (?)

		// update s.peers too?
		if s.peers == nil {
			s.peers = make(map[string]*RaftNodeInfo)
		} else {
			if haveOld {
				oldFoll, haveOldInPeers := s.peers[oldCktP.PeerID]
				_ = oldFoll
				if haveOldInPeers {
					alwaysPrintf("%v replacing s.peers oldFoll for '%v'; oldCktP.PeerID='%v'", s.name, oldCktP.PeerName, oldCktP.PeerID)
					delete(s.peers, oldCktP.PeerID)
				}
			}
		}
		s.peers[cktP.PeerID] = s.newRaftNodeInfo(cktP.PeerID, cktP.PeerName, cktP.PeerServiceName, cktP.PeerServiceNameVersion)
	}

	return
} // end addToCktall

func keys[V any](m map[string]V) (ks []string) {
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return
}
func (s *TubeNode) setAddrURL(det *PeerDetail, cktP *cktPlus) (url0 string) {
	//return cktP0.baseServerCircuitURL()
	//return ckt.RemoteCircuitURL()
	//return ckt.RemotePeerURL()
	addr := cktP.ckt.RpbTo.BaseServerAddr
	if addr == "" {
		addr = cktP.ckt.RpbTo.NetAddr
		if addr == "" {
			panic("must have some NetAddr or addr")
		}
	}
	url0 = cktP.ckt.RemoteBaseServerNoCktURL(addr)
	//vv("%v using addr = '%v'; url0 = '%v'", s.name, addr, url0)
	//if strings.Contains(url0, "auto-cli-from") {
	//	vv("%v where is auto-cli in URL from? stack='%v'", s.name, stack())
	//}

	// catch problems early
	_, err := url.Parse(url0)
	panicOn(err)

	det.URL = url0
	det.Addr = addr
	return
}

// thinclone() all that should be needed to allow
// clusterup_test.go:73 to not race
// with tube.go:2781, since we send cktPlus
// to the test to let it verify it is a replica.
func (s *cktPlus) thinclone() *cktPlus {
	return &cktPlus{
		sn:                     atomic.AddInt64(&nextCktPlusSN, 1),
		PeerServiceName:        s.PeerServiceName,
		PeerName:               s.PeerName,
		PeerServiceNameVersion: s.PeerServiceNameVersion,
		PeerID:                 s.PeerID,
		PeerBaseServerAddr:     s.PeerBaseServerAddr,
		ckt:                    s.ckt, // is only a data race if the test fails.
		//isReplica:       s.isReplica,
	}
}

var nextCktPlusSN int64 // keep them distinct in the rb.Tree

type cktPlus struct {
	sn int64
	// node lets the watchdog goro vv debug print
	node *TubeNode

	PeerName               string
	PeerServiceName        string
	PeerServiceNameVersion string

	PeerID             string
	PeerBaseServerAddr string
	ckt                *rpc.Circuit
	Addr               string

	// single watchdog per cktP manages such stuff now
	//isPending bool // we have requested, but not back yet.
	//pendingTm time.Time
	//pendingAttempts int

	// must be able to confirm a previous
	// call has completed failed/cancelled or else
	// we will try to auto-cli the same name 2x
	// and our simnet will properly crash us for that.
	//pending                          atomic.Bool

	perCktWatchdogHalt *idem.Halter

	// must hold requestReconnectMut while accessing
	// requestReconnect or requestReconnectLastBeganTm
	requestReconnectMut         sync.Mutex
	requestReconnect            *packReconnect
	requestReconnectLastBeganTm time.Time
	requestReconnectPulse       chan bool

	atomicIsUp atomic.Bool

	// indicate the address is bad or completely
	// down (at the moment) and so the watchdog has shut down.
	atomicConnRefused atomic.Bool

	isGone bool // died/left/network connection severed.
	goneTm time.Time

	lastSeenMut sync.Mutex // protect lastSeenTm
	// AE, AEack, tallyPre, tallyVote via seen() call.
	lastSeenTm time.Time

	latestCancFuncMut sync.Mutex
	latestCancFunc    func()

	MC           *MemberConfig
	LastLogIndex int64
	LastLogTerm  int64
	CurrentTerm  int64

	stalledOnSeenTkt []*Ticket
}

func (s *TubeNode) newCktPlus(peerName, peerServiceName string) *cktPlus {
	//vv("%v newCktPlus, caller: '%v'; peerName='%v', peerServiceName='%v'", s.me(), fileLine(2), peerName, peerServiceName)
	c := &cktPlus{
		sn:                 atomic.AddInt64(&nextCktPlusSN, 1),
		node:               s,
		PeerName:           peerName,
		PeerServiceName:    peerServiceName,
		perCktWatchdogHalt: idem.NewHalter(),

		requestReconnectPulse: make(chan bool),
	}
	s.Halt.AddChild(c.perCktWatchdogHalt)
	return c
}

func (c *cktPlus) Close() {
	// caller include :1244 in <-s.electionTimeoutCh:
	//vv("%v cktPlus.Close called by stack=\n\n%v\n", c.node.me(), stack())
	c.perCktWatchdogHalt.RequestStop()

	c.latestCancFuncMut.Lock()
	cancelFunc := c.latestCancFunc
	c.latestCancFuncMut.Unlock()
	if cancelFunc != nil {
		cancelFunc()
	}
	select {
	case <-c.perCktWatchdogHalt.Done.Chan:
	case <-c.node.Halt.ReqStop.Chan:
	case <-time.After(time.Millisecond * 10):
	}
}

func (cktP *cktPlus) baseServerCircuitURL() string {
	if cktP.ckt == nil {
		return cktP.PeerBaseServerAddr
	}
	//if cktP.PeerBaseServerAddr == "" {
	//	return cktP.ckt.RemoteCircuitURL()
	//}
	//return cktP.ckt.RemoteServerURL(cktP.PeerBaseServerAddr)
	return cktP.ckt.RemoteBaseServerNoCktURL(cktP.PeerBaseServerAddr)
}

func (cktP *cktPlus) isReplica() bool {
	return cktP.PeerServiceName == TUBE_REPLICA
}

func (s *cktPlus) String() string {
	if s.ckt != nil {
		return fmt.Sprintf(`&cktPlus{ckt: {(remote)PeerName: %v, PeerID: %v, CircuitID: %v, isPending:%v, PeerBaseServerAddr:%v}}`, s.PeerName, s.PeerID, s.ckt.CircuitID, s.isPending(), s.PeerBaseServerAddr)
	}
	return fmt.Sprintf(`&cktPlus{ckt: {(remote)PeerName: %v, PeerID: %v, ckt:nil, isPending:%v, PeerBaseServerAddr:%v}}`, s.PeerName, s.PeerID, s.isPending(), s.PeerBaseServerAddr)
}

// if not "up", assuming pending. See isPending() below.
func (c *cktPlus) up() (ans bool) {
	return c.atomicIsUp.Load()
}

func (c *cktPlus) isPending() (ans bool) {
	return !c.atomicIsUp.Load()
}

func (s *MemberConfig) hasNamePeerID(peerName, peerID string) bool {
	detail, ok := s.PeerNames.Get2(peerName)
	if ok {
		if detail.PeerID == peerID {
			return true
		}
	}
	return false
}

// called on reboot to refresh our own address;
// in case we become leader we are ready to go.
func (s *TubeNode) updateSelfAddressInMemberConfig(mc *MemberConfig) (changed bool) {
	det, present := mc.PeerNames.Get2(s.name)
	if !present {
		// cluster may not want us in the peer membership at the moment.
		return
	}
	if det.URL == s.URL {
		// looks good, no update needed
		return
	}
	changed = true
	updatedDetail := &PeerDetail{
		Name:                   s.name,
		URL:                    s.URL,
		PeerID:                 s.PeerID,
		PeerServiceName:        s.PeerServiceName,
		PeerServiceNameVersion: s.PeerServiceNameVersion,
		Addr:                   s.MyPeer.NetAddr,
	}
	if s.MyPeer.BaseServerAddr != "" {
		updatedDetail.Addr = s.MyPeer.BaseServerAddr
	}
	mc.setNameDetail(s.name, updatedDetail, s)
	s.state.Known.PeerNames.Set(s.name, updatedDetail)

	return
}

func (a *PeerDetail) differs(b *PeerDetail) bool {
	if a.Name != b.Name {
		return true
	}
	if a.URL != b.URL {
		return true
	}
	if a.PeerID != b.PeerID {
		return true
	}
	if a.Addr != b.Addr {
		return true
	}
	if a.PeerServiceName != b.PeerServiceName {
		return true
	}
	if a.PeerServiceNameVersion != b.PeerServiceNameVersion {
		return true
	}
	return false
}

// 1st key is peerName, 2nd key is sn
func newParkedTree() map[string]map[int64]*cktPlus {
	return make(map[string]map[int64]*cktPlus)
}
func (tkt *Ticket) addRmString() string {
	if tkt.AddPeerName != "" {
		return fmt.Sprintf("add node '%v'", tkt.AddPeerName)
	}
	return fmt.Sprintf("remove node '%v'", tkt.RemovePeerName)
}

func peerNamesAsString(peers map[string]*RaftNodeInfo) (r string) {
	i := 0
	for _, info := range peers {
		if i > 0 {
			r += ", "
		}
		r += fmt.Sprintf("%v", info.PeerName)
		i++
	}
	return
}

// called on leader by case RedirectTicketToLeaderMsg,
// by dispatchAwaitingLeaderTickets(),
// and by resubmitStalledTickets().
// Calls s.replicateTicket(tkt) at the end.
func (s *TubeNode) commandSpecificLocalActionsThenReplicateTicket(tkt *Ticket, fromWhom string) {
	//vv("%v top commandSpecificLocalActionsThenReplicateTicket; fromWhom='%v'; tkt='%v'", s.me(), fromWhom, tkt.Short())
	if s.role != LEADER {
		panic("only for leader; followers should not be calling here.")
	}
	switch tkt.Op {
	case WRITE:
		// a failed lease write turns into a read anyway,
		// so for a fast path: if we are leasing, do a fast local
		// read if we can, and if the write would not win anway just
		// return the read, keeping it local only (especially
		// under highly contended election winning writes).
		if tkt.Leasor != "" && tkt.LeaseRequestDur > 0 &&
			s.leaderServedLocalRead(tkt, true) {

			//vv("%v failed lease write turned into local fast read", s.name)
			tkt.Stage += ":RedirectTicketToLeaderMsg_leaderServedLocalRead_true_failed_lease_write"
			return
		}

	case MEMBERSHIP_SET_UPDATE:
		s.changeMembership(tkt)
		//vv("%v changeMembership finished, returning from commandSpecificLocalActionsThenReplicateTicket without doing replicateTicket().", s.me())
		return
	case READ, SHOW_KEYS, READ_KEYRANGE, READ_PREFIX_RANGE:
		if s.leaderServedLocalRead(tkt, false) {
			//vv("%v leaderServedLocalRead true, returning early! tkt='%v'", s.name, tkt.Short())
			tkt.Stage += ":RedirectTicketToLeaderMsg_leaderServedLocalRead_true"
			return
		} else {
			tkt.Stage += ":RedirectTicketToLeaderMsg_leaderServedLocalRead_false_calling_replicateTicket"
			// fallthrough to replicateTicket
		}

	case ADD_SHADOW_NON_VOTING:
		_, isMember := s.state.MC.PeerNames.Get2(tkt.AddPeerName)
		if isMember {
			tkt.Err = fmt.Errorf("node is already in replica membership MC, so cannot use ADD_SHADOW_NON_VOTING to add them to ShadowReplica: '%v'", tkt.AddPeerName)
			s.respondToClientTicketApplied(tkt)
			s.FinishTicket(tkt, true)
			return
		}
	// fallthrough to replicateTicket

	case REMOVE_SHADOW_NON_VOTING:
		_, isShadow := s.state.ShadowReplicas.PeerNames.Get2(tkt.RemovePeerName)
		if !isShadow {
			tkt.Err = fmt.Errorf("node is not in ShadowReplicas, so cannot use REMOVE_SHADOW_NON_VOTING to remove them: '%v'", tkt.RemovePeerName)
			s.respondToClientTicketApplied(tkt)
			s.FinishTicket(tkt, true)
			return
		}
		// we should stop any watchdog for them:

		cktP, ok := s.cktAllByName[tkt.RemovePeerName]
		if ok && cktP != nil {
			s.deleteFromCktAll(cktP)
			vv("REMOVE_SHADOW_NON_VOTING: did deleteFromCktAll(cktP) for '%v'", tkt.RemovePeerName)
			// this adds stuff back... skip unless we figure out we need it.
			//s.adjustCktReplicaForNewMembership()
		}

	// fallthrough to replicateTicket

	case USER_DEFINED_FSM_OP:
		if err := s.doUserDefinedLegitCheck(tkt); err != nil {
			tkt.Err = err
			s.respondToClientTicketApplied(tkt)
			s.FinishTicket(tkt, true)
			return
		}

	case SESS_NEW:
		s.leaderSideCreateNewSess(tkt)
		// fallthrough to replicateTicket
	}

	//vv("%v %v -> commandSpecificLocalActionsThenReplicateTicket about to call replicateTicket() for tkt=%v", s.me(), fromWhom, tkt)

	s.replicateTicket(tkt)
}

// If we are good to reconfigure, returns nil.
// Returns a non-nil error if we are not good to go.
// Our caller should reject back to the (remote/local) client
// who requested the MC udpate; or stall.
// PRE: we are leader.
// PRE: the newMC.ConfigVersion has already been incremented,
// so we do not adjust it again (even on error).
//
// See the blog discussion here in addition to the paper.
// https://will62794.github.io/distributed-systems/consensus/2025/08/25/logless-raft.html
// https://arxiv.org/abs/2102.11960 (the paper).
// newMC can be nil if we just checking if the
// current MC has been committed.
func (s *TubeNode) mongoLeaderCanReconfig(curMC, newMC *MemberConfig, inCurrentConfigCount, inCurTermCount int) (err error) {
	if s.role != LEADER {
		panic("not leader")
		return fmt.Errorf("mongoLeaderCanReconfig error on '%v': we are not leader.", s.name)
	}
	// page 6 of "Design and Analysis of a Logless
	// Dynamic Reconfiguration Protocol"
	// by William Schultz et al. where C is the current MemberConfig (MC).
	//
	// Q1. Config Quorum Check: There must be a quorum of
	// servers in C.m that are currently in configuration C.
	need := curMC.majority()
	if inCurrentConfigCount < need {
		return fmt.Errorf("Q1 unmet, mongoLeaderCanReconfig cannot reconfig on '%v': inCurrentConfigCount(%v) < curMC.majority(%v); curMC='%v'; newMC='%v'", s.name, inCurrentConfigCount, need, curMC.Short(), newMC.Short())
	}

	// Q2. Term Quorum Check: There must be a quorum of
	// servers in C.m that are currently in term T.
	if inCurTermCount < need {
		return fmt.Errorf("Q2 unmet, mongoLeaderCanReconfig cannot reconfig on '%v': inCurTermCount(%v) < curMC.majority(%v); curMC='%v'; newMC='%v'", s.name, inCurTermCount, need, curMC.Short(), newMC.Short())
	}

	// P1. Oplog Commitment: All oplog entries committed
	// in terms <= T must be committed on some quorum of servers in C.m.
	//
	// Obviously technically this is impossible, since old
	// entries are probably committed with a very old and
	// gone set of servers. But the intent is
	// ensure that all future leaders must have all
	// committed entries (the Leader Completeness property
	// of Raft, chapter 3, page 14, Figure 3.2). So noop0
	// should suffice.

	// I think the noop0 takes care of that.
	// in mongoLeaderCanReconfig() here.
	if !s.initialNoop0HasCommitted {
		return fmt.Errorf("P1 unmet, mongoLeaderCanReconfig cannot reconfig on '%v' since initialNoop0HasCommitted is false", s.name)
	}

	if newMC != nil {
		if !quorumsOverlap(curMC, newMC) {
			return fmt.Errorf("quroum overlap condition unmet, mongoLeaderCanReconfig cannot reconfig on '%v'; curMC='%v'; newMC='%v'", s.name, curMC.Short(), newMC.Short())
		}
	}
	return nil
}

// QuorumsOverlap(mi, mj), where mi and mj
// are sets of peers in a membership config,
// the property is defined as:
//
// For any quorum subset qi from Quorums(mi),
// and any quorum subset qj from Quorums(mj),
// the intersection(qi, qj) is not empty.
//
// The overlap property holds if the sum
// of the two quorum sizes is greater
// than the total number of unique members,
// (the size of the union of mi and mj)
// by the pigeonhole principle:
//
// a) all members are drawn from the union set;
//
// b) the two quorums of size need0 and need1
// can try to be as different as possible;
//
// c) but if together their number when
// distinct exceeds the union of choices, then
//
// d) we conclude there must be at least
// one member in the intersection of the
// two subsets, by the pigeonhole principle.
func quorumsOverlap(curMC, newMC *MemberConfig) bool {
	need0 := curMC.majority()
	need1 := newMC.majority()

	// special case handling of transition to/from empty set.
	// No waiting needed.
	if need0 == 0 || need1 == 0 {
		return true
	}

	curN := curMC.PeerNames.Len()
	newN := newMC.PeerNames.Len()

	intersection := make(map[string]bool)
	if curN < newN {
		// curN is smaller
		for name := range curMC.PeerNames.All() {
			_, ok := newMC.PeerNames.Get2(name)
			if ok {
				intersection[name] = true
			}
		}
	} else {
		// newN is smaller
		for name := range newMC.PeerNames.All() {
			_, ok := curMC.PeerNames.Get2(name)
			if ok {
				intersection[name] = true
			}
		}
	}
	intersectionSz := len(intersection)
	unionSz := curN + newN - intersectionSz

	ans := (need0 + need1) > unionSz
	//vv("intersecionSz = %v; unionSz = %v; need0 = %v, need1 = %v; returning ans = (need0 + need1) > unionSz == %v", intersectionSz, unionSz, need0, need1, ans)
	return ans
}

// used by tubecli.go to dump wal and show cur state.
func (s *TubeNode) SetState(state *RaftState) {
	s.state = state
}

func (s *TubeNode) isFollowerKaput() bool {
	if s.role != FOLLOWER {
		return false
	}
	if !s.state.MC.IsCommitted {
		return false
	}
	_, weAreInCurConfig := s.state.MC.PeerNames.Get2(s.name)
	if weAreInCurConfig {
		return false
	}
	_, weAreShadow := s.state.ShadowReplicas.PeerNames.Get2(s.name)
	return !weAreShadow
}

// called on leader election timeout on leader.
func (s *TubeNode) leaderShouldStepDownBecauseNotInCurCommittedMC() bool {
	curCommitted := s.onLeaderIsCurrentMCcommitted()
	if !curCommitted {
		return false
	}
	_, weAreInCurConfig := s.state.MC.PeerNames.Get2(s.name)
	return !weAreInCurConfig
}

// called from above and seen().
func (s *TubeNode) onLeaderIsCurrentMCcommitted() (curCommited bool) {
	//vv("%v top of onLeaderIsCurrentMCcommitted(), s.me())

	if s.role != LEADER {
		panic("IsCurrentMCcommitted should only be called on leader")
	}
	// INVAR: we are leader.

	if s.state.MC == nil {
		return false
	}
	if s.state.MC.IsCommitted {
		return true
	}

	curConfig := s.state.MC

	inCurrentConfigCount := 1 // for self (note we are never in cktAllByName)
	inCurTermCount := 1       // for self (we always have our current MC).
	if !s.weAreMemberOfCurrentMC() {
		// per section 4.2.2, do not count ourselves if
		// we are being removed, even though we are leader.
		inCurrentConfigCount = 0
		inCurTermCount = 0
	}
	curTerm := s.state.CurrentTerm
	for peerName, _ := range curConfig.PeerNames.All() {
		cktP0, ok := s.cktAllByName[peerName]
		if ok && cktP0.ckt != nil {
			if peerName == s.name {
				// sanity check our inCurrentConfigCount logic.
				panic("impossible, we never add self to cktAllByName")
			}
			if cktP0.MC != nil {
				if cktP0.MC.VersionEqual(curConfig) {
					inCurrentConfigCount++
				}
				if cktP0.CurrentTerm == curTerm {
					inCurTermCount++
				}
			}
		}
	}
	// mongo-raft-reconfig safety checks that current config has
	// been "loglessly committed", which is the equivalent of
	// Raft-log-committed in the cluster (on a quorum). This
	// is based on the MC from AE acks (update: not pre/vote tallies)
	// as submitted via cktPlus.seen(mc) when we see those messages.
	err := s.mongoLeaderCanReconfig(curConfig, nil, inCurrentConfigCount, inCurTermCount)
	if err != nil {
		//vv("%v mongoLeaderCanReconfig gave err = '%v'", s.me(), err)
		// curCommited stays false
		// weShouldStepDown stays false
		return false
	}
	// the "logless commit" has been observed:
	// a quorum of servers meet the RECONFIG
	// conditions Q1,Q2,P1 of Algorithm 1 (page 6)
	// of the Mongo paper.
	s.state.MC.IsCommitted = true
	s.updateMCindex(s.state.CommitIndex, s.state.CommitIndexEntryTerm)

	return true
}

func (s *TubeNode) mergeCktPToKnown(cktP *cktPlus) {
	addr := cktP.ckt.RpbTo.BaseServerAddr
	if addr == "" {
		addr = cktP.ckt.RpbTo.NetAddr
	}
	ckt := cktP.ckt
	det := &PeerDetail{
		Name:                   ckt.RemotePeerName,
		PeerID:                 ckt.RemotePeerID,
		PeerServiceName:        ckt.RemoteServiceName,
		PeerServiceNameVersion: ckt.RemotePeerServiceNameVersion,
		Addr:                   addr,
	}
	// in mergeCktPToKnown here
	s.state.Known.PeerNames.Set(det.Name, det)
}

func (a *MemberConfig) merge(b *MemberConfig) {
	if b == nil || a == nil {
		return
	}
	for name, det := range b.PeerNames.All() {
		_, ok := a.PeerNames.Get2(name)
		if ok {
			// already have name. which to keep?
			// Avoid new one if it has no info; is pending,
			// otherwise adopt it, since is probably
			// a snapshot from leader who is more actively
			// keeping connections up.
			if det.URL != "pending" {
				a.PeerNames.Set(name, det)
			}
			continue
		}
		// totally new peer, add it.
		a.PeerNames.Set(name, det)
	}
}

func (s *TubeNode) updateMCindex(commitIndex, commitIndexEntryTerm int64) {
	if s == nil || s.state == nil || s.state.MC == nil {
		return
	}
	if commitIndex > s.state.MC.CommitIndex {
		s.state.MC.CommitIndex = commitIndex
		s.state.MC.CommitIndexEntryTerm = commitIndexEntryTerm
	}
}

func (s *TubeNode) handleRequestStateSnapshot(frag *rpc.Fragment, ckt *rpc.Circuit, caller string) {
	//vv("%v top of handleRequestStateSnapshot(), caller='%v' to '%v'", s.name, caller, ckt.RemotePeerName) // seen 065

	// do we want to set these here? yes I think so!
	// The recipient will reference them and install
	// them to their wal. but problem is, these do not
	// match our compression level yet! so restore them after!
	// also a good place to assert they are up to date
	if s.state.CompactionDiscardedLast.Index != s.wal.logIndex.BaseC {
		panic("should be in sync")
	}
	if s.state.CompactionDiscardedLast.Term != s.wal.logIndex.CompactTerm {
		panic("should be in sync")
	}

	// getStateSnapshot() will set CompactionDiscardedLastIndex/Term
	// correctly so we don't roll back applied log entries.
	snap := s.getStateSnapshot()

	bts, err := snap.MarshalMsg(nil)
	panicOn(err)

	// convention/encoding for FragPart for state Snapshots:
	//fragEnc.FragPart = 0 // all in one Fragment.
	//fragEnc.FragPart = 1 // more to come, this is part 1, 2, 3, ...
	//fragEnc.FragPart = -4 or neg of max // the is the last of more to come.
	// so typically either just 0, or: 1,2,3,-4
	// (there should never need be a -1 on FragPart because that means the same as 0).
	// so anytime we see FragPart <= 0, we know after appending to
	// any prior stuff, we are done.

	npay := len(bts)
	const mx = rpc.UserMaxPayload
	if npay > mx {
		vv("why so large?? snap.Footprint = %v", snap.Footprint())

		// accumulate the checksum of the whole to make sure we
		// sequenced it right.
		h := blake3.New(64, nil)
		h.Write(bts)
		b3checksumWhole := blake3ToString33B(h)

		var part int64
		left := bts
		for len(left) > 0 {
			frag := s.newFrag()
			frag.FragOp = StateSnapshotEnclosed
			frag.FragSubject = "StateSnapshotEnclosed"
			frag.SetUserArg("leader", s.leaderID)
			frag.SetUserArg("leaderName", s.leaderName)
			frag.SetUserArg("b3checksumWhole", b3checksumWhole)

			part++ // start at 1
			frag.FragPart = part
			sendme := left[:min(len(left), mx)]
			left = left[len(sendme):]
			frag.Payload = sendme
			if len(left) == 0 {
				// negative means we are done after this last part
				// (and 0 means not multi-part)
				frag.FragPart = -frag.FragPart
			}
			s.SendOneWay(ckt, frag, -1, 0)
		}
		vv("note: send snapshot in %v parts", part)
	} else {
		fragEnc := s.newFrag()
		fragEnc.FragOp = StateSnapshotEnclosed
		fragEnc.FragSubject = "StateSnapshotEnclosed"
		fragEnc.SetUserArg("leader", s.leaderID)
		fragEnc.SetUserArg("leaderName", s.leaderName)

		// just one part
		fragEnc.FragPart = 0 // redundant; for emphasis.
		fragEnc.Payload = bts
		s.SendOneWay(ckt, fragEnc, -1, 0)
	}
}

func (s *TubeNode) resetToNoSnapshotInProgress() {
	s.snapInProgressLastPart = 0
	s.snapInProgressB3checksumWhole = ""
	s.snapInProgressHasher = nil
	s.snapInProgress = nil
}

func (s *TubeNode) handleStateSnapshotEnclosed(frag *rpc.Fragment, ckt *rpc.Circuit, caller string) {

	part := frag.FragPart

	// if part != 0, snapshot was too big for
	// one message and sender is sending it in multiple parts.

	b3checksumWhole, ok := frag.GetUserArg("b3checksumWhole")
	if !ok && part != 0 {
		panicf("multi-part snapshots must have b3checksumWhole set on their fragments")
	}

	switch {
	case part == 1:
		// first of a new multipart snapshot
		s.snapInProgressLastPart = part
		s.snapInProgressB3checksumWhole = b3checksumWhole
		s.snapInProgressHasher = blake3.New(64, nil)
		s.snapInProgressHasher.Write(frag.Payload)
		s.snapInProgress = append([]byte{}, frag.Payload...)
	case part > 1:
		// next of a multipart
		if s.snapInProgressB3checksumWhole != b3checksumWhole {
			alwaysPrintf("part is from a different overall snapshot: discard and start over; s.snapInProgressB3checksumWhole(%v) != b3checksumWhole(%v)", s.snapInProgressB3checksumWhole, b3checksumWhole)
			s.resetToNoSnapshotInProgress()
			return
		}
		if part != s.snapInProgressLastPart+1 {
			alwaysPrintf("parts out of order s.snapInProgressLastPart=%v but this next =%v. discard and ignore.", s.snapInProgressLastPart, part)
			s.resetToNoSnapshotInProgress()
			return
		}
		s.snapInProgressLastPart = part
		s.snapInProgressHasher.Write(frag.Payload)
		s.snapInProgress = append(s.snapInProgress, frag.Payload...)

	case part < 0:
		// last of a multipart snapshot

		if s.snapInProgressB3checksumWhole != b3checksumWhole {
			alwaysPrintf("part is from a different overall snapshot: discard and start over")
			s.resetToNoSnapshotInProgress()
			return
		}
		if -part != s.snapInProgressLastPart+1 {
			alwaysPrintf("parts out of order s.snapInProgressLastPart=%v but this next -part=%v", s.snapInProgressLastPart, -part)
			s.resetToNoSnapshotInProgress()
			return
		}

		h := s.snapInProgressHasher
		h.Write(frag.Payload)
		b3checksumWhole2 := blake3ToString33B(h)
		if b3checksumWhole2 != b3checksumWhole {
			alwaysPrintf("re-assembly checksum failure: b3checksumWhole2 = '%v' but b3checksumWhole='%v'", b3checksumWhole2, b3checksumWhole)
			s.resetToNoSnapshotInProgress()
			return
		}
		s.snapInProgress = append(s.snapInProgress, frag.Payload...)

		state2 := &RaftState{}
		_, err := state2.UnmarshalMsg(s.snapInProgress)
		panicOn(err)
		s.applyNewStateSnapshot(state2, caller)
		vv("applied %v part state snapshot", -part)
		// and reset.
		s.resetToNoSnapshotInProgress()
		return

	case part == 0:
		// single part
		state2 := &RaftState{}
		_, err := state2.UnmarshalMsg(frag.Payload)
		panicOn(err)
		s.applyNewStateSnapshot(state2, caller)
		vv("applied single part state snapshot")
		s.resetToNoSnapshotInProgress()
		return
	}
}

// arg. this is fragile; we forgot to add ShadowReplicas
// at first... why cannot we just apply the full thing,
// instead of going field by field? maybe we were
// just wanting to assert before to figure out
// how stuff was getting lost, but we already fixed that.
//
// Well, we do want to merge the two Known sets, but besides
// that...
//
// called when <-s.ApplyNewStateSnapshotCh; and when
// above handleStateSnapshotEnclosed.
func (s *TubeNode) applyNewStateSnapshot(state2 *RaftState, caller string) {
	//vv("%v top of applyNewStateSnapshot; caller='%v'; will set s.state.CommitIndex from %v -> %v; ", s.me(), caller, s.state.CommitIndex, state2.CommitIndex) // not seen 065

	// consider this real scenario where this next (commented) assert wedged us, killing the 2 followers after a leader was elected. I think we must allow older state + longer logs to overwrite nestate snapshot but insufficient logs to surpass the leader's log.
	//    ovh node_2
	//                CommitIndex: 39173,
	//       CommitIndexEntryTerm: 5,
	//                LastApplied: 39173,
	//               lastLogIndex: 40075, from term 5.
	//
	//    aorus node_6
	//                CommitIndex: 39173,
	//       CommitIndexEntryTerm: 5,
	//                LastApplied: 39173,
	//            LastAppliedTerm: 5,
	//               LastLogIndex: 41474, from term 5
	//
	//    rog node_1
	//                CommitIndex: 39073,
	//       CommitIndexEntryTerm: 5,
	//                LastApplied: 39073, <<< has the longest log, so elected. but others will not accept this snapshot!!? i think they must
	//            LastAppliedTerm: 5,
	//               LastLogIndex: 41573 from term 5  (when node_1 was leader)
	//
	//if state2.CommitIndex < s.state.CommitIndex {
	//	panic(fmt.Sprintf("%v we should never be rolling back CommitIndex with state snapshots! state2.CommitIndex(%v) < s.state.CommitIndex(%v)", s.name, state2.CommitIndex, s.state.CommitIndex))
	// same kind of reasoning for this:
	//}
	//if state2.CurrentTerm < s.state.CurrentTerm {
	//	panic(fmt.Sprintf("%v we should never be rolling back Terms with state snapshots! state2.CurrentTerm(%v) < s.state.CurrentTerm(%v)", s.name, state2.CurrentTerm, s.state.CurrentTerm))
	//}

	s.state.CurrentTerm = state2.CurrentTerm

	s.state.CommitIndex = state2.CommitIndex
	s.state.CommitIndexEntryTerm = state2.CommitIndexEntryTerm
	s.state.LastApplied = state2.LastApplied
	s.state.LastAppliedTerm = state2.LastAppliedTerm

	// KVstore could have been emptied at some point in the
	// history, so we need to allow this in general. we already
	// caught the earlier bug with this heuristic, maybe, and it
	// is not a current concern.
	//	if state2.KVstore != nil {
	//		// try to catch where our state is getting blown away in prod local/ test
	//		if len(s.state.KVstore.m) > 0 && len(state2.KVstore.m) == 0 {
	//			panic(fmt.Sprintf("arg! why are we blowing away our KVstore in a snapshot application from caller '%v'\n new state2='%v'\n\nexisting state='%v'\n", caller, state2, s.state))
	//		}
	//		s.state.KVstore = state2.KVstore
	//	}
	if state2.MC != nil {
		s.state.MC = state2.MC
	}
	if state2.ShadowReplicas != nil {
		s.state.ShadowReplicas = state2.ShadowReplicas
	}
	if s.state.Known == nil {
		s.state.Known = state2.Known
	} else {
		s.state.Known.merge(state2.Known)
	}
	s.state.Known.merge(s.state.MC)
	s.state.LastSaveTimestamp = state2.LastSaveTimestamp

	//compactIndex := state2.CompactionDiscardedLast.Index
	//compactTerm := state2.CompactionDiscardedLast.Term

	//vv("%v snapshot about to update s.state.CompactionDiscardedLastIndex from %v -> %v; and s.state.CompactionDiscardedLast.Term from %v -> %v", s.name, s.state.CompactionDiscardedLastIndex, compactIndex, s.state.CompactionDiscardedLast.Term, compactTerm)

	// as the scenario above illustrates, we must, actually.
	//if compactIndex < s.state.CompactionDiscardedLast.Index {
	//	panic(fmt.Sprintf("%v we should never be rolling back commits with state snapshots! compactIndex(%v) < s.state.CompactionDiscardedLastIndex(%v)", s.name, compactIndex, s.state.CompactionDiscardedLast.Index))
	//}
	s.state.CompactionDiscardedLast = state2.CompactionDiscardedLast

	// then we must force the wal to match the new snapshot: this
	// is what s.wal.installedSnapshot() does below.

	// but we do want too: s.wal.logIndex.BaseC = s.state.CompactionDiscardedLast.Index
	// which wal.installedSnapshot() just below sets for us, and we assert
	// immediately afterwards.

	//vv("%v snapshot set s.state.CompactionDiscardedLastIndex=%v; s.state.CompactionDiscardedLast.Term=%v", s.name, s.state.CompactionDiscardedLastIndex, s.state.CompactionDiscardedLast.Term)

	if state2.SessTable != nil {
		s.state.SessTable = state2.SessTable
	}

	s.saver.save(s.state)
	s.wal.installedSnapshot(s.state)
	s.assertCompactOK()

	vv("%v end of applyNewStateSnapshot. good: s.wal.index.BaseC(%v) == s.state.CompactionDiscardedLastIndex; logIndex.Endi=%v ; wal.lli=%v", s.me(), s.wal.logIndex.BaseC, s.wal.logIndex.Endi, s.wal.lli)
}

// properly set CompactionDiscardedLastIndex/Term
// on a clone of s.state so the recipient can
// install it and correctly update their
// wal.logIndex.BaseC/Term. This is  essential
// to being log-compaction aware.
func (s *TubeNode) getStateSnapshot() (snapshot *RaftState) {

	// restore these below
	idx0 := s.state.CompactionDiscardedLast.Index
	term0 := s.state.CompactionDiscardedLast.Term

	// why is LastApplied the right point? well, its
	// the only thing that reflects our current KVstore state.
	// even though we might have more in our log.
	// Can we ever compress more than LastApplied? no, that
	// should be impossible!
	s.state.CompactionDiscardedLast.Index = s.state.LastApplied
	// seems like this might skew out of sync.
	//s.state.CompactionDiscardedLast.Term = s.state.CurrentTerm
	s.state.CompactionDiscardedLast.Term = s.state.LastAppliedTerm

	snapshot = s.state.clone()

	// restore afterwards, so we don't get confused about
	// what we have locally (on leader here) compacted.
	s.state.CompactionDiscardedLast.Index = idx0
	s.state.CompactionDiscardedLast.Term = term0

	return
}

func (s *TubeNode) weAreMemberOfCurrentMC() bool {
	if s == nil || s.state == nil || s.state.MC == nil {
		return false
	}
	_, ok := s.state.MC.PeerNames.Get2(s.name)
	return ok
}

// enable a single node cluster to recognize
// that it has been removed from membership
// and finish up.
func (s *TubeNode) observerOnlyNow() bool {
	if s.state == nil {
		return false
	}
	_, isObs := s.state.Observers.PeerNames.Get2(s.name)
	if !isObs {
		return false
	}
	if s.state.MC == nil {
		return false
	}
	if s.state.MC.PeerNames.Len() > 0 &&
		!s.state.MC.IsCommitted {
		return false
	}
	_, ok := s.state.MC.PeerNames.Get2(s.name)
	if ok {
		return false
	}
	alwaysPrintf("%v ABOUT TO RETURN(finish TubeNode.Start): since we are Observer only and not in the current MC='%v'", s.me(), s.state.MC.Short())
	return true
}

func (s *TubeNode) amShadowReplica() bool {
	if s.state == nil {
		return false
	}
	if s.PeerServiceName != TUBE_REPLICA {
		return false
	}
	if s.state.ShadowReplicas == nil {
		return false
	}
	_, isShadow := s.state.ShadowReplicas.PeerNames.Get2(s.name)
	return isShadow
}

func (s *TubeNode) refreshSession(from time.Time, ste *SessionTableEntry) (refreshTil time.Time) {
	// refresh the session, even reads should avoid expiry

	// we must delete and re-add the entry every
	// time we update the sess.SessionEndxTm in
	// order to get the s.sessByExpiry.tree to rebalance.
	// Still only O(log N) each time.
	s.sessByExpiry.Delete(ste)
	refreshTil = from.Add(ste.SessRequestedInitialDur)
	ste.SessionEndxTm = refreshTil
	s.sessByExpiry.tree.Insert(ste)
	return
}

// called by case RedirectTicketToLeaderMsg
func (s *TubeNode) bootstrappedOrForcedMembership(tkt *Ticket) bool {
	// allow bootstrapping by handling ADD of self here.
	//vv("%v top bootstrappedOrForcedMembership", s.name)

	// we can be leader but not in membership and
	// need to allow ourselves to be added back
	// to the membership. so don't do this:
	//if s.role == LEADER {
	//	return false
	//}
	if s.PeerServiceName != TUBE_REPLICA {
		//vv("%v I am not a replica", s.me())
		return false
	}
	if tkt.Op != MEMBERSHIP_SET_UPDATE {
		//vv("%v tkt.Op is not membership update", s.me())
		return false
	}
	calledOnLeader := s.role == LEADER // for FinishTicket below too.

	if tkt.ForceChangeMC {
		return s.forceChangeMC(tkt, calledOnLeader)
	}
	if tkt.AddPeerName == "" {
		//vv("%v tkt.Op is not membership Add peer", s.me())
		return false
	}
	n := s.state.MC.PeerNames.Len()
	//	if n > 1 {
	if n > 0 {
		//vv("%v not just me; MC=%v", s.name, s.state.MC)
		return false
	}
	// We experimented with letting a node add another
	// node, but then we don't have the connection details
	// to give out, and they might be offline and stay
	// offline. (Hopefully we avoid having the list
	// of drowned sailors be the members of the
	// part time parliament...)
	if tkt.AddPeerName != "" && tkt.AddPeerName != s.name {
		//vv("%v not trying to add self to MC", s.me())
		return false
	}
	if tkt.RemovePeerName != "" && tkt.RemovePeerName != s.name {
		//vv("%v not trying to remove self to MC", s.me())
		return false
	}
	if n == 1 {
		_, justMe := s.state.MC.PeerNames.Get2(s.name)
		if !justMe {
			//vv("%v not just me 2; MC=%v", s.name, s.state.MC)
			return false
		}
	}
	// empty MC, or just Me.

	detail := s.getMyDetails()
	s.state.MC.setNameDetail(s.name, detail, s)
	s.state.Known.PeerNames.Set(s.name, detail)

	s.state.ShadowReplicas.PeerNames.Delkey(s.name)
	//vv("%v bootstrapped rather than redirectToLeader. now MC='%v'", s.me(), s.state.MC)
	if s.role != LEADER {
		s.becomeLeader()
	}
	s.addInspectionToTicket(tkt)
	// this does s.FinishTicket(tkt) only if WaitingAtLeader
	s.respondToClientTicketApplied(tkt)
	s.FinishTicket(tkt, calledOnLeader)
	return true
}

func (s *TubeNode) getMyDetails() *PeerDetail {
	return &PeerDetail{
		Name:                   s.name,
		URL:                    s.URL,
		PeerID:                 s.PeerID,
		Addr:                   s.MyPeer.BaseServerAddr,
		PeerServiceName:        s.MyPeer.PeerServiceName,
		PeerServiceNameVersion: s.MyPeer.PeerServiceNameVersion,
	}
}

// helper for bootstrappedOrForcedMembership
// to handle forced MC changes; tubeadd -f and
// tuberm -f to resurrect a flattened cluster
// when some needed member is permanently gone.
func (s *TubeNode) forceChangeMC(tkt *Ticket, calledOnLeader bool) bool {
	if s.PeerServiceName != TUBE_REPLICA {
		//vv("%v I am not a replica", s.me())
		return false
	}
	if tkt.Op != MEMBERSHIP_SET_UPDATE {
		//vv("%v tkt.Op is not membership update", s.me())
		return false
	}
	if !tkt.ForceChangeMC {
		return false
	}
	if tkt.RemovePeerName == "" && tkt.AddPeerName == "" {
		panicf("what other new kind of membership change is there? tkt='%v'", tkt)
		return false
	}
	isAdd := true
	target := tkt.AddPeerName
	if target == "" {
		isAdd = false
		target = tkt.RemovePeerName
	}
	// needed?
	detailTarget, have := s.state.MC.PeerNames.Get2(target)
	_ = detailTarget
	if isAdd {
		if have {
			// return done, is no-op
			tkt.Err = fmt.Errorf("forceChangeMC ADD is no-op, target already present: target='%v'; MC='%v'", target, s.state.MC)
			s.respondToClientTicketApplied(tkt)
			return true
		}
	} else {
		if !have {
			// return done, is no-op
			tkt.Err = fmt.Errorf("forceChangeMC ADD is no-op, target already absent: target='%v'; MC='%v'", target, s.state.MC)
			s.respondToClientTicketApplied(tkt)
			return true
		}
	}
	// yes, confirmed we do need to force add/remove
	// INVAR: target is in s.state.MC, if removing.
	// and is not, if adding.
	changeMade := false
	defer func() {
		if changeMade {
			s.saver.save(s.state)
		}
	}()

	if tkt.AddPeerName != "" {
		if tkt.AddPeerName == s.name {

			detail := s.getMyDetails()

			s.state.MC.ConfigVersion++
			s.state.MC.setNameDetail(s.name, detail, s)
			s.state.ShadowReplicas.PeerNames.Delkey(s.name)
			s.state.Known.PeerNames.Set(s.name, detail)
			changeMade = true

			//vv("%v forcing MC add rather than redirectToLeader. added me='%v'; now MC='%v'", s.me(), s.name, s.state.MC)

			s.respondToClientTicketApplied(tkt)
			return true
		}
		// add, not me though.
		// where do details come from?
		// we can try Shadow and Known... else error out?
		// or put pending? I mean Shadow and Known
		// might have good host:port, so worth trying.
		detail, ok := s.state.ShadowReplicas.PeerNames.Get2(target)
		if !ok {
			detail, ok = s.state.Known.PeerNames.Get2(target)
		}
		if ok {

			s.state.MC.ConfigVersion++
			s.state.MC.setNameDetail(target, detail, s)
			s.state.ShadowReplicas.PeerNames.Delkey(target)
			changeMade = true

			//vv("%v forcing MC add rather than redirectToLeader. target='%v'; now MC='%v'", s.me(), target, s.state.MC)

			s.respondToClientTicketApplied(tkt)
			return true
		}

		tkt.Err = fmt.Errorf("forceChangeMC aborted, no details of how to add target are available from Known or ShadowReplicas; target = '%v'", target)
		s.respondToClientTicketApplied(tkt)
		return true
	}
	// remove
	if tkt.AddPeerName == s.name {
		// remove myself

		detail := s.getMyDetails()

		s.state.MC.ConfigVersion++
		s.state.MC.PeerNames.Delkey(s.name)

		// assume if we are force removing that
		// the node is dead and gone, and we do not
		// want them lingering in shadows either, so comment out:
		//s.state.ShadowReplicas.PeerNames.Set(s.name, detail)

		s.state.Known.PeerNames.Set(s.name, detail)
		changeMade = true

		//vv("%v forcing MC remove of myself('%v') rather than redirectToLeader. now MC='%v'", s.me(), s.name, s.state.MC)

		s.respondToClientTicketApplied(tkt)
		return true
	}
	// remove, not me
	s.state.MC.ConfigVersion++
	s.state.MC.PeerNames.Delkey(target)
	changeMade = true

	// assume if we are force removing that
	// the node is dead and gone, and we do not
	// want them lingering in shadows either, so comment out:
	//s.state.ShadowReplicas.PeerNames.Set(target, detailTarget)

	//vv("%v forcing MC remove of non-self target rather than redirectToLeader. target='%v'; now MC='%v'", s.me(), target, s.state.MC)

	s.respondToClientTicketApplied(tkt)
	return true
}

func (s *TubeNode) clearFromObservers(name string) {
	if s == nil || s.state == nil {
		return
	}
	if s.state.Observers == nil {
		s.state.Observers = s.NewMemberConfig("clearFromObserverse")
		return
	}
	_, ok := s.state.Observers.PeerNames.Get2(name)
	if !ok {
		return
	}
	s.state.Observers.PeerNames.Delkey(name)
}

func (s *TubeNode) InjectEmptyMC(ctx context.Context, targetURL, nodeName string) (err error) {
	emptyFrag := s.newFrag()
	emptyFrag.FragOp = InstallEmptyMC
	emptyFrag.FragSubject = "InstallEmptyMC"
	emptyFrag.SetUserArg("target", nodeName)
	_, _, _, err = s.getCircuitToLeader(ctx, targetURL, emptyFrag, false)
	return
}

// ifLostTicketTellClient:
// Important client interaction detail--when
// can you report an error back to the client!
//
// from https://thesquareplanet.com/blog/students-guide-to-raft/
// how do you know when a client operation has completed?
// In the case of no failures, this is simple – you just wait
// for the thing you put into the log to come back out
// (i.e., be passed to apply()). When that happens, you
// return the result to the client. However, what happens
// if there are failures?
//
// For example, you may have been the leader when the client
// initially contacted you, but someone else has since been
// elected, and the client request you put in the log has
// been discarded. Clearly you need to have the client try
// again, but how do you know when to tell them about the error?
//
// One simple way to solve this problem is to record where
// in the Raft log the client’s operation appears when you
// insert it. Once the operation at that index is sent to
// apply(), you can tell whether or not the client's
// operation succeeded based on whether the operation
// that came up for that index is in fact the one you
// put there. If it isn't, a failure has happened and
// an error can be returned to the client.
func (s *TubeNode) ifLostTicketTellClient(calledOnLeader bool) {

	var waiting *imap
	if calledOnLeader {
		waiting = s.WaitingAtLeader
	} else {
		waiting = s.WaitingAtFollow
	}

	// yeilds ascending tkt.LogIndex order
	for _, tkt := range waiting.all() {

		if tkt.LogIndex > s.state.LastApplied {
			// all others will be higher LogIndex values.
			return
		}
		// INVAR: tkt.LogIndex <= LastApplied

		// compacted away?
		baseC := s.wal.getCompactBase()
		if tkt.LogIndex <= baseC {
			if tkt.Op != SESS_END && tkt.Op != NOOP {
				alwaysPrintf("tell client of problem: we still have a ticket in waiting (onLead: %v), but its index has been compacted away and we should have replied by now! tkt='%v'", calledOnLeader, tkt)

				tkt.Err = fmt.Errorf("log truncation caused this ticket outcome to be lost. Client may need to resubmit request. TicketID = '%v' (Op: '%v')", tkt.TicketID, tkt.Op)
				tkt.Stage += ":truncated_wal_detected_in_ifLostTicketTellClient1" // bajillions of repeats on same ticket????

				s.respondToClientTicketApplied(tkt)
				s.FinishTicket(tkt, calledOnLeader)
			}
		}
		// INVAR: tkt.LogIndex > baseC
		entry, err := s.wal.GetEntry(tkt.LogIndex)
		if err != nil {
			// not in log yet...?
			continue
		}
		if entry.Ticket.TicketID != tkt.TicketID {
			alwaysPrintf("at tkt.LogIndex=%v, (%v)entry.Ticket.TicketID != our waiting tkt.TicketID(%v), so return error to client", tkt.LogIndex, entry.Ticket.TicketID, tkt.TicketID)
			tkt.Err = fmt.Errorf("leader fail/new leader election resulted in log truncation which caused this ticket to be lost. Client must resubmit request.")
			tkt.Stage += ":truncated_wal_detected_in_ifLostTicketTellClient2"

			s.respondToClientTicketApplied(tkt)
			s.FinishTicket(tkt, calledOnLeader)
		}
	}
}

func (s *TubeNode) doAddShadow(tkt *Ticket) {
	//vv("%v top of doAddShadow() tkt='%v'", s.me(), tkt.Short())
	detail := &PeerDetail{
		Name:                   tkt.AddPeerName,
		PeerID:                 tkt.AddPeerID,
		PeerServiceName:        tkt.AddPeerServiceName,
		PeerServiceNameVersion: tkt.AddPeerServiceNameVersion,
		Addr:                   tkt.AddPeerBaseServerHostPort,
		//URL:                    tkt.AddPeerURL ??
	}
	s.state.ShadowReplicas.PeerNames.Set(tkt.AddPeerName, detail)
	s.state.Known.PeerNames.Set(tkt.AddPeerName, detail)
	s.clearFromObservers(tkt.AddPeerName)
}

func (s *TubeNode) doRemoveShadow(tkt *Ticket) {
	//vv("%v top of doRemoveShadow() tkt='%v'", s.me(), tkt.Short())

	s.state.ShadowReplicas.PeerNames.Delkey(tkt.RemovePeerName)
	cktP, ok := s.cktAllByName[tkt.RemovePeerName]
	if ok && cktP != nil {
		s.deleteFromCktAll(cktP)
	}
}

// For user defined library operations we will need
// MarshalMessage/UnmarshalMessage for serz of state.
// Provide a way to define an op code an associated callback functions.
// The op code to be stored in tkt.UserDefinedOpCode.
// LegitCheckOp() to preliminary check against existing
// leader state, and ApplyCommittedOp() once the
// operation has been committed and is being applied.
// How initiated in the first place? ReplicateOp() perhaps.
// Internally, send the Ticket on s.writeReqCh.
//
// Also register a callback for when membership
// changes, e.g. hermes protocol use case. Something like
// RegisterMemberChangeCallback().
func (s *TubeNode) doApplyUserDefinedOp(tkt *Ticket) {
	//vv("%v top of doApplyUserDefinedOp() tkt='%v'", s.me(), tkt.Short())
	_ = tkt.UserDefinedOpCode
}

func (s *TubeNode) doUserDefinedLegitCheck(tkt *Ticket) error {
	//vv("%v top of doUserDefinedLegitCheck() tkt='%v'", s.me(), tkt.Short())
	_ = tkt.UserDefinedOpCode
	return nil
}

func URLTrimCktID(url string) string {
	if url == "" || url == "pending" {
		return url
	}
	addr, service, peerID, _, err := rpc.ParsePeerURL(url)

	if err != nil {
		return url
	}
	return addr + "/" + service + "/" + peerID
}

/*
Q: Can the PAR checks be integrated with the
Pre-Vote of Raft? The Pre-Vote seems essential
for liveness and reduced leader churn anyway.

A: (gemini 2.5 pro)

Yes, integrating Protocol-Aware Recovery
(PAR) checks into Raft's Pre-Vote phase
is an excellent and highly logical design.
It's a natural point in the protocol to
add data integrity checks, strengthening
the cluster against both unnecessary
elections and data corruption in a
single, efficient step.

***

### Why Pre-Vote is the Perfect Place for PAR

The synergy between Pre-Vote and PAR
is very strong because they both serve
to vet a potential leader *before* it
can disrupt the cluster.

* **Pre-Vote asks:** "Is the cluster in
a state where it *needs* a new leader,
and is my log complete enough to win?"
This prevents partitioned nodes from
causing term churn.

* **PAR asks:** "Is my log data *correct*
and free from corruption?" This prevents
a node with damaged data from propagating it.

Combining them creates a comprehensive
check: "Am I a **valid and necessary**
candidate to become the next leader?"

Think of it like a job interview process.
Standard Pre-Vote is checking if you have
the minimum qualifications (an up-to-date log).
A PAR-enhanced Pre-Vote is like running
a background check at the same time to
ensure none of those qualifications
were falsified.

***

### How the Integration Would Work

The integration would involve augmenting the `PreVote` RPC and the corresponding logic.

1.  **Modified RPC:** The `PreVote` RPC would be expanded to include not just the candidate's term and last log index, but also **log validation information**. This could be a set of checksums for recent log entries or, more efficiently, a [Merkle root](https://en.wikipedia.org/wiki/Merkle_tree) of the log.

2.  **New Validation Step:** When a server receives a `PreVote` request, its checklist becomes:
  - Can I currently hear from a healthy leader? (Standard Pre-Vote check)
  - Is the candidate's log at least as up-to-date as mine? (Standard Raft check)
  - **New PAR Check:** Does the candidate's log validation info match my own? If the checksums or Merkle roots don't match, it signals data corruption on either the candidate's or the local server's log.

3.  **Informed Response:** If the PAR check fails, the server would deny the pre-vote. More importantly, it can flag that a corruption was detected, allowing both nodes to enter a PAR-style recovery or audit mode.

***

### The Major Advantages

This approach yields significant benefits for building a production-grade Raft implementation:

* **Early, Proactive Detection:** Corruption is found *before* a disruptive election begins, not after a corrupt leader has already been elected and is trying to replicate bad data.

* **Efficiency:** It leverages an existing protocol phase. Instead of adding a separate, periodic "log scrubbing" RPC, it piggybacks on the network traffic that already occurs when a node times out.

* **Prevents Corrupt Leaders:** A node with a damaged log will fail the Pre-Vote round. It learns about its own corruption and is prevented from ever starting a real election, let alone winning it and propagating bad data. This is a massive improvement to the system's overall safety.

Q2: Okay, so if PAR is done during the Pre-vote, does that then make the simpler reconfig algorithm viable again?

Yes, it does. Integrating Protocol-Aware Recovery (PAR) into the Pre-Vote phase makes the simpler reconfiguration algorithm viable again, even in a realistic fault model.

This combination works because it effectively restores the core assumption that the simpler algorithm was built on: **the leader's log can be trusted.**

---

### Restoring the "Trustworthy Leader" Assumption

The original problem was that a leader with a corrupt log could make disastrous decisions regarding cluster membership. The Pre-Vote+PAR combination acts as a powerful gatekeeper that prevents a corrupt node from ever gaining leadership in the first place.

* A node with a corrupt log will fail the PAR check during the Pre-Vote phase.
* Other servers will detect the data mismatch and deny its request to become a candidate.
* Therefore, any node that *does* successfully pass the Pre-Vote round and win an election has had its log validated by a quorum of its peers.

The leader isn't inherently infallible, but it has been **certified as trustworthy** by the election process itself. Because you can now trust the leader's log, the simple, authoritative logic of the reconfiguration algorithm becomes safe to use again.

---

### How It Solves the Failure Scenarios

Let's look at how this combination solves the specific problems we discussed:

1.  **The Corrupt Leader:** A node with a corrupt `C_new` entry starts a Pre-Vote. Healthy followers compare its log checksums with their own and find a mismatch. They reject the pre-vote, and the corrupt node is blocked from leadership. **The problem is prevented.**
2.  **The Incorrect Rollback:** A node with a damaged log that's *missing* a valid `C_new` starts a Pre-Vote. Healthy followers see that their logs are more up-to-date (and that the candidate's log hash is incorrect). They reject the pre-vote. A healthy node with the correct log will win the election instead. **The problem is prevented.**

In both cases, the cluster is protected because a faulty node is stopped before it can issue any commands, including configuration changes.

---

### The "Simplicity" Trade-Off

This brings up an important design point. While the *logic of the reconfiguration algorithm itself* remains simple, the overall system that supports it is now more complex. You've made a trade-off:

* You've **pushed complexity down** from the application-level feature (reconfiguration) into the core consensus protocol (elections).

This is generally a very good trade-off. By adding Pre-Vote and PAR, you've made the entire system more robust and resilient, which in turn allows higher-level logic like the simple reconfiguration algorithm to operate safely. The algorithm is "saved" because you've reinforced the foundation it stands on.

Q3: But what about liveness with the simpler reconfiguration algorithm and PAR in the Pre-vote? We've only discussed safety so far.

Yes, there is a liveness concern. The combination of PAR and the Pre-Vote phase intentionally trades liveness for safety. The system will halt and wait for operator intervention rather than risk making progress with potentially corrupt data.

This is a critical and desirable trade-off for any system that prioritizes data integrity.

---
## The Liveness vs. Safety Trade-Off

The core of the issue lies in the new, stricter requirements for electing a leader.

* **Standard Raft Liveness:** Requires a majority of servers to be online and able to communicate. It assumes their logs are correct.
* **PAR-Raft Liveness:** Requires a majority of servers to be online, able to communicate, AND have **provably consistent, uncorrupted logs**.

This second condition is much harder to meet. If storage faults create a situation where no majority of servers can agree on a single, non-corrupt version of the log, then no candidate will ever be able to pass the PAR-enhanced Pre-Vote phase. The cluster will be unable to elect a leader and will stop making forward progress.

This is a deliberate design choice. A system that safely stops is infinitely better than one that continues operating while corrupting your data. It’s like the emergency brake on a train: its purpose is to sacrifice liveness (movement) for the guarantee of safety.

---
## The Scenario for Lost Liveness

Liveness is lost if the cluster partitions
in such a way that no single partition
contains a majority of nodes with mutually consistent logs.

For example, in a five-node cluster `{A, B, C, D, E}`:
1.  A failure occurs during a membership change.
2.  Nodes `{A, B}` have one version of the log history.
3.  Nodes `{C, D}` have a slightly different, conflicting version due to bit rot on one of their disks.
4.  Node `E` is offline.

In this state, no leader can be elected. `{A, B}` can't get a majority. `{C, D}` can't get a majority. And their PAR checks against each other would fail anyway. The cluster would remain "stuck" until the issue is resolved.

---
## Restoring Liveness: The Role of the Operator

This "stuck" state is not permanent, but it requires
**human intervention**. This is the operational
contract of a PAR-hardened system.

When the cluster halts, monitoring and alerts (triggered by the failing PAR checks) would notify an operator. The operator would then:
1.  **Investigate** the state of the cluster's logs.
2.  **Identify** a node or backup that holds the "golden copy" of the data.
3.  **Repair** the corrupt nodes by restoring their state from the trusted source.
4.  **Restart** the repaired nodes.

Once a majority of servers are back online with consistent, non-corrupt logs, the Pre-Vote process will naturally succeed, a leader will be elected, and the cluster will automatically resume normal operation.
*/

func (s *TubeNode) errorOutAwaitingLeaderTooLongTickets() {
	if len(s.ticketsAwaitingLeader) == 0 {
		return
	}
	//vv("%v top errorOutAwaitingLeaderTooLongTickets()", s.me())
	// TODO: possible optimization would be to use
	// an imap.go tree indexed by deadline instead of
	// a simple go map, to avoid a linear scan
	// through all waiting tickets. 2nd order optimzation; defer for now.
	now := time.Now()
	var goner []string
	for ticketID, tkt := range s.ticketsAwaitingLeader {
		if tkt.WaitLeaderDeadline.IsZero() {
			// client did not specify an errWriteDur,
			// indicating that they wish to keep waiting
			// for as long as it takes.
			continue
		}
		if tkt.WaitLeaderDeadline.Before(now) {

			tkt.Err = fmt.Errorf("%v ticketsAwaitingLeader deadline passed (deadline='%v' < now='%v' by '%v'), releasing ticket '%v'", s.name, tkt.WaitLeaderDeadline, now, now.Sub(tkt.WaitLeaderDeadline), tkt.Short())
			//vv("%v", tkt.Err.Error())

			//s.respondToClientTicketApplied(tkt)
			s.replyToForwardedTicketWithError(tkt)
			s.FinishTicket(tkt, false)
			goner = append(goner, ticketID)
		}
	}
	// delete from map while iterating is not guaranteed
	// by the Go spec to mean we will see all keys (in
	// fact not mentioned at all) so cleanup afterwards
	// just to be safe.
	for _, ticketID := range goner {
		delete(s.ticketsAwaitingLeader, ticketID)
	}
}

// Use the key in a user arg when FragOp == PruneRedundantCircuit,
// the key maps to the value which holds the CircuitID to prune.
const pruneVictimKey = "pruneVictimCircuitID"
const pruneKeeperKey = "pruneKeeperCircuitID"

var ErrPruned = fmt.Errorf("pruned redundant circuit")

func (s *TubeNode) handlePruneRedundantCircuit(frag *rpc.Fragment) {
	victimCID, ok := frag.GetUserArg(pruneVictimKey)
	if !ok {
		panicf("expected to have user arg '%v' on frag: '%v'", pruneVictimKey, frag)
	}
	keeperCID, ok := frag.GetUserArg(pruneKeeperKey)
	if !ok {
		panicf("expected to have user arg '%v' on frag: '%v'", pruneKeeperKey, frag)
	}
	_, ok = debugGlobalCkt.Get(keeperCID)
	if !ok {
		vv("%v rejecting prune request: we do not have keeperCID '%v' in s.cktAudit.", s.name, keeperCID)
		return
	}
	cktV, ok := debugGlobalCkt.Get(victimCID)
	if ok {
		vv("%v acting on prune request: closing redundant circuit '%v'", s.name, cktV)
		// no! s.cktAuditByPeerID.Del(cktV.RemotePeerID)
		s.cktAuditByCID.Del(cktV.CircuitID)
		cktV.Close(ErrPruned)
	}
}

func (s *TubeNode) pruneRedundantCircuitMessageTo(sendOnCkt *rpc.Circuit, victimCID, keeperCID string) {
	frag := s.newFrag()
	frag.FragOp = PruneRedundantCircuit
	frag.FragSubject = "PruneRedundantCircuit"
	frag.SetUserArg(pruneVictimKey, victimCID)
	frag.SetUserArg(pruneKeeperKey, keeperCID)
	s.SendOneWay(sendOnCkt, frag, -1, 0)
}

// possible TODO: use Ticket system instead? We don't,
// for now, because this is intended as a rarely
// needed wedge-recovery operation for when quorum has been
// lost, and so it forgos the usually niceties.
// We certainly don't want to wait for consensus
// in such cases, as it will never happen due
// to the fact that we have already lost quorum.
// But, replying to a ticket could tell the
// operator about mis-specified targets (not us)
// to help them realize quickly what they need
// to ask for... currently it always looks like
// tuberm -e succeeds, even when it does not...
func (s *TubeNode) handleInstallEmptyMC(frag *rpc.Fragment, ckt *rpc.Circuit) {
	target, ok := frag.GetUserArg("target")
	if ok && target == s.name {
		alwaysPrintf("installing empty MC per request from '%v'", frag.FromPeerName)
		vers := s.state.MC.ConfigVersion
		s.state.MC = s.NewMemberConfig("InstallEmptyMC")
		s.state.MC.ConfigVersion = vers + 1
		s.saver.save(s.state)
	} else {
		alwaysPrintf("%v not for us! ignoring empty MC per request from '%v'", s.name, frag.FromPeerName)
	}
}

func (s *TubeNode) GetMyPeerDetail() *PeerDetail {
	return &PeerDetail{
		Name:                   s.name,
		URL:                    s.MyPeer.URL(),
		PeerID:                 s.MyPeer.PeerID,
		Addr:                   s.MyPeer.BaseServerAddr,
		PeerServiceName:        s.MyPeer.PeerServiceName,
		PeerServiceNameVersion: s.MyPeer.PeerServiceNameVersion,
		PID:                    int64(os.Getpid()),
		Hostname:               s.hostname,
	}
}

// figure out where s.state.CompactionDiscardedLastIndex
// is falling behind wal.logIndex.BaseC and getting out of sync. assert in AE:
// s.state.CompactionDiscardedLastIndex(1088) != s.wal.logIndex.BaseC(1187)
func (s *TubeNode) assertCompactOK() {
	if s == nil {
		return
	}
	if s.state.CompactionDiscardedLast.Index != s.wal.logIndex.BaseC {
		panicf("%v s.state.CompactionDiscardedLast.Index(%v) "+
			"!= s.wal.logIndex.BaseC(%v)",
			s.name,
			s.state.CompactionDiscardedLast.Index,
			s.wal.logIndex.BaseC)
	}
	if s.state.CompactionDiscardedLast.Term != s.wal.logIndex.CompactTerm {
		panicf("%v s.state.CompactionDiscardedLast.Term(%v) "+
			"!= s.wal.logIndex.CompactTerm(%v)",
			s.name,
			s.state.CompactionDiscardedLast.Term,
			s.wal.logIndex.CompactTerm)

	}
}
