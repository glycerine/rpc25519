package tube

import (
	"time"

	rpc "github.com/glycerine/rpc25519"
)

// keep the interface and implementations in sync.
var _ hoster = &TubeNode{}

//var _ hoster = &TubeSim{} // moved to raft_test.go

// hoster allows the handleAppendEntries()
// actual implementation to be tested
// under the simae_test.go raft_test.go
// testing framework.
type hoster interface {
	newAE() (ae *AppendEntries)
	newAEtoPeer(toPeerID, toPeerName, toPeerServiceName, toPeerServiceNameVersion string, sendThese []*RaftLogEntry) (ae *AppendEntries, foll *RaftNodeInfo)
	waitingSummary() (sum string, tot int)
	mustGetLastLogIndex() int64
	newRaftNodeInfo(peerID, peerName, peerServiceName, peerServiceNameVersion string) (info *RaftNodeInfo)
	getRaftLogSummary() (localFirstIndex, localFirstTerm, localLastIndex, localLastTerm int64)
	preVoteOn() bool
	ackAE(ack *AppendEntriesAck, ae *AppendEntries)
	becomeFollower(term int64, mc *MemberConfig, save bool)
	dispatchAwaitingLeaderTickets()
	resetElectionTimeout(where string) time.Duration
	commitWhatWeCan(leader bool)
	choice(format string, a ...interface{})
	logsAreMismatched(ae *AppendEntries) (reject bool, conflictTerm int64, conflictTerm1stIndex int64)
	handleAppendEntries(ae *AppendEntries, ckt0 *rpc.Circuit) (numOverwrote, numTruncated, numAppended int64)
	// specific to TubeNode or TubeSim
	//me() string
	//resetMethodCounters()
	//methodCallCounts() string
}
