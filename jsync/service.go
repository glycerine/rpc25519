package jsync

import (
	"context"
	"fmt"
	//myblake3 "github.com/glycerine/rpc25519/hash"
	//"github.com/glycerine/rpc25519/progress"
	//"io"
	"os"
	//"path/filepath"
	//"strconv"
	"sync"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/jcdc"
)

//go:generate greenpack -no-dedup=true

// Default_CDC is a package global to allow
// benchmarks comparing CDC chunkers and settings.
//
// At the moment, the choice must match on the Client and
// Server pair actually in use, as we have not
// implemented any reader-makes-right switching.
//
// In our benchmarks, FastCDC_PlakarAlgo results in
// the fewest bytes need to be updated.
var Default_CDC jcdc.CDCAlgo = jcdc.FastCDC_PlakarAlgo

// Default_CDC_Config was optimized in the jcdc
// benchmarks. The current 2KB min/8KB target/64KB max
// was found to minimize the total bytes of diff chunks
// transferred.
//
// Notes:
// [1] https://minkirri.apana.org.au/wiki/RollsumChunking
// recommends MinSize being at (or my take, less) than
// 25% of TargetSize, to allow faster re-alignement.
//
// The Microsoft Deduplication paper
// https://www.usenix.org/system/files/conference/atc12/atc12-final293.pdf
// recommends 64KB chunk sizes.
//
// Donovan Baarda in [1] points out that Microsoft
// was probably missing 20% of the dedup opporunities
// by have MaxSize too small and forcing truncations.
//
// But after actual benchmarking, the plakar defaults
// where the best: min 2KB, target 8KB, max 64KB, with
// the FastCDC_PlakarAlgo algo out performing all
// the others in terms of minimizing the deltas
// of total bytes changed.
var Default_CDC_Config = &jcdc.CDC_Config{
	MinSize:    2 * 1024,
	TargetSize: 8 * 1024,
	MaxSize:    64 * 1024,
}

// SyncService implements a file syncing
// service using an rsync-like protocol.
// Users register it as a peer using the
// Peer/Circuit/Fragment API.
//
//msgp:ignore SyncService
type SyncService struct {
	Halt *idem.Halter

	SyncPathRequestCh chan *RequestToSyncPath

	U rpc.UniversalCliSrv

	ServiceName string
}

// This file is the top-level starting point for
// the file syncing (rsync-like) protocol.
// The SyncService.Start() method launches
// Taker (taker.go) and Giver (giver.go).
//
// Taker and Giver implement finite-state-machines
// (FSM) that communicate over the wire to sync
// files. There are 16 FragOp fragment operations
// (message types) sent and/or received to
// implement both push and pull. The
// rpc25519 Peer/Circuit/Fragment API was designed to
// support this file synching protocol.
//
// Implementing both Push flows and Pull flows,
// the Taker and the Giver see the following
// flows (sequences of messages back and forth)
// through their state machines while syncing.
// During push, the local client is the Giver.
// During pull, the local client is the Taker.
// The rpc.Client and rpc.Server can act
// in either role, as they both implement the
// rpc.UniversalCliSrv interface.
//
// In the following flows, the "-> 1" means
// that FragOp 1 was received on the opposite
// side. For example, the push flow is initiated
// by the "giver -> 1" meaning that the giver
// sends OpRsync_RequestRemoteToTake and the
// taker reads it off the wire. The full
// table of FragOps follows the flows.
//
// push flows:
//          1 (RequestRemoteToTake)
// giver -> 1 -> err same file (giver returns)
// giver -> 1 -> 7 FileSizeModTimeMatch -> FIN (taker returns)
// giver -> 1 -> 8 LightRequestEnclosed(giver) giverSendsPlanAndData.. file checksums already match, yay. -> ToTakerMetaUpdateAtLeast (taker) -> FIN (giver returns)
// giver -> 1 -> 8 LightRequestEnclosed(giver) giverSendsPlanAndData -> 11,10,9 (taker) -> FileAllReadAck -> FIN (taker returns)
// giver -> 1 -> 2 NeedFullFile2(giver) giverSendsWholeFile -> 11 (file not found via SenderPlanEnclosed.FileIsDeleted) (taker deletes file) -> FIN (giver returns)
// giver -> 1 -> 2 NeedFullFile2(giver) giverSendsWholeFile -> FullFileBegin3,FullFileMore4,FullFileEnd5 (taker) -> FileAllReadAck -> FIN (taker returns)
//
//
// pull flows:
//
//          12 (RequestRemoteToGive)
//
// taker -> 12 (RequestRemoteToGive) giverSendsWholeFile -> as above... -> FIN giver returns
// taker -> 12 remoteGiverAreDiffChunksNeeded -> 13 TellTakerToDelete (taker deletes file) -> FIN (giver returns)
// taker -> 12 remoteGiverAreDiffChunksNeeded -> err same file(giver) -> FIN (taker returns)
// taker -> 12 remoteGiverAreDiffChunksNeeded -> FileSizeModTimeMatch(taker) -> FIN (giver returns)
// taker -> 12 remoteGiverAreDiffChunksNeeded giverSendsPlanAndData(giver) ... giverReportFileNotFound  -> (taker) SenderPlanEnclosed w/ SenderPlan.FileIsDeleted, taker deletes file -> FIN (giver returns)
// taker -> 12 remoteGiverAreDiffChunksNeeded giverSendsPlanAndData(giver) ... -> (taker) SenderPlanEnclosed,HeavyDiffChunksEnclosed,HeavyDiffChunksLast .. size 0 file -> (giver) FileAllReadAckToGiver -> FIN (taker returns)
// taker -> 12 remoteGiverAreDiffChunksNeeded giverSendsPlanAndData(giver) ... -> (taker) SenderPlanEnclosed,HeavyDiffChunksEnclosed,HeavyDiffChunksLast .. size >0 file .. taker saves plan,goalPreic -> (giver) FileAllReadAckToGiver -> FIN (taker returns)

// lazy taker -> 19 (LazyTakerWantsToPull) on giver.. not found|modTm match|no match -> (taker gets:) CallPeerError|OpRsync_FileSizeModTimeMatch|OpRsync_LazyTakerNoLuck_ChunksRequired, if no luck -> OpRsync_RequestRemoteToGive 12 (joins above flow)

// directory sync flows:
// (taker) -> 21 TakerRequestsDirSyncBegin -> (giver) 22 DirSyncBeginToTaker (enter flow below)
//
// (giver) -> 22 DirSyncBeginToTaker -> 23 DirSyncBeginReplyFromTaker -> 26/27/28 GiverSendsTopDirListing{|More|End} -> (giver) 29 TakerReadyForDirContents -> giver does individual file syncs (newly deleted files can be simply not transferred on the taker side to the new dir!) ... -> at end, giver -> DirSyncEndToTaker -> DirSyncEndAckFromTaker -> FIN.
//
// Both 21 and 22 should put the circuit into "dir-sync" mode wherein
// we ignore all the other FragOps unrelated to coordinating the top
// level directory sync. i.e. they call into their own subroutine
// FSMs. This partitions off the top-dir-sync circuit from any other
// individual file sync that mistakenly use the top-level circuitID.

// The only tricky/unique thing for directories, is that
// we need to delete files on the taker that are not
// on the giver. The giver won't know about these,
// since they do not have them.
// Other than that, we should be able to just push
// each file on the giver to the taker.
//
// So the switch over can be atomic, and not interfere
// with the parts trying to scan/update paths, the taker
// should create a new top-level versioned directory,
// and do all writes there. All reads of existing files
// come from the original still on disk. If no errors
// at the end, we can rename the new to old dir (possibly
// rename the old to old.backup to manually verify everything).

const (
	OpRsync_RequestRemoteToTake            = 1  // to taker
	OpRsync_ToGiverNeedFullFile2           = 2  // ... to Giver
	OpRsync_HereIsFullFileBegin3           = 3  // to taker
	OpRsync_HereIsFullFileMore4            = 4  // to taker
	OpRsync_HereIsFullFileEnd5             = 5  // to taker
	OpRsync_FileAllReadAckToGiver          = 6  // ... to Giver -> exit when recv.
	OpRsync_FileSizeModTimeMatch           = 7  // to taker -> exit when recv.
	OpRsync_LightRequestEnclosed           = 8  // ... to Giver (if needs more Chunk space, use 17/18 to follow up)
	OpRsync_HeavyDiffChunksEnclosed        = 9  // to taker
	OpRsync_HeavyDiffChunksLast            = 10 // to taker
	OpRsync_SenderPlanEnclosed             = 11 // to taker needs to send extra Chunks
	OpRsync_RequestRemoteToGive            = 12 // ... to Giver
	OpRsync_TellTakerToDelete              = 13 // to taker -> ack back fin to giver.
	OpRsync_ToTakerMetaUpdateAtLeast       = 14 // to taker (future feature, off atm.)
	OpRsync_AckBackFIN_ToTaker             = 15 // to taker -> exit when recv.
	OpRsync_AckBackFIN_ToGiver             = 16 // ... to Giver -> exit when recv.
	OpRsync_RequestRemoteToGive_ChunksMore = 17 // ... to Giver (if 8/12 Chunks too big)
	OpRsync_RequestRemoteToGive_ChunksLast = 18 // ... to Giver (end send 8/12 Chunks)
	OpRsync_LazyTakerWantsToPull           = 19 // ... to Giver, quick size + modTime check
	OpRsync_LazyTakerNoLuck_ChunksRequired = 20 // to taker (quick size/modTime failed)

	// top dir sync setup + directory listings transfer

	// (start) send my (takers) temp new top dir for paths to go into.
	// be sure to setup the new temp dir as separately as possible,
	// to avoid overlapping dir transfers having crosstalk.
	OpRsync_TakerRequestsDirSyncBegin = 21 // to giver, please send me 22,26/27/28

	// (start or reply to 21) to taker, please setup a tempdir and tell the
	// giver the path so we can send new files into that path.
	// If start, or reply: expect 23 back to establish write path; even
	// if redudundant in case of reply (keep it simple at first).
	// Later optimization: If reply to 21, they just gave us
	// write path. go directly to sending 26/27/28.
	OpRsync_DirSyncBeginToTaker        = 22 // to taker, please setup a top tempdir
	OpRsync_DirSyncBeginReplyFromTaker = 23 // to giver, here is my top tempdir

	// taker can rename the temp top dir/replace any old top dir.
	OpRsync_DirSyncEndToTaker = 24 // to taker, end of dir sync

	// giver can shut down all dir sync stuff.
	OpRsync_DirSyncEndAckFromTaker = 25 // to giver, ack end of dir sync

	OpRsync_GiverSendsTopDirListing     = 26 // to taker, here is my starting dir tree
	OpRsync_GiverSendsTopDirListingMore = 27 // to taker, here is more of 26
	OpRsync_GiverSendsTopDirListingEnd  = 28 // to taker, here is end of 26

	OpRsync_TakerReadyForDirContents = 29 // to giver, ready for individual file syncs
)

var once sync.Once

// AliasRsyncOps prints human readable names
// on fragment print-outs to ease reading
// and interpretation. As opposed to using
// a Stringer which would require casts
// everywhere, this makes the FragOp
// assignments much more readable.
func AliasRsyncOps() {
	rpc.FragOpRegister(OpRsync_RequestRemoteToTake, "OpRsync_RequestRemoteToTake")
	rpc.FragOpRegister(OpRsync_RequestRemoteToGive, "OpRsync_RequestRemoteToGive")
	rpc.FragOpRegister(OpRsync_ToGiverNeedFullFile2, "OpRsync_ToGiverNeedFullFile2")
	rpc.FragOpRegister(OpRsync_HereIsFullFileBegin3, "OpRsync_HereIsFullFileBegin3")
	rpc.FragOpRegister(OpRsync_HereIsFullFileMore4, "OpRsync_HereIsFullFileMore4")
	rpc.FragOpRegister(OpRsync_HereIsFullFileEnd5, "OpRsync_HereIsFullFileEnd5")
	rpc.FragOpRegister(OpRsync_FileAllReadAckToGiver, "OpRsync_FileAllReadAckToGiver")
	rpc.FragOpRegister(OpRsync_FileSizeModTimeMatch, "OpRsync_FileSizeModTimeMatch")
	rpc.FragOpRegister(OpRsync_LightRequestEnclosed, "OpRsync_LightRequestEnclosed")
	rpc.FragOpRegister(OpRsync_HeavyDiffChunksEnclosed, "OpRsync_HeavyDiffChunksEnclosed")
	rpc.FragOpRegister(OpRsync_HeavyDiffChunksLast, "OpRsync_HeavyDiffChunksLast")
	rpc.FragOpRegister(OpRsync_SenderPlanEnclosed, "OpRsync_SenderPlanEnclosed")
	rpc.FragOpRegister(OpRsync_TellTakerToDelete, "OpRsync_TellTakerToDelete")
	rpc.FragOpRegister(OpRsync_ToTakerMetaUpdateAtLeast, "OpRsync_ToTakerMetaUpdateAtLeast")
	rpc.FragOpRegister(OpRsync_AckBackFIN_ToTaker, "OpRsync_AckBackFIN_ToTaker")
	rpc.FragOpRegister(OpRsync_AckBackFIN_ToGiver, "OpRsync_AckBackFIN_ToGiver")

	// the adds to OpRsync_RequestRemoteToGive (12) when Chunks
	// won't fit in one Message.
	rpc.FragOpRegister(OpRsync_RequestRemoteToGive_ChunksMore, "OpRsync_RequestRemoteToGive_ChunksMore")
	rpc.FragOpRegister(OpRsync_RequestRemoteToGive_ChunksLast, "OpRsync_RequestRemoteToGive_ChunksLast")
	rpc.FragOpRegister(OpRsync_LazyTakerWantsToPull, "OpRsync_LazyTakerWantsToPull")
	rpc.FragOpRegister(OpRsync_LazyTakerNoLuck_ChunksRequired, "OpRsync_LazyTakerNoLuck_ChunksRequired")

	// top dir sync setup + directory listings transfer

	rpc.FragOpRegister(OpRsync_TakerRequestsDirSyncBegin, "OpRsync_TakerRequestsDirSyncBegin")

	rpc.FragOpRegister(OpRsync_DirSyncBeginToTaker, "OpRsync_DirSyncBeginToTaker")
	rpc.FragOpRegister(OpRsync_DirSyncBeginReplyFromTaker, "OpRsync_DirSyncBeginReplyFromTaker")
	rpc.FragOpRegister(OpRsync_DirSyncEndToTaker, "OpRsync_DirSyncEndToTaker")
	rpc.FragOpRegister(OpRsync_DirSyncEndAckFromTaker, "OpRsync_DirSyncEndAckFromTaker")

	rpc.FragOpRegister(OpRsync_GiverSendsTopDirListing, "OpRsync_GiverSendsTopDirListing")
	rpc.FragOpRegister(OpRsync_GiverSendsTopDirListingMore, "OpRsync_GiverSendsTopDirListingMore")
	rpc.FragOpRegister(OpRsync_GiverSendsTopDirListingEnd, "OpRsync_GiverSendsTopDirListingEnd")
	rpc.FragOpRegister(OpRsync_TakerReadyForDirContents, "OpRsync_TakerReadyForDirContents")
}

// NewRequestToSyncPath creates an empty
// RequestToSyncPath with a new Done channel set.
func NewRequestToSyncPath() *RequestToSyncPath {
	return &RequestToSyncPath{
		Done: idem.NewIdemCloseChan(),
	}
}

// RequestToSyncPath is the main bridge
// between user code and the file syncing service
// implementation. In essense, the user must:
//
// a) request := &RequestToSyncPath{} and set details/RemoteTakes to true/false
// b) reqs := make(chan *RequestToSyncPath)
// c) call RunRsyncService(..., reqs) to get a local peer.
// d) does reqs <- request to start the sync between local and remote peers.
//
// See the rpc25519/cmd/cli/client.go or
// the rpc25519/jsync/jsync.go or the tests
// in this package for examples.
//
// The RequestToSyncPath struct is used
// to initiate both push and pull.
// They are distinguished in that a
// pull must set RemoteTakes to true
// and fill in the Precis and Chunks
// in the RequestToSyncPath request;
// using GetHashesOneByOne.
//
// In advance, the user must have registered the
// SyncService.Start method on both
// the rpc25519.Client and Server.
//
// To study the implementation, see
// the SyncService.Start() method
// where RequestToSyncPath injected
// into the state machine either
// through a Start() function call parameter
// for local peers, or over the wire
// for remote peers. Over the wire,
// it arrives as the first fragment
// in the incomming circut, having
// been sent from the local peer with
// the StartRemotePeerAndGetCircuit
// call in Start().
type RequestToSyncPath struct {
	GiverPath string `zid:"0"`
	TakerPath string `zid:"1"`

	ModTime  time.Time `zid:"2"`
	FileSize int64     `zid:"3"`
	FileMode uint32    `zid:"4"`

	Done *idem.IdemCloseChan `msg:"-"`

	ToRemotePeerServiceName string `zid:"5"`
	ToRemoteNetAddr         string `zid:"6"`
	ToRemoteURL             string `zid:"7"`
	ToRemotePeerID          string `zid:"8"`

	SyncFromHostname string `zid:"9"`
	SyncFromHostCID  string `zid:"10"`

	FullFileInitSideBlake3    string `zid:"11"`
	FullFileRespondSideBlake3 string `zid:"12"`

	SizeModTimeMatch bool   `zid:"13"`
	AbsDir           string `zid:"14"`

	// Errs can be checked after Done.Chan is closed
	// to see if the request encountered any problems.
	Errs string `zid:"15"`

	BytesSent              int64 `zid:"16"`
	BytesRead              int64 `zid:"17"`
	RemoteBytesTransferred int64 `zid:"18"`

	// MoreChunksComming is for internal use.
	// It has to be exported to be conveyed over the wire.
	//
	// The initial Chunks didn't all fit into one
	// Message in the inital syncReq. So, the recipient should
	// expect 	OpRsync_RequestRemoteToGiveChunksMore,
	// and      OpRsync_RequestRemoteToGiveChunksEnd
	// to be incomming with the rest, after getting this syncReq.
	MoreChunksComming bool `zid:"19"`

	// If RemoteTakes is false => remote giver, local taker.
	RemoteTakes bool        `zid:"20"`
	Precis      *FilePrecis `zid:"21"`

	Chunks *Chunks `zid:"22"`

	GiverIsDir       bool `zid:"23"`
	TakerIsDir       bool `zid:"24"`
	GiverExistsLocal bool `zid:"25"`

	// TakerExistsLocal: if taker wants to pull the file but does
	// not have it currently, set this to false (the default).
	// This will avoid any OpRsync_LazyTakerWantsToPull
	// extra round trip.
	TakerExistsLocal bool `zid:"26"`
	TakerStartsEmpty bool `zid:"27"`
	GiverStartsEmpty bool `zid:"28"`
}

// RequestToSyncDir is a separate
// struct/message from RequestToSyncPath
// to keep things organized as we add
// in directory support. A directory
// sync will recursively search for
// giver files under GiverDir and send them.
// It should also search for taker
// files under TakerDir, and sync them
// as well, so they are deleted if
// no longer on the giver.
type RequestToSyncDir struct {
	GiverDir string `zid:"0"`

	TopTakerDirTemp  string `zid:"1"`
	TopTakerDirFinal string `zid:"2"`

	// If RemoteTakes is false => remote giver, local taker.
	// If RemoteTakes is true  => remote taker, local giver.
	RemoteTakes bool `zid:"3"`

	SR *RequestToSyncPath `zid:"4"` // original local request
}

const assembleInMem = true

func RunRsyncService(
	cfg *rpc.Config,
	u rpc.UniversalCliSrv,
	serviceName string,
	isCli bool,
	reqs chan *RequestToSyncPath,

) (lpb *rpc.LocalPeer, ctx context.Context, canc context.CancelFunc, err error) {

	// get our nice print outs of fragments.
	once.Do(AliasRsyncOps)

	//vv("RunRsyncService for serviceName '%v'; isCli = %v", serviceName, isCli)

	rsyncd := NewSyncService(reqs)
	rsyncd.U = u
	err = u.RegisterPeerServiceFunc(serviceName, rsyncd.Start)
	panicOn(err)
	rsyncd.ServiceName = serviceName

	ctx, canc = context.WithCancel(context.Background())

	lpb, err = u.StartLocalPeer(ctx, serviceName, nil)
	panicOn(err)

	//vv("RunRsyncService back from StartLocalPeer for serviceName '%v'; isCli = %v", serviceName, isCli)

	return
}

func NewSyncService(reqs chan *RequestToSyncPath) *SyncService {
	return &SyncService{
		Halt:              idem.NewHalter(),
		SyncPathRequestCh: reqs,
	}
}

const rsyncRemoteTakesString = "rsync remote takes"
const rsyncRemoteGivesString = "rsync remote gives"
const rsyncRemoteTakesDirString = "rsync remote takes dir"
const rsyncRemoteGivesDirString = "rsync remote gives dir"

func (s *SyncService) mkTempDir(finalDir string) (tempDir string, err error) {

	tempDir, err = os.MkdirTemp(".", "tempdir-for-final-topdir-"+finalDir)

	return
}

func (s *SyncService) Start(
	myPeer *rpc.LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *rpc.Circuit,

) error {

	name := myPeer.PeerServiceName
	_ = name // used when vv logging is on.

	defer func() {
		////vv("%v: end of start() inside defer, about the return/finish", name)
		s.Halt.ReqStop.Close()
		s.Halt.Done.Close()
		myPeer.Close()

		// suppress halted/context cancelled shutdowns, as
		// this is normal during shutdown/end of Circuit.
		if r := recover(); r != nil {
			if r != rpc.ErrContextCancelled && r != rpc.ErrHaltRequested {
				panic(r)
			} else {
				//vv("SyncService.Start() suppressing ErrContextCancelled or ErrHaltRequested, this is normal shutdown.")
			}
		}
	}()

	//vv("%v: start() top.", name)
	//vv("%v: ourID = '%v'; peerServiceName='%v';", name, myPeer.PeerID, myPeer.ServiceName())

	rpc.AliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		//vv("%v: top of select", name)
		select {
		case <-done0:
			//////vv("%v: done0! cause: '%v'", name, context.Cause(ctx0))
			return rpc.ErrContextCancelled
			//case <-s.halt.ReqStop.Chan:
			//	//zz("%v: halt.ReqStop seen", name)
			//	return ErrHaltRequested

			// new Circuit connection arrives => we are the passive side for it.
		case rckt := <-newCircuitCh:
			// rckt is a remote circuit connection.
			//vv("%v: newCircuitCh got rckt! service sees new peerURL: '%v'", name, rckt.RemoteCircuitURL())
			// we are the "remote"

			switch rckt.Name {

			case rsyncRemoteTakesString:
				// we the remote take a file
				go s.Taker(ctx0, rckt, myPeer, nil)
			case rsyncRemoteGivesString:
				// we the remote give a file
				go s.Giver(ctx0, rckt, myPeer, nil)
			case rsyncRemoteTakesDirString:
				// we the remote take a directory
				go s.DirTaker(ctx0, rckt, myPeer, nil)
			case rsyncRemoteGivesDirString:
				// we the remote give a directory
				go s.DirGiver(ctx0, rckt, myPeer, nil)
			default:
				panic(fmt.Sprintf("sync service does not recognize circuit name '%v'", rckt.Name))
			}

		case syncReq := <-s.SyncPathRequestCh:
			// we are the "local" getting this request from the cli client.go.

			vv("%v: sees requested on SyncPathRequestCh: '%#v'", name, syncReq)

			// begin dir sync bootstrap
			if !syncReq.RemoteTakes && syncReq.TakerIsDir {
				vv("%v: we are the local taker of dir. sending 21 OpRsync_TakerRequestsDirSyncBegin", name)

				// we generate a temp dir first, then send 21
				// OpRsync_TakerRequestsDirSyncBegin = 21 // to giver, please send me 22,26/27/28
				targetTakerTopTempDir, err := s.mkTempDir(syncReq.TakerPath)
				panicOn(err)

				reqDir := &RequestToSyncDir{
					GiverDir:         syncReq.GiverPath,
					TopTakerDirTemp:  targetTakerTopTempDir,
					TopTakerDirFinal: syncReq.TakerPath,
					RemoteTakes:      syncReq.RemoteTakes,
					SR:               syncReq,
				}
				bts, err := reqDir.MarshalMsg(nil)
				panicOn(err)
				pulldir := rpc.NewFragment()
				pulldir.FragOp = OpRsync_TakerRequestsDirSyncBegin // 21
				pulldir.Payload = bts
				pulldir.FromPeerID = myPeer.PeerID
				pulldir.ServiceName = syncReq.ToRemotePeerServiceName
				pulldir.SetUserArg("targetTakerTopTempDir", targetTakerTopTempDir)

				cktName := rsyncRemoteGivesDirString //"rsync remote gives dir"

				ckt, err := s.U.StartRemotePeerAndGetCircuit(myPeer, cktName,
					pulldir, syncReq.ToRemotePeerServiceName,
					syncReq.ToRemoteNetAddr, 0)
				panicOn(err)

				// local takes from remote. the remote gives us the update.
				go s.DirTaker(ctx0, ckt, myPeer, reqDir)

				continue
			}
			// is this a local giver dir -> remote taker dir syn request?
			if syncReq.RemoteTakes && syncReq.GiverIsDir {

				vv("%v: we are the local giver of dir. sending 22 OpRsync_DirSyncBeginToTaker", name)

				// fan out the request, one for each
				// actual file on the local giver.
				//
				// The only tricky/unique thing for directories, is that
				// we need to delete files on the taker that are not
				// on the giver. The giver won't know about these,
				// since they do not have them.
				// Other than that, we should be able to just push
				// each file on the giver to the taker.
				//
				// If the local is pulling, send the dir listing over,
				// so the remote giver can tell what to delete.
				//
				// If the local pushing, send the dir listing over,
				// so the remote taker can tell what to delete.
				//
				// So the switch over can be atomic, and not interfere
				// with the parts trying to scan/update paths, we
				// should create a new top-level versioned directory,
				// and do all writes there. All reads of existing files
				// come from the original still on disk. If no errors
				// at the end, we can rename the new to old dir (possibly
				// rename the old to old.backup to start and manually verify).
				reqDir := &RequestToSyncDir{
					GiverDir: syncReq.GiverPath,

					// remote taker to provide their temp dir path
					// in 23 DirSyncBeginReplyFromTaker message,
					// so we leave blank.
					//TopTakerDirTemp:
					TopTakerDirFinal: syncReq.TakerPath,
					RemoteTakes:      syncReq.RemoteTakes,
					SR:               syncReq,
				}
				bts, err := reqDir.MarshalMsg(nil)
				panicOn(err)
				pushdir := rpc.NewFragment()
				pushdir.FragOp = OpRsync_DirSyncBeginToTaker
				pushdir.Payload = bts
				cktName := rsyncRemoteTakesDirString // "rsync remote takes dir"

				ckt, err := s.U.StartRemotePeerAndGetCircuit(myPeer, cktName,
					pushdir, syncReq.ToRemotePeerServiceName,
					syncReq.ToRemoteNetAddr, 0)
				panicOn(err)

				// local pushes to remote.
				// Since we are "local", we start the Giver.
				// The remote takes from us.
				go s.DirGiver(ctx0, ckt, myPeer, reqDir)
				continue
			} // end if GiverIsDir

			// =========== end directory sync bootstrap.

			// =========== begin single file handling:

			// Use the circuitName to indicate remote taker or giver,
			// so they don't need to accept a frag to know which to start.
			// It would be awkward to re-submit that frag into the FSM.
			var cktName string

			frag := rpc.NewFragment()
			frag.FromPeerID = myPeer.PeerID
			frag.ServiceName = syncReq.ToRemotePeerServiceName

			if syncReq.RemoteTakes {
				// (first implemented)
				frag.FragOp = OpRsync_RequestRemoteToTake
				cktName = rsyncRemoteTakesString // "rsync remote takes"
			} else {
				// (second implemented)
				frag.FragOp = OpRsync_RequestRemoteToGive
				cktName = rsyncRemoteGivesString //"rsync remote gives"
				if syncReq.TakerExistsLocal {
					if syncReq.Chunks == nil {
						// client wants to pull without having to
						// scan their (possibly huge) local file for checksums;
						// Can we lazily/efficiently just use the file size + modTime?
						frag.FragOp = OpRsync_LazyTakerWantsToPull
					}
				}
				// maybe? ensure FileSize and mod time are set, else we
				// might be fooled into thinking this is a delete request?
				//if syncReq.FileSize == 0 {

				//}

			}

			// 111.1 bytes is the average chunk size without any Data.
			// Hence ~ 10_000 *should* be the most chunks we
			// pack into the initial sync request to fit under
			// UserMaxMessage, but we saw even 9_000 go over the
			// limit, so we keep it to K = 5_000 now.
			//
			// The is only a problem in pulls. In pulls we
			// send our local chunk hashes first. They can get big.
			//
			// Hence we now handle the general case and
			// support needing to send the Chunks separately.

			var origChunks []*Chunk
			const K = 5000 // how many we keep in first message, 9K too large.
			extraComing := false
			if syncReq.Chunks != nil && len(syncReq.Chunks.Chunks) > K ||
				syncReq.Msgsize() > rpc.UserMaxPayload-10_000 {

				// must send chunks separately
				extraComing = true
				syncReq.MoreChunksComming = true
				vv("set syncReq.MoreChunksComming = true")
				origChunks = syncReq.Chunks.Chunks

				// truncate down the initial Message,
				syncReq.Chunks.Chunks = syncReq.Chunks.Chunks[:K]

				upperBound := syncReq.Msgsize()
				if upperBound > rpc.UserMaxPayload-10_000 {
					panic(fmt.Sprintf("upperBound = %v > %v = "+
						"rpc.UserMaxPayload-10_000 even after splitting at K=%v",
						upperBound, rpc.UserMaxPayload-10_000, K))
				}
			}
			data, err := syncReq.MarshalMsg(nil)
			panicOn(err)

			// restore so locals get it!
			if extraComing {
				syncReq.Chunks.Chunks = origChunks
			}

			// should not be too big now, but verify.
			if len(data) > rpc.UserMaxPayload-10_000 {

				panic(fmt.Sprintf("problem! Start()"+
					" sees synReq from host that is too big!  "+
					"%v = len(data) > rpc.UserMaxPayload-10_000 = %v",
					len(data), rpc.UserMaxPayload-10_000))
			}
			frag.Payload = data

			ckt, err := s.U.StartRemotePeerAndGetCircuit(myPeer, cktName,
				frag, syncReq.ToRemotePeerServiceName, syncReq.ToRemoteNetAddr, 0)

			panicOn(err)
			if syncReq.RemoteTakes {
				// local pushes to remote. (first implemented)
				// Since we are "local", we start the Giver. The remote takes from us.
				go s.Giver(ctx0, ckt, myPeer, syncReq)
			} else {
				// local pulls from remote. the remote gives us the update.
				go s.Taker(ctx0, ckt, myPeer, syncReq)
			}
			if extraComing {

				xtra := &Chunks{
					Path:   syncReq.Chunks.Path,
					Chunks: origChunks[K:],
				}
				bt := &byteTracker{}
				err = s.packAndSendChunksLimitedSize(
					xtra,
					frag.FragSubject,
					OpRsync_RequestRemoteToGive_ChunksLast,
					OpRsync_RequestRemoteToGive_ChunksMore,
					ckt,
					bt,
					syncReq.Chunks.Path,
				)
				if err != nil {
					alwaysPrintf("error back from packAndSendChunksLimitedSize()"+
						" during xtra sending: '%v'", err)
					return err
				}
				/* example too large message, in case we need fields
				hdr.go:253 2025-01-22 16:58:18.399 -0600 CST ErrTooLarge! len(m.JobSerz)= 1963155 > 1309616 = maxMessage-1024;
				 m=
				&rpc25519.HDR{
				    "Created": "2025-01-22 22:58:18.399447057 +0000 UTC m=+1.598354190",
				    "From": "tcp://192.168.254.152:55842 (client: cli)",
				    "To": "tcp://192.168.254.151:8443",
				    "ServiceName": "rsync_server",
				    "Args": map[string]string{"#circuitName":"rsync remote gives", "#fromServiceName":"rsync_client", "#pleaseAssignNewPeerID":"0z6MyccTccimVfBqJoPvjJsdNAk="},
				    "Subject": "",
				    "Seqno": 2,
				    "Typ": CallPeerStartCircuitTakeToID,
				    "CallID": "PFjIpzLr_kAf5DxpTx_VjxWCEPM= (rsync remote gives)",
				    "Serial": 1,
				    "LocalRecvTm": "0001-01-01 00:00:00 +0000 UTC",
				    "Deadline": "0001-01-01 00:00:00 +0000 UTC",
				    "FromPeerID": "S75TLBC2esJ3J7TUfvM7F92YHUA= (rsync_client)",
				    "ToPeerID": "0z6MyccTccimVfBqJoPvjJsdNAk= (rsync_server)",
				    "StreamPart": 0,
				    "FragOp": 12 (OpRsyncRequestRemoteToGive),
				}
				*/
			}
		}
	}
	return nil
}

func (s *SyncService) ackBackFINToTaker(ckt *rpc.Circuit, frag *rpc.Fragment) {
	ack := rpc.NewFragment()
	ack.FragSubject = frag.FragSubject
	ack.FragOp = OpRsync_AckBackFIN_ToTaker
	err := ckt.SendOneWay(ack, 0)
	//panicOn(err) races with shutdown, skip.
	_ = err // rpc25519 error: context cancelled. Normal shutdown, don't panic.
}

func (s *SyncService) ackBackFINToGiver(ckt *rpc.Circuit, frag *rpc.Fragment) {
	ack := rpc.NewFragment()
	ack.FragSubject = frag.FragSubject
	ack.FragOp = OpRsync_FileSizeModTimeMatch
	ack.FragOp = OpRsync_AckBackFIN_ToGiver
	err := ckt.SendOneWay(ack, 0)
	//panicOn(err) races with shutdown, skip.
	_ = err // rpc25519 error: context cancelled. Normal shutdown. Don't panic.
}
