package jsync

// JsyncClient runs an rsync-like protocol. We call
// it Jsync rather than Rsync so its not mistaken
// for actual rsync. This file shows example use/
// provides a simple demo of file syncing.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	// "strings"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

var _ = fmt.Sprintf
var _ = time.Time{}
var _ = os.Open

type JsyncClient struct {
	*jsyncU
}

type JsyncServer struct {
	*jsyncU
}

type jsyncU struct {
	rsyncd      *SyncService
	serviceName string
	reqs        chan *RequestToSyncPath
	isCli       bool
	cfg         *rpc.Config
	lpb         *rpc.LocalPeer
}

func NewJsyncClient(u *rpc.Client) (j *JsyncClient, err error) {
	j = &JsyncClient{}

	j.jsyncU, err = newJsyncU(u, "jsync_client", true)
	if err != nil {
		return nil, err
	}
	return
}
func NewJsyncServer(u *rpc.Server) (j *JsyncServer, err error) {
	j = &JsyncServer{}
	j.jsyncU, err = newJsyncU(u, "jsync_server", false)
	if err != nil {
		return nil, err
	}
	return
}

func newJsyncU(u rpc.UniversalCliSrv, serviceName string, isCli bool) (j *jsyncU, err error) {
	once.Do(AliasRsyncOps)

	reqs := make(chan *RequestToSyncPath)
	rsyncd := NewSyncService(reqs)
	rsyncd.U = u
	j = &jsyncU{
		reqs:        reqs,
		rsyncd:      rsyncd,
		serviceName: serviceName,
		isCli:       isCli,
		cfg:         u.GetConfig(),
	}
	err = u.RegisterPeerServiceFunc(serviceName, rsyncd.Start)

	ctx := context.Background()
	lpb, err := u.StartLocalPeer(ctx, serviceName, nil)
	panicOn(err)
	j.lpb = lpb

	return
}

func (c *jsyncU) Close() {
	c.lpb.Close()
}

// PullToFrom is a mnemonic for the order of its arguments.
// The toLocalPath is first. The fromRemotePath is second.
// The action is to pull from the remote into the local.
func (s *jsyncU) PullToFrom(toLocalPath, fromRemotePath string) (dataBytesMoved int, err error) {

	t0 := time.Now()
	_ = t0
	cli := s.rsyncd.U

	var req *RequestToSyncPath

	if fileExists(toLocalPath) {
		fi, err := os.Stat(toLocalPath)
		panicOn(err)
		fmt.Printf("PullToFrom to pull into toLocalPath: '%v' <- fromRemotePath: '%v'\n", toLocalPath, fromRemotePath)

		//const keepData = false
		//const wantChunks = true

		var precis *FilePrecis
		var chunks *Chunks

		if parallelChunking {
			precis, chunks, err = ChunkFile(toLocalPath)
		} else {
			precis, chunks, err = GetHashesOneByOne(rpc.Hostname, toLocalPath)
		}
		panicOn(err)

		// joins with current working directory if relative.
		absPath, err := filepath.Abs(toLocalPath)
		panicOn(err)
		dir := filepath.Dir(absPath)

		req = &RequestToSyncPath{
			GiverPath:               fromRemotePath,
			TakerPath:               toLocalPath,
			TakerFileSize:           fi.Size(),
			TakerModTime:            fi.ModTime(),
			TakerFileMode:           uint32(fi.Mode()),
			Done:                    idem.NewIdemCloseChan(),
			ToRemotePeerServiceName: "jsync_server",

			//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
			ToRemoteNetAddr: cli.RemoteAddr(),

			SyncFromHostname: rpc.Hostname,
			SyncFromHostCID:  rpc.HostCID,
			GiverDirAbs:      dir,

			RemoteTakes: false,

			// (pull): for taking an update locally. Tell remote what we have now.
			Precis: precis,
			Chunks: chunks,

			UpdateProgress: make(chan *ProgressUpdate, 100),
		}
		//vv("PullToFrom using ToRemoteNetAddr: '%v'", req.ToRemoteNetAddr)
	} else {
		//vv("PullToFrom: such toLocalPath at the moment, toLocalPath='%v'", toLocalPath)

		cwd, err := os.Getwd()
		panicOn(err)
		dir, err := filepath.Abs(cwd)
		panicOn(err)

		// pull new file we don't have at the moment.
		req = &RequestToSyncPath{
			TakerPath:               toLocalPath,
			GiverPath:               fromRemotePath,
			TakerStartsEmpty:        true,
			Done:                    idem.NewIdemCloseChan(),
			ToRemotePeerServiceName: "jsync_server",
			//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
			ToRemoteNetAddr: cli.RemoteAddr(),

			SyncFromHostname: rpc.Hostname,
			SyncFromHostCID:  rpc.HostCID,
			GiverDirAbs:      dir,
			RemoteTakes:      false,

			UpdateProgress: make(chan *ProgressUpdate, 100),
		}
	}

	//vv("PullToFrom about to send on reqs chan")
	s.reqs <- req
jobDone:
	for {
		select {
		case prog := <-req.UpdateProgress:

			fmt.Printf("progress: %30v %8v %8v\n", prog.Path, prog.Latest, prog.Total)
			continue
		case <-req.Done.Chan:
			break jobDone
		}
	}

	//vv("PullToFrom pulled toLocalPath '%v' in '%v'", toLocalPath, time.Since(t0))

	if req.Errs != "" {
		alwaysPrintf("req.Err: '%v'", req.Errs)
	}

	return int(req.BytesRead), nil
}

// PushFromTo is a mnemonic for the order of its arguments.
// The fromLocalPath is first. The toRemotePath is second.
// The action is to push from the local to the remote.
func (s *jsyncU) PushFromTo(fromLocalPath, toRemotePath string) (dataBytesMoved int, err error) {

	t0 := time.Now()
	_ = t0
	//path := *rsyncPath

	cli := s.rsyncd.U

	//vv("jsyncU.PushFromTo(): fromLocalPath='%v', toRemotePath='%v'", fromLocalPath, toRemotePath)
	//vv(" dirExists(fromLocalPath) = '%v'", dirExists(fromLocalPath))
	//vv("fileExists(fromLocalPath) = '%v'", fileExists(fromLocalPath))
	//vv(" dirExists(toRemotePath)  = '%v'", dirExists(toRemotePath))
	//vv("fileExists(toRemotePath)  = '%v'", fileExists(toRemotePath))

	if dirExists(fromLocalPath) {
		// ok: we will dir sync
		return -1, s.DirPushFromTo(fromLocalPath, toRemotePath, cli)

	} else if !fileExists(fromLocalPath) {
		// no such path at the moment
		return 0, fmt.Errorf("error on PushFromTo: no such fromLocalPath '%v'",
			fromLocalPath)
	}

	// single file synce below, dir sync was above.

	fi, err := os.Stat(fromLocalPath)
	if err != nil {
		return 0, fmt.Errorf("error on PushFromTo: could not os.Stat() fromLocalPath '%v': '%v'", fromLocalPath, err)
	}
	fmt.Printf("PushFromTo '%v' about to send fromLocalPath: '%v'\n", s.serviceName, fromLocalPath)

	//absPath, err := filepath.Abs(fromLocalPath)
	//panicOn(err)
	//dir := filepath.Dir(absPath)
	cwd, err := os.Getwd()
	panicOn(err)

	req := &RequestToSyncPath{
		GiverPath:               fromLocalPath,
		TakerPath:               toRemotePath,
		GiverFileSize:           fi.Size(),
		GiverModTime:            fi.ModTime(),
		GiverFileMode:           uint32(fi.Mode()),
		Done:                    idem.NewIdemCloseChan(),
		ToRemotePeerServiceName: "jsync_server",

		//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
		ToRemoteNetAddr: cli.RemoteAddr(),

		SyncFromHostname: rpc.Hostname,
		SyncFromHostCID:  rpc.HostCID,
		GiverDirAbs:      cwd,

		RemoteTakes: true,

		UpdateProgress: make(chan *ProgressUpdate, 100),

		// (pull): for taking an update locally. Tell remote what we have now.
		//Precis: precis,
		//Chunks: chunks,
	}
	//vv("PushFromTo req using ToRemoteNetAddr: '%v'. push (remote takes) = %v", req.ToRemoteNetAddr, req.RemoteTakes)

	//lpb, ctx, canc, err := RunRsyncReader(s.cfg, cli, "jsync_client", true, reqs)
	//panicOn(err)
	//defer lpb.Close()
	//defer canc()
	//_ = ctx

	//vv("PushFromTo about to send on reqs chan")
	s.reqs <- req

jobDone:
	for {
		select {
		case prog := <-req.UpdateProgress:

			fmt.Printf("progress: %30v %8v %8v\n", prog.Path, prog.Latest, prog.Total)
			continue
		case <-req.Done.Chan:
			break jobDone
		}
	}

	//vv("PushFromTo push fromLocalPath '%v' -> '%v' in %v: err='%v'", fromLocalPath, toRemotePath, time.Since(t0), req.Errs)

	if req.Errs != "" {
		return 0, fmt.Errorf("%v", req.Errs)
	}
	return int(req.BytesSent), nil
}

func (s *jsyncU) DirPushFromTo(fromLocalDir, toRemoteDir string, cli rpc.UniversalCliSrv) (err error) {

	t0 := time.Now()
	_ = t0
	cwd, err := os.Getwd()
	panicOn(err)

	req := &RequestToSyncPath{
		GiverPath: fromLocalDir,
		TakerPath: toRemoteDir,

		GiverIsDir: true,
		TakerIsDir: true,

		//FileSize:                fi.Size(),
		//ModTime:                 fi.ModTime(),
		//FileMode:                uint32(fi.Mode()),
		Done:                    idem.NewIdemCloseChan(),
		ToRemotePeerServiceName: "jsync_server",

		//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
		ToRemoteNetAddr: cli.RemoteAddr(),

		SyncFromHostname: rpc.Hostname,
		SyncFromHostCID:  rpc.HostCID,
		GiverDirAbs:      cwd,

		RemoteTakes: true,

		UpdateProgress: make(chan *ProgressUpdate, 100),
	}
	//vv("PushFromTo req using ToRemoteNetAddr: '%v'. push (remote takes) = %v", req.ToRemoteNetAddr, req.RemoteTakes)

	//lpb, ctx, canc, err := RunRsyncReader(s.cfg, cli, "jsync_client", true, reqs)
	//panicOn(err)
	//defer lpb.Close()
	//defer canc()
	//_ = ctx

	//vv("DirPushFromTo about to send on reqs chan: '%#v'", req)
	s.reqs <- req
jobDone:
	for {
		select {
		case prog := <-req.UpdateProgress:
			fmt.Printf("progress: %30v %8v %8v\n", prog.Path, prog.Latest, prog.Total)
			continue
		case <-req.Done.Chan:
			break jobDone
		}
	}
	//<-req.Done.Chan

	//vv("DirPushFromTo push fromLocalDir '%v' -> '%v' in %v: err='%v'", fromLocalDir, toRemoteDir, time.Since(t0), req.Errs)

	if req.Errs != "" {
		return fmt.Errorf("%v", req.Errs)
	}
	return nil

}
