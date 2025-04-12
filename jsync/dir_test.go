package jsync

import (
	//"bytes"
	//cryrand "crypto/rand"
	"fmt"
	//"path/filepath"
	//"io"
	mathrand2 "math/rand/v2"
	"os"
	"testing"
	//"time"

	cv "github.com/glycerine/goconvey/convey"
	rpc "github.com/glycerine/rpc25519"
)

func Test440_directory_sync(t *testing.T) {

	cv.Convey("using our rsync-like-protocol, a client doing a push of a directory should suceed", t, func() {

		// set up two isolated test dirs, so we can
		// be sure exactly what is changing by inspection.

		// deterministic pseudo-random numbers as data.
		var seed [32]byte
		seed[1] = 2
		generator := mathrand2.NewChaCha8(seed)

		cwd, err := os.Getwd()
		panicOn(err)
		_ = cwd

		localDir := cwd + sep + "local_cli_dir_test440/"
		remoteDir := cwd + sep + "remote_srv_dir_test440/"
		// remove any old leftover test files first.
		os.RemoveAll(localDir)
		os.RemoveAll(remoteDir)
		os.RemoveAll(remoteDir + ".oldvers")
		panicOn(os.MkdirAll(localDir, 0755))
		panicOn(os.MkdirAll(remoteDir, 0755))

		// create test files for local push
		for nfiles := range 2 {
			localBase := fmt.Sprintf("test440_%03dkb.dat", nfiles)
			localPath := localDir + localBase
			vv("localPath = '%v'", localPath)

			// modify "local" target path so we don't overwrite our
			// source file when testing in one directory. Also to
			// check that we can.
			//remoteBase := localBase + ".remote_rsync_out"
			//remotePath := remoteDir + sep + remoteBase
			//vv("remotePath = '%v'", remotePath)

			testfd, err := os.Create(localPath)
			panicOn(err)
			slc := make([]byte, 1<<10) // 1 KB slice

			// random or zeros?
			allZeros := false
			if allZeros {
				// slc is already ready with all 0.
			} else {
				generator.Read(slc)
			}
			_, err = testfd.Write(slc)
			panicOn(err)
			testfd.Close()
			vv("created 1 KB test file in localPath='%v'.", localPath)
		}
		// set up a server and a client.

		vv("two dirs and two files local setup done")

		cfg := rpc.NewConfig()
		//cfg.TCPonly_no_TLS = true
		//cfg.CompressionOff = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := rpc.NewServer("srv_rsync_test440", cfg)

		// ==== configure server's remote dir first, start server.
		//os.Setenv("RPC25519_SERVER_DATA_DIR", remoteDir)
		// try not to impact other tests:
		//defer os.Unsetenv("RPC25519_SERVER_DATA_DIR")

		// for this to work: the srv must note its home dir when
		// starting, just once, and keep referencing that, and
		// not the cwd, which we will change below for the cli
		// to also note its start dir.
		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		srvDataDir := srv.DataDir()

		vv("server Start() returned serverAddr = '%v'; wd = '%v'; srv.DataDir() = '%v'",
			serverAddr, srv.ServerStartingDir(), srvDataDir)

		// JsyncServer and JsyncClient are re-prefixed to
		// tell them apart from "actual" rsync. Jsync is ours.
		jSyncSrv, err := NewJsyncServer(srv)
		panicOn(err)
		defer jSyncSrv.Close()

		// Note: avoid changing directories. It messes up other tests.

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := rpc.NewClient("cli_rsync_test440", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		vv("client Start() coming from cli.LocalAddr = '%v'; wd = '%v'",
			cli.LocalAddr(), cli.ClientStartingDir())

		defer cli.Close()

		// ============ actual test now:

		jSyncCli, err := NewJsyncClient(cli)
		panicOn(err)
		defer jSyncCli.Close()

		// 	dataBytesMoved0
		_, err = jSyncCli.PushFromTo(localDir, remoteDir)
		vv("PushFromTo gave err = '%v'", err)
		panicOn(err)

		// confirm it happened. localDir = expected, remoteDir = observed
		diff, err := compareDirs(localDir, remoteDir)
		panicOn(err)
		cv.So(diff, cv.ShouldEqual, "")

		// check mod time being updated
		lsz, lmod, err := FileSizeModTime(localDir)
		panicOn(err)
		rsz, rmod, err := FileSizeModTime(remoteDir)
		panicOn(err)
		if lsz != rsz {
			panic("lsz != rsz")
		}
		if !rmod.Equal(lmod) {
			t.Fatalf("error: lmod='%v' but lmod='%v'", lmod, rmod) // red.
		}

	})
}

// test: local taker has a dir, but giver side has converted that
//       path to a file, no longer a dir. local taker should
//       either error out, or more likely: delete the dir and replace with the
//       giver's file.
