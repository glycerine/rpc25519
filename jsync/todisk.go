package jsync

import (
	"bufio"
	//"bytes"
	"fmt"
	//"io"
	"os"
	filepath "path/filepath"
	//"strings"
	"time"

	// check-summing utilities.
	rpc "github.com/glycerine/rpc25519"

	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
	myblake3 "github.com/glycerine/rpc25519/hash"
)

//msgp:ignore PerCallID_FileToDiskState
type FileToDiskState struct {
	T0 time.Time

	WriteToPath    string
	WriteToPathTmp string
	Randomness     string

	Fd            *os.File
	FdBufioWriter *bufio.Writer

	BytesWrit int64

	Blake3hash *myblake3.Blake3

	PartsSeen map[int64]bool
	SeenCount int
}

func NewFileToDiskState(writeToPath string) (f *FileToDiskState) {
	f = &FileToDiskState{
		WriteToPath: writeToPath,
		PartsSeen:   make(map[int64]bool),
		Blake3hash:  myblake3.NewBlake3(),
		T0:          time.Now(),
	}
	// do this check just once, not every time!
	dirs := filepath.Dir(f.WriteToPath)
	if !dirExists(dirs) {
		err := os.MkdirAll(dirs, 0700)
		panicOn(err)
	}
	return f
}

func (s *FileToDiskState) WriteOneMsgToFile(req *rpc.Message, last bool) (err error) {

	s.SeenCount++
	hdr1 := &req.HDR
	if hdr1.StreamPart == 0 {
		if s.SeenCount != 1 {
			panic("we saw a part before 0!")
		}
		//vv("ServerSideUploadState.ReceiveFileInParts sees part 0: hdr1='%v'", hdr1.String())
		s.PartsSeen = make(map[int64]bool)
		//s.Blake3hash.Reset()

		s.Randomness = cryRandBytesBase64(18)
		s.WriteToPathTmp = s.WriteToPath + ".tmp_" + s.Randomness

		/*
			sum := blake3OfBytesString(req.JobSerz)
			blake3checksumBase64, ok := hdr1.Args["blake3"]
			if ok {
				if blake3checksumBase64 != sum {
					panic(fmt.Sprintf("checksum on first %v bytes disagree: client sent blake3sum='%v'; we computed = '%v'", len(req.JobSerz), blake3checksumBase64, sum))
				}
			}
		*/

		// save the file handle for the next callback too.
		s.Fd, err = os.Create(s.WriteToPathTmp)
		if err != nil {
			return fmt.Errorf("error: server could not create path '%v': '%v'", s.WriteToPathTmp, err)
		}
		// arg no! slows things waaaay down!
		//s.Fd.Sync() // get the file showing on disk asap

		// add buffering to writes.
		s.FdBufioWriter = bufio.NewWriterSize(s.Fd, rpc.UserMaxPayload+10_000)
	}

	part := req.HDR.StreamPart
	seen := s.PartsSeen[part]
	if seen {
		panic(fmt.Sprintf("part %v was already seen.", part))
	}
	s.PartsSeen[part] = true

	// can be slow. we see its working, time to optimize/speed up.
	//s.Blake3hash.Write(req.JobSerz)
	//serverSum := blake3OfBytesString(req.JobSerz)
	//clientSum := req.HDR.Args["blake3"]

	//vv("server part %v, len %v, server-sum='%v' \n        while Subject blake3    client-sum='%v'\n", req.HDR.StreamPart, len(req.JobSerz), serverSum, clientSum)

	//if part > 0 && serverSum != clientSum {
	//	panic(fmt.Sprintf("checksum disagree on part %v; see above."+
	//		" server sees len %v req.JobSerz='%v'; serverSum='%v'; clientSum='%v'",
	//		part, len(req.JobSerz),
	//		string(req.JobSerz), serverSum, clientSum))
	//}
	n := len(req.JobSerz)
	if n > 0 {
		nw, err := s.FdBufioWriter.Write(req.JobSerz)
		// less allocation. don't need a bytes.NewBuffer.
		//nw, err := io.Copy(s.FdBufioWriter, bytes.NewBuffer(req.JobSerz))
		s.BytesWrit += int64(nw)
		if err != nil {
			err = fmt.Errorf("ReceiveFileInParts: on "+
				"writing StreamPart 1 to path '%v', we got error: "+
				"'%v', after writing %v of %v", s.WriteToPathTmp, err, nw, n)
			//vv("problem: %v", err.Error())
			return err
		}
	}
	if last {
		//vv("WriteOneMsgToFile sees last, renaming '%v' -> '%v'", s.WriteToPathTmp, s.WriteToPath)

		// remember to Flush the buffering before closing s.Fd.
		s.FdBufioWriter.Flush()
		s.Fd.Close()
		err = os.Rename(s.WriteToPathTmp, s.WriteToPath)
		panicOn(err)
	}
	return
}

func blake3OfBytes(by []byte) []byte {
	h := blake3.New(64, nil)
	h.Write(by)
	return h.Sum(nil)
}

func blake3OfBytesString(by []byte) string {
	sum := blake3OfBytes(by)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
}
