package jsync

import (
	//"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/jcdc"
)

// job delegates file chunkinging duties
// out to multiple parallel goroutines
// that hash different segments of a
// single file. See ChunkFile and ChunkFile2
// below.
type job struct {
	beg  int
	endx int

	nodeK int

	isLast bool
}

// ChunkFile uses multiple parallel goroutines to read and
// chunk.
// See ChunkFile2 to control the details.
func ChunkFile(path string) (precis *FilePrecis, chunks *Chunks, err error) {
	precis, chunks, err = ChunkFile2(rpc.Hostname, path, 0, 0)
	return
}

// ChunkFile2 processes a file in parallel using
// segments of size (1 << parallelBits) bytes.
//
// parallelBits == 0 means use the default (19).
//
// parallelBits < 14 will be ignored and we'll use 14,
// as that gives the minimum segment size of 16KB.
//
// We use runtime.NumCPU goroutines to read and hash
// if ngoro <= 0; else we use ngoro.
//
// The simple call is ChunkFile2(path, nil, 0, 0) for
// the defaults. See ChunkFile for an easy invocation.
func ChunkFile2(
	host, path string,
	parallelBits int,
	ngoro int,

) (precis *FilePrecis, chunks0 *Chunks, err0 error) {

	fd, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		err0 = err
		return
	}
	defer fd.Close()

	fi, err := fd.Stat()
	if err != nil {
		err0 = err
		return
	}
	sz := int(fi.Size())
	if sz == 0 {
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false)
	}

	cdc := jcdc.GetCutpointer(Default_CDC, Default_CDC_Config)

	// side effect: warm up the filesystem cache of path.
	fcry, err := hash.Blake3OfFile(path)
	panicOn(err)

	precis = &FilePrecis{
		Host:        host,
		Path:        path,
		FileSize:    sz,
		ModTime:     fi.ModTime(),
		FileCry:     fcry,
		ChunkerName: cdc.Name(),
		CDC_Config:  cdc.Config(),
		HashName:    "blake3.33B",
	}
	chunks0 = NewChunks(path)
	chunks0.FileSize = precis.FileSize
	chunks0.FileCry = precis.FileCry

	// segment is the size in bytes that one goroutine
	// reads from disk and hashes.
	segment := int(1 << 19) // 512KB by default.
	if parallelBits != 0 {
		segment = 1 << parallelBits
	}
	minsz := int(Default_CDC_Config.MaxSize) // min 64KB (1 << 16)
	if segment < minsz {
		segment = minsz
	}

	jobN := sz / segment
	if sz*segment < jobN {
		jobN++ // round up. any fraction left at the end still gets processed.
	}
	if jobN == 0 {
		jobN = 1
	}

	// how big a goroutine pool to use
	// to process the jobs.
	nCPU := runtime.NumCPU()
	nWorkers := nCPU
	if ngoro > 0 {
		nWorkers = ngoro
	}

	if jobN < nWorkers {
		nWorkers = jobN // get smaller, but not larger.
	}

	buf := make([][]byte, nWorkers)
	for i := 0; i < nWorkers; i++ {
		buf[i] = make([]byte, segment)
	}

	// buffered channel for less waiting on scheduling.
	work := make(chan *job, 1024)
	var wg sync.WaitGroup
	wg.Add(int(nWorkers))

	// the number of sub-tree root-nodes (only
	// marked as parents though) to be merged after
	// all the parallel hashing is done.
	nNodes := (sz + segment - 1) / segment
	vv("nNodes = %v", nNodes)

	wchunks := make([][]*Chunk, nNodes)

	nW := int(nWorkers)
	vv("nW = %v", nW)
	for worker := 0; worker < nW; worker++ {

		go func(worker int) {
			defer func() {
				wg.Done()
			}()

			var chunks []*Chunk
			addChunk := func(slc []byte, beg int) {
				hsh := hash.Blake3OfBytesString(slc)
				//fmt.Printf("[%03d]GetHashes hsh = %v\n", k, hsh)
				chunk := &Chunk{
					Beg:  beg,
					Endx: beg + len(slc),
					Cry:  hsh,
				}
				chunks = append(chunks, chunk)
			}

			f, err := os.OpenFile(path, os.O_RDONLY, 0)
			panicOn(err)
			defer f.Close()

			var job *job
			var ok bool
			for {
				select {
				case job, ok = <-work:
					if !ok {
						return
					}
				}
				f.Seek(int64(job.beg), 0)
				lenseg := job.endx - job.beg
				if lenseg == 0 {
					panic("lenseg should not be 0")
				}

				nr, err := io.ReadFull(f, buf[worker][:lenseg])
				// either io.EOF (0 bytes) or
				// io.ErrUnexpectedEOF (nr<lenseg) are problems.
				panicOn(err)

				if nr != lenseg {
					panic(fmt.Sprintf("short read!?!: path = '%v'. "+
						"expected = %v; got = %v; on worker=%v",
						path, lenseg, nr, worker))
				}

				// offset where data starts in the original file;
				// to pass to addChunk
				dataoff := job.beg

				data := buf[worker][:lenseg]
				chunks = wchunks[job.nodeK]

				for j := 0; len(data) > 0; j++ {

					cut := cdc.NextCut(data)

					//now we take any sized cut

					addChunk(data[:cut], dataoff)
					//vv("j=%v  legit cut: '%v'", j, chunks.Chunks[len(chunks.Chunks)-1])
					data = data[cut:]
					dataoff += cut
				}

				wchunks[job.nodeK] = chunks
			}

		}(int(worker))
	}

	// send off all the jobs
	last := len(wchunks) - 1
	for i := range wchunks {
		beg := i * int(segment)
		endx := (i + 1) * int(segment)
		if endx > sz {
			endx = sz
		}
		if endx == beg {
			panic("logic error: must have endx > beg. don't process empty segment")
		}
		job := &job{
			beg:    beg,
			endx:   endx,
			nodeK:  i,
			isLast: i == last,
		}
		work <- job
	}
	// we have sent off njob = nNodes to be hashed
	close(work)
	wg.Wait()

	// assemble all the []*Chunk in order.
	for _, c := range wchunks {
		chunks0.Chunks = append(chunks0.Chunks, c...)
	}
	vv("len chunks0.Chunks = %v", len(chunks0.Chunks))
	return
}
