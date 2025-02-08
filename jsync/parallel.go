package jsync

import (
	//"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	//"sort"
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

	isLast   bool
	isPenult bool

	// Chunk.Cry is key
	idx map[string]*chunkPos

	// how many did we trim off the beginning,
	// so we index the end correctly.
	trimmed int
}

type chunkPos struct {
	chunk *Chunk
	pos   int
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

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/target settings in
	// order to give good chunking.

	//	cdcCfg := &jcdc.CDC_Config{
	//		MinSize:    2 * 1024,
	//		TargetSize: 8 * 1024,
	//		MaxSize:    64 * 1024,
	//	}

	//chunker := jcdc.ResticRabin_Algo
	//chunker := jcdc.FastCDC_StadiaAlgo
	//chunker := jcdc.RabinKarp_Algo
	//chunker := jcdc.UltraCDC_Algo
	//chunker := jcdc.FNV_Algo

	// UltraCDC_Algo      CDCAlgo = 0
	// FastCDC_StadiaAlgo CDCAlgo = 1
	// FastCDC_PlakarAlgo CDCAlgo = 2
	// FNV_Algo           CDCAlgo = 3
	// RabinKarp_Algo     CDCAlgo = 4
	// ResticRabin_Algo   CDCAlgo = 8

	//cdc := jcdc.GetCutpointer(chunker, cdcCfg)

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
	segment := int(1 << 20) // 1<<19 => 512KB

	// try to re-compute prev cut without knowing it.
	// Good: we see the pre-reading allows us to
	// align separately computed (in parallel) segment
	// chunks, at the cost of re-doing the chunking
	// on a smaller amount (2 * max size) overlapping
	// portion.
	preRead := 2 * int(Default_CDC_Config.MaxSize)

	if parallelBits != 0 {
		segment = 1 << parallelBits
	}
	minSegSize := 3 * int(Default_CDC_Config.MaxSize) // min 64KB (1 << 16)
	if segment < minSegSize {
		segment = minSegSize
	}

	segN := sz / segment
	if sz*segment < segN {
		segN++ // round up. any fraction left at the end still gets processed.
	}
	if segN == 0 {
		segN = 1
	}

	// how big a goroutine pool to use
	// to process the jobs.
	nCPU := runtime.NumCPU()
	nWorkers := nCPU
	if ngoro > 0 {
		nWorkers = ngoro
	}

	if segN < nWorkers {
		nWorkers = segN // get smaller, but not larger.
	}

	buf := make([][]byte, nWorkers)
	for i := 0; i < nWorkers; i++ {
		buf[i] = make([]byte, segment+preRead)
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

	// output
	wchunks := make([][]*Chunk, nNodes)
	jobs := make([]*job, nNodes) // store indexes too.
	//overlaps := make([][]*Chunk, nNodes-2)

	nW := int(nWorkers)
	vv("nW = %v", nW)
	for worker := 0; worker < nW; worker++ {

		go func(worker int) {
			//func(worker int) {
			defer func() {
				wg.Done()
			}()

			var job *job
			var chunks []*Chunk
			addChunk := func(slc []byte, beg int) {
				hsh := hash.Blake3OfBytesString(slc)
				//fmt.Printf("[%03d]GetHashes hsh = %v\n", k, hsh)
				chunk := &Chunk{
					Beg:  beg,
					Endx: beg + len(slc),
					Cry:  hsh,
				}
				job.idx[hsh] = &chunkPos{
					chunk: chunk,
					pos:   len(chunks),
				}
				chunks = append(chunks, chunk)
			}

			f, err := os.OpenFile(path, os.O_RDONLY, 0)
			panicOn(err)
			defer f.Close()

			var ok bool
			for {
				select {
				case job, ok = <-work:
					if !ok {
						return
					}
				}
				// compute a quick lookup index for the segment too
				job.idx = make(map[string]*chunkPos)

				pre := preRead
				if job.beg >= preRead {
					f.Seek(int64(job.beg-pre), 0)
				} else {
					pre = 0
					f.Seek(int64(job.beg), 0)
				}
				lenseg := pre + (job.endx - job.beg)
				if lenseg == 0 {
					panic("lenseg should not be 0")
				}

				nr, err := io.ReadFull(f, buf[worker][:lenseg])
				_ = nr
				// either io.EOF (0 bytes) or
				// io.ErrUnexpectedEOF (nr<lenseg) are problems.
				panicOn(err)

				// offset where data starts in the original file;
				// to pass to addChunk
				dataoff := job.beg - pre
				//vv("worker %v  has job.beg = %v, pre = %v, starting dataoff = job.beg - pre = %v", worker, job.beg, pre, job.beg-pre)
				data := buf[worker][:lenseg]

				chunks = wchunks[job.nodeK]
				//now we take any sized cut
				for j := 0; len(data) > 0; j++ {
					cut := cdc.NextCut(data)
					addChunk(data[:cut], dataoff)
					data = data[cut:]
					dataoff += cut
				}
				wchunks[job.nodeK] = chunks
				jobs[job.nodeK] = job
			}

		}(int(worker))
	}

	// send off all the jobs
	last := len(wchunks) - 1
	penult := len(wchunks) - 2
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
			beg:      beg,
			endx:     endx,
			nodeK:    i,
			isLast:   i == last,
			isPenult: i == penult,
		}
		work <- job
	}
	// we have sent off njob = nNodes to be hashed
	close(work)
	wg.Wait()

	// assemble all the []*Chunk in order.
	// INVAR: nNodes == len(wchunks).

	/*
		for i, c := range wchunks {
			showEachSegment(i, c)
			chunks0.Chunks = append(chunks0.Chunks, c...)
		}
	*/

	if len(wchunks) == 1 {
		chunks0.Chunks = append(chunks0.Chunks, wchunks[0]...)
	} else {

		lasti := len(wchunks) - 1
	alldone:
		for i, cs := range wchunks {
			if i == 0 {
				continue
			}
			// INVAR: i > 0
			prevjob := jobs[i-1]
			curjob := jobs[i]

			// find the first overlap in curjob with prevjob
			foundOverlap := false
			for j, c := range cs {
				w, ok := prevjob.idx[c.Cry]
				if ok {
					foundOverlap = true
					// join here w.pos : j
					// we have to lazily only add the prev set now
					// slice bounds out of range [:22] with capacity 20
					chunks0.Chunks = append(chunks0.Chunks, wchunks[i-1][:(w.pos-prevjob.trimmed)]...)
					// and truncate the (cur) sets beginning, and
					// wait to add it til next time, when we can
					// again remove the overlap at its tail.
					// (unless we are on the lasti, see below).
					wchunks[i] = wchunks[i][j:]
					curjob.trimmed = j
					vv("had to look through j = %v to find the overlap", j)

					if i == lasti {
						chunks0.Chunks = append(chunks0.Chunks, wchunks[i]...)
						break alldone
					}
					break
				}
			}
			if !foundOverlap {
				//   Line 574: - overlap not found. this should be impossible b/c we go back 2 * max chunk size into the previous segment. i = 15; lasti = 6813
				showEachSegment(i-1, wchunks[i-1])
				showEachSegment(i, wchunks[i])
				panic(fmt.Sprintf("overlap not found. this should be impossible b/c we go back 2 * max chunk size into the previous segment. i = %v; lasti = %v", i, lasti))
			}
			//chunks0.Chunks = append(chunks0.Chunks, c...)
		}
	}
	return
}

func showEachSegment(i int, cs []*Chunk) {
	fmt.Printf("segment i = %v\n", i)
	for j, c := range cs {
		fmt.Printf("  %03d  [ %v : %v ) (len %v) %v\n",
			j, c.Beg, c.Endx, (c.Endx - c.Beg), c.Cry)
	}
}
