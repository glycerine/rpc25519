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

	newEndx int

	nodeK int

	genCuts bool // else get hashes
	isLast  bool

	cand []int

	cuts []int

	chunks []*Chunk
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

	if parallelBits != 0 {
		segment = 1 << parallelBits
	}
	minSegSize := int(Default_CDC_Config.MaxSize) // 1 MB
	if segment < minSegSize {
		segment = minSegSize
	}
	postRead := minSegSize

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
		buf[i] = make([]byte, segment*3)
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

	mincut := int(Default_CDC_Config.MinSize)
	nW := int(nWorkers)
	vv("nW = %v", nW)

	workfunc := func(work chan *job, worker int) {
		//func(worker int) {
		defer func() {
			wg.Done()
		}()

		var job *job

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

			if !job.genCuts {
				//vv("on hashing... job = '%#v'", job)
			}

			f.Seek(int64(job.beg), 0)

			lenseg := (job.newEndx - job.beg)
			if lenseg == 0 {
				panic("lenseg should not be 0")
			}
			if job.genCuts && !job.isLast {
				lenseg += postRead
				if job.endx+postRead > sz {
					lenseg = sz - job.beg
				}
			} else {
				//vv("on last job: job.beg = %v; job.endx = %v; span =%v", job.beg, job.endx, job.endx-job.beg)
			}

			nr, err := io.ReadFull(f, buf[worker][:lenseg])
			_ = nr
			// either io.EOF (0 bytes) or
			// io.ErrUnexpectedEOF (nr<lenseg) are problems.
			panicOn(err)

			data := buf[worker][:lenseg]
			if job.genCuts {

				// offset where data starts in the original file;
				dataoff := job.beg

				for j := 0; len(data) >= mincut; j++ {
					cut := cdc.NextCut(data)
					job.cand = append(job.cand, dataoff+cut)
					data = data[cut:]
					dataoff += cut
				}
				jobs[job.nodeK] = job

			} else {
				//vv("gen hashes job = '%#v'", job)
				if len(job.cuts) == 0 {
					return
				}
				dataoff := job.cuts[0]
				prev := dataoff - job.beg
				for _, cut := range job.cuts {
					d := cut - dataoff
					if d == 0 {
						continue // skip first truncated.
					}
					slc := data[prev : prev+d]
					chunk := &Chunk{
						Beg:  dataoff,
						Endx: dataoff + d,
						Cry:  hash.Blake3OfBytesString(slc),
					}
					prev += d
					//vv("chunk [%v, %v) = %v", chunk.Beg, chunk.Endx, chunk.Cry)
					job.chunks = append(job.chunks, chunk)
					dataoff += d
				}
			}
		}

	}

	for worker := 0; worker < nW; worker++ {
		go workfunc(work, int(worker))
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
			beg:     beg,
			endx:    endx,
			newEndx: endx,
			nodeK:   i,
			isLast:  i == last,
			genCuts: true,
		}
		work <- job
	}
	// we have sent off njob = nNodes to be hashed
	close(work)
	wg.Wait()

	//couldNotResync := 0

	// compute keeper cutpoints
	var gkeep []int

	prev := 0
	var prevjob *job
	for i, curjob := range jobs {
		if i == 0 {
			// base case
			curjob.cuts = []int{0}
		}
		for _, cut := range curjob.cand {

			if cut <= prev {
				continue
			}
			d := cut - prev
			if d >= mincut {
				if prevjob != nil {
					// tell prevjob where their last cut ends.
					//prevjob.cuts = append(prevjob.cuts, cut)
					//vv("set segment newEndx: endx:%v -> cut:%v  (delta %v)", prevjob.endx, cut, cut-prevjob.endx)
					prevjob.cuts = append(prevjob.cuts, cut)
					prevjob.newEndx = cut

					need := cut - prevjob.beg
					if need > segment*3 {
						// ugh: need = 2150427 but buf are size 2097152
						panic(fmt.Sprintf("ugh: need = %v but buf are size %v ; missing %v",
							need, segment*3, need-segment*3))
						//buf[i] = make([]byte, segment*2) // what?
					}

					prevjob = nil
				} else {
					//keep = append(keep, cut)
				}
				gkeep = append(gkeep, cut)
				prev = cut

				if cut >= curjob.beg && cut < curjob.endx {
					curjob.cuts = append(curjob.cuts, cut)
				} else {
					break
				}
			}
		}
		prevjob = curjob
	}
	// concluding case.
	prevjob.cuts = append(prevjob.cuts, sz)

	// truncate off the redundant cuts
	for _, curjob := range jobs {
		for i, cut := range curjob.cuts {
			if cut > curjob.endx {
				curjob.cuts = curjob.cuts[:i+1]
				break
			}
		}
	}

	//vv("gkeep = '%#v'", gkeep)
	if false {
		for i := range gkeep {
			if i == 0 {
				continue
			}
			fmt.Printf("d = %v\n", gkeep[i]-gkeep[i-1])
		}
	}

	if false {
		// verify that all cuts are inside their segment data
		prevcut := 0
		for j, curjob := range jobs {
			gotonext := false
			for i, cut := range curjob.cuts {
				extra := ""
				if cut > curjob.endx {
					//panic("cut > curjob.endx, this is a problem")
					extra = "***"
					//curjob.cuts = curjob.cuts[:i+1]
					gotonext = true
				}
				fmt.Printf("job j=%v; [beg:%v , endx:%v)  cut i=%v: %v  (%v)  %v\n", j, curjob.beg, curjob.endx, i, cut, cut-prevcut, extra)
				prevcut = cut
				if gotonext {
					//break
				}
			}
		}
	}

	// re-open work, it was closed.
	work = make(chan *job, 1024)
	wg.Add(int(nWorkers))

	// have the goroutines do the hashing now.
	for worker := 0; worker < nW; worker++ {
		go workfunc(work, int(worker))
	}

	for i := range wchunks {
		jobs[i].genCuts = false
		work <- jobs[i]
	}
	close(work)
	wg.Wait()

	// assemble all the []*Chunk in order.
	// INVAR: nNodes == len(wchunks).

	for j, job := range jobs {
		_ = j
		//showEachSegment(j, job.chunks)

		if len(chunks0.Chunks) > 0 {
			if len(job.chunks) > 0 {
				if job.chunks[0].Beg != chunks0.Chunks[len(chunks0.Chunks)-1].Endx {
					panic(fmt.Sprintf("j=%v; bad append! job.chunks[0].Beg = %v != chunks0.Chunks[len(chunks0.Chunks)-1].Endx = %v", j, job.chunks[0].Beg, chunks0.Chunks[len(chunks0.Chunks)-1].Endx))
				}
			}
		}

		chunks0.Chunks = append(chunks0.Chunks, job.chunks...)
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
