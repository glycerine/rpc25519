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
	bytes0 "github.com/glycerine/rpc25519/bytes"
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
	rle0 []bool // is rle0 candidate?

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

	//vv("top of ChunkFile2")
	//defer func() {
	//vv("ChunkFile2 returning; err0 = '%v'; len chunks0.Chunks = %v", err0, len(chunks0.Chunks))
	//}()
	// must handle non-existant files without error.
	if !fileExists(path) {
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false)
	}
	//vv("file exists")
	t0 := time.Now()
	_ = t0
	fd, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		//vv("ChunkFile2: error on OpenFile(path='%v'): '%v'", path, err)
		err0 = err
		return
	}
	defer fd.Close()

	fi, err := fd.Stat()
	if err != nil {
		//vv("path did not stat: '%v': '%v'", path, err)
		err0 = err
		return
	}
	sz := int(fi.Size())
	if sz == 0 {
		//vv("path is empty! '%v'", path)
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false)
	}
	//vv("file is not empty")
	cdcCfg := Default_CDC_Config
	mincut := int(cdcCfg.MinSize) // filter for this mincut on 2nd pass.
	mincutCand := mincut
	//awfull mincutCand := 64              // try to get all the candidates on first pass.
	//awful! cdcCfg.MinSize = mincutCand

	cdc := jcdc.GetCutpointer(Default_CDC, cdcCfg)

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

	// Without preRead,          180 chunks different, 74MB on Ub.
	// 1 seg preRead too gets us: 68 chunks different, 53MB on Ub.
	// 2 seg preRead              49 chunks different, 40MB on Ub. 33sec.
	// 2 seg preRead with 16/64/128 264 diff, 30MB. 21sec mac, 12.6sec linux.
	// 3 seg               4/16/128           18MB             13.7sec linux.
	//
	// Nice: rsync: 34 sec; vs ChunkFile 9.2 sec to update Ub across lan
	// after appending a few bytes.
	preRead := 2 * minSegSize
	postRead := minSegSize
	//preRead := 3 * minSegSize
	//postRead := 2 * minSegSize

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
		//buf[i] = make([]byte, segment*5)
		buf[i] = make([]byte, segment*7)
	}

	// buffered channel for less waiting on scheduling.
	work := make(chan *job, 1024)
	var wg sync.WaitGroup
	wg.Add(int(nWorkers))

	nJobs := (sz + segment - 1) / segment
	//vv("nJobs = %v", nJobs)

	// output
	jobs := make([]*job, nJobs)

	maxcut := int(Default_CDC_Config.MaxSize)
	nW := int(nWorkers)
	//vv("nW = %v; mincut = %v; maxcut = %v", nW, mincut, maxcut)

	workfunc := func(work chan *job, worker int) {

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

			pre := preRead
			if job.beg < pre {
				pre = job.beg
			}
			lastCut := 0
			if !job.genCuts {
				pre = 0
				//vv("on hashing... job = '%#v'", job)
				nc := len(job.cuts)
				if nc == 0 {
					return
				}
				lastCut = job.cuts[nc-1]
				if job.newEndx < lastCut {
					// see lots of:
					//vv("updating newEndx %v -> %v", job.newEndx, lastCut)
					// essential to getting hashes to match!
					job.newEndx = lastCut
				}
			}

			seek0 := job.beg - pre
			f.Seek(int64(seek0), 0)

			lenseg := (job.newEndx - seek0)
			if lenseg == 0 {
				panic("lenseg should not be 0")
			}

			if job.genCuts && !job.isLast {
				lenseg += postRead
				if job.endx+postRead > sz {
					lenseg = sz - seek0
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
				dataoff := seek0

				// look for RLE0; encodable spans of zeros.
				beg0, endx0 := bytes0.LongestZeroSpan(data)
				// start simple. Just take leading spans that are big enough.
				if beg0 == 0 && endx0 >= 64 {
					//vv("have a span of 0: [%v, %v) appending RLE0; cut: %v", beg0, endx0, dataoff+endx0)
					job.cand = append(job.cand, dataoff+endx0)
					job.rle0 = append(job.rle0, true)
					data = data[endx0:]
					dataoff += endx0
				}

				for j := 0; len(data) >= mincutCand; j++ {
					relcut := cdc.NextCut(data)
					job.cand = append(job.cand, dataoff+relcut)
					job.rle0 = append(job.rle0, false)
					data = data[relcut:]
					dataoff += relcut
				}
				jobs[job.nodeK] = job

			} else {
				//vv("gen hashes job = '%#v'", job)
				if len(job.cuts) == 0 {
					return
				}
				dataoff := job.cuts[0]
				// job.beg is where data starts
				prev := dataoff - job.beg
				// prev = job.cuts[0](2146461) - job.beg(3145728) = -999267
				//vv("prev = job.cuts[0](%v) - job.beg(%v) = %v", job.cuts[0], job.beg, prev)
				for i, cut := range job.cuts {
					if i == 0 {
						continue
					}
					d := cut - dataoff
					if d == 0 {
						//vv("job.cuts = '%#v'", job.cuts)
						panic(fmt.Sprintf("shoud not have empty chunk! cut = %v; i=%v;  prev=%v; dataoff = %v; job.beg = %v; nodeK=%v", cut, i, prev, dataoff, job.beg, job.nodeK))
					}
					// prev = -999267; d = 16384
					//vv("prev = %v; d = %v", prev, d)
					slc := data[prev : prev+d]
					chunk := &Chunk{
						Beg:  dataoff,
						Endx: dataoff + d,
					}
					if bytes0.AllZero(slc) {
						chunk.Cry = "RLE0;"
					} else {
						chunk.Cry = hash.Blake3OfBytesString(slc)
					}
					prev += d
					//if job.nodeK >= 6812 {
					//	vv("job.nodeK=%v; chunk [%v, %v) = %v", job.nodeK, chunk.Beg, chunk.Endx, chunk.Cry)
					//}
					job.chunks = append(job.chunks, chunk)
					dataoff += d
				}
			}
		}
	} // end workfunc

	for worker := 0; worker < nW; worker++ {
		go workfunc(work, int(worker))
	}

	// send off all the jobs

	last := nJobs - 1
	for i := range nJobs {

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
	//vv("we have sent off njob = nJobs = %v to be hashed", nJobs)
	close(work)
	wg.Wait()

	//couldNotResync := 0

	// compute keeper cutpoints
	var gkeep []int

	prev := 0

	lastjob := len(jobs) - 1
	for i, curjob := range jobs {
		if i == 0 {
			// base case
			curjob.cuts = []int{0}
		}
		if false {
			vv("here are candidates: jobs[%v].cand = '%#v'", i, jobs[i].cand)
			prev := 0
			for _, cut := range jobs[i].cand {
				fmt.Printf("%v (%v)\n", cut, cut-prev)
				prev = cut
			}
		}

		for j, cut := range curjob.cand {
			_ = j
			if cut <= prev {
				continue
			}
			if curjob.rle0[j] {
				// give RLE; priority, just accept it, without clamping.

			} else {

				d := cut - prev
				if d < mincut {
					continue
				}
				if d >= maxcut {
					//vv("d over maxcut, will clamp %v -> %v", cut, prev+maxcut)
					cut = prev + maxcut
					d = maxcut
				}
			}
			gkeep = append(gkeep, cut)
			prev = cut

			n := len(jobs[i].cuts)
			if n > 0 && jobs[i].cuts[n-1] == cut {
				// do not add redundant cut!
			} else {
				jobs[i].cuts = append(jobs[i].cuts, cut)
			}

			if cut >= curjob.endx {
				if i != lastjob {
					// start the next. leaving out => bad append.
					// This tells the next job that where
					// they should start from.
					jobs[i+1].cuts = append(jobs[i+1].cuts, cut)
				}
				break // go to next job
			}
		}
	}
	// concluding case.
	n := len(jobs[lastjob].cuts)
	if n > 0 && jobs[lastjob].cuts[n-1] == sz {
		// do not add redundant cut!
		// This *is* reached.
	} else {
		if len(jobs[lastjob].cuts) == 0 && lastjob > 0 {
			jobs[lastjob-1].cuts = append(jobs[lastjob-1].cuts, sz)
		} else {
			// may not be right, but where else to stash it?
			jobs[lastjob].cuts = append(jobs[lastjob].cuts, sz)
		}
	}
	//vv("debug/selected cuts:")
	//printCutsPerJob(0, jobs, false)

	//vv("gkeep = '%#v'", gkeep)
	if false {
		for i := range gkeep {
			if i == 0 {
				continue
			}
			fmt.Printf("d = %v\n", gkeep[i]-gkeep[i-1])
		}
	}

	//vv("sz = %v", sz)

	if false {
		lastj := len(jobs) - 1 // debug
		printCutsPerJob(lastj-1, jobs[lastj-1:], true)
	}

	//showEachSegment(lastj, jobs[lastj].chunks)

	// re-open work, it was closed.
	work = make(chan *job, 1024)
	wg.Add(int(nWorkers))

	// have the goroutines do the hashing now.
	for worker := 0; worker < nW; worker++ {
		go workfunc(work, int(worker))
	}

	for i := range nJobs {
		jobs[i].genCuts = false
		work <- jobs[i]
	}
	close(work)
	wg.Wait()

	// assemble all the []*Chunk in order.
	// INVAR: nJobs == len(wchunks).

	for j, job := range jobs {
		_ = j
		//if j == len(jobs)-1 {
		//showEachSegment(j, job.chunks)
		//}

		if len(chunks0.Chunks) > 0 {
			if len(job.chunks) > 0 {
				if job.chunks[0].Beg != chunks0.Chunks[len(chunks0.Chunks)-1].Endx {
					//vv("elap = '%v'", time.Since(t0))

					//vv("chunks0.Chunks[last] = ")
					showEachSegment(-1, chunks0.Chunks[len(chunks0.Chunks)-1:])

					//vv("job.chunks[:1] = ")
					showEachSegment(-1, job.chunks[:1])

					panic(fmt.Sprintf("j=%v; bad append! job.chunks[0].Beg = %v != chunks0.Chunks[len(chunks0.Chunks)-1].Endx = %v", j, job.chunks[0].Beg, chunks0.Chunks[len(chunks0.Chunks)-1].Endx))
				}
			}
		}

		chunks0.Chunks = append(chunks0.Chunks, job.chunks...)
	}
	if false {
		printCutsPerJob(0, jobs, true)
	}

	// coalesce runs of zeros
	var coal []*Chunk

	var lastRLE *Chunk
	total0 := 0
	for _, chnk := range chunks0.Chunks {
		if chnk.Cry == "RLE0;" {
			if lastRLE != nil {
				// coalesce
				lastRLE.Endx = chnk.Endx
				// leave lastRLE same
			} else {
				coal = append(coal, chnk)
				lastRLE = chnk
			}
			total0 += (chnk.Endx - chnk.Beg)
			continue
		}
		// not RLE0;
		coal = append(coal, chnk)
		lastRLE = nil
	}
	if len(coal) == 0 {
		panic("len coal cannot be 0")
	}
	//vv("len coal = '%v' vs len orig %v", len(coal), len(chunks0.Chunks))
	//vv("RLE0 encoded %v bytes; path = '%v'; host='%v'", total0, path, host)
	chunks0.Chunks = coal

	return

} // end ChunkFile2

func printCutsPerJob(begJobNum int, jobs []*job, showChunks bool) {
	// print cuts for each segment.
	prevcut := 0
	k := begJobNum - 1
	for _, curjob := range jobs {
		k++
		if !showChunks {
			fmt.Printf("\n  job %03d ----- 'cut' view from phase 1:\n", k)
			for i, cut := range curjob.cuts {
				extra := ""
				if cut > curjob.endx {
					extra = "***"
				}
				fmt.Printf("job j=%03d: [%v, %v)  cut i=%v: %v  (%v)  %v\n", k, curjob.beg, curjob.endx, i, cut, cut-prevcut, extra)
				prevcut = cut
			}
		} else {
			if len(curjob.chunks) > 0 {
				fmt.Printf("\n job %03d ----- 'chunk' view from phase 2:\n", k)
				for _, c := range curjob.chunks {
					extra := ""
					if c.Endx > curjob.endx {
						extra = "*"
					}
					fmt.Printf(" job %03d seg [%v, %v) chnk [ %6d : %6d) (len %6d) %v %v\n", k, curjob.beg, curjob.endx, c.Beg, c.Endx, (c.Endx - c.Beg), c.Cry[11:20], extra)
				}
				fmt.Println()
			} else {
				fmt.Printf("   ...(no chunks avail for job %v [%v, %v))\n",
					k, curjob.beg, curjob.endx)
			}
		}
	}

	fmt.Printf("\n============\n")
}

func showEachSegment(i int, cs []*Chunk) {
	fmt.Printf("job/segment i = %v\n", i)
	for j, c := range cs {
		cry := c.Cry
		if cry != "RLE0;" {
			cry = c.Cry[11:20]
		}
		fmt.Printf("  %03d  Chunk:[Beg:%6d : Endx:%6d ) (len %6d) %v\n",
			j, c.Beg, c.Endx, (c.Endx - c.Beg), cry)
	}
}

func showCuts(i int, cuts []int) {
	prevcut := 0
	for i, cut := range cuts {
		fmt.Printf(" cut i=%v: %v  (%v)\n", i, cut, cut-prevcut)
		prevcut = cut
	}
}
