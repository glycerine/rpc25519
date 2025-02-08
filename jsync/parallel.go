package jsync

import (
	//"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
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

	//cdc := jcdc.GetCutpointer(Default_CDC, Default_CDC_Config)

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/target settings in
	// order to give good chunking.

	cdcCfg := &jcdc.CDC_Config{
		MinSize:    2 * 1024,
		TargetSize: 8 * 1024,
		MaxSize:    64 * 1024,
	}

	chunker := jcdc.FastCDC_PlakarAlgo
	//chunker := jcdc.FastCDC_StadiaAlgo
	//chunker := jcdc.RabinKarp_Algo
	//chunker := jcdc.UltraCDC_Algo
	//chunker := jcdc.FNV_Algo

	// UltraCDC_Algo      CDCAlgo = 0
	// FastCDC_StadiaAlgo CDCAlgo = 1
	// FastCDC_PlakarAlgo CDCAlgo = 2
	// FNV_Algo           CDCAlgo = 3
	// RabinKarp_Algo     CDCAlgo = 4

	cdc := jcdc.GetCutpointer(chunker, cdcCfg)

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
	if parallelBits != 0 {
		segment = 1 << parallelBits
	}
	minsz := int(Default_CDC_Config.MaxSize) // min 64KB (1 << 16)
	if segment < minsz {
		segment = minsz
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

	// output
	wchunks := make([][]*Chunk, nNodes)
	overlaps := make([][]*Chunk, nNodes-2)

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
				//now we take any sized cut
				for j := 0; len(data) > 0; j++ {
					cut := cdc.NextCut(data)
					addChunk(data[:cut], dataoff)
					data = data[cut:]
					dataoff += cut
				}
				wchunks[job.nodeK] = chunks

				// do overlaps too, unless last/next-to-last.
				if !job.isLast && !job.isPenult {

					halfway := lenseg / 2
					beg := job.beg + halfway
					endx := job.endx + halfway

					f.Seek(int64(beg), 0)
					lenseg = endx - beg
					_, err := io.ReadFull(f, buf[worker][:lenseg])
					panicOn(err)
					// offset where data starts in the original file;
					// to pass to addChunk
					dataoff = beg
					data = buf[worker][:lenseg]

					chunks = overlaps[job.nodeK]
					//now we take any sized cut
					for j := 0; len(data) > 0; j++ {
						cut := cdc.NextCut(data)
						addChunk(data[:cut], dataoff)
						data = data[cut:]
						dataoff += cut
					}
					overlaps[job.nodeK] = chunks

				}
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

	// index overlap begins
	ob := make(map[int]*Chunk)
	var oblin []*Chunk
	for _, cs := range overlaps {
		for _, c := range cs {
			ob[c.Beg] = c
			oblin = append(oblin, c)
		}
	}
	nlook := 0
	nbridge := 0
	for i := 0; i < nNodes; i++ {

		c := wchunks[i]
		k := len(c)
		if k > 0 && i < penult {
			// look for bridges
			nlook++
			// can we repair the inter-chunk?
			lastcut := c[k-1].Beg
			firstcut := wchunks[i+1][0].Beg
			bridge, ok := ob[lastcut]
			if ok && bridge.Endx == firstcut {
				vv("we have a bridge")
				nbridge++
				// skip the last of whunks[i]
				chunks0.Chunks = append(chunks0.Chunks, c[:k-1]...)
				// put in the bridge Chunk.
				chunks0.Chunks = append(chunks0.Chunks, bridge)
				// and skip the first of wchunks[i+1]
				wchunks[i+1] = wchunks[i+1][1:]
				continue
			} else {
				closest2lastcut := sort.Search(len(oblin), func(j int) bool {
					return oblin[j].Beg >= lastcut
				})
				closest2firstcut := sort.Search(len(oblin), func(j int) bool {
					return oblin[j].Endx >= firstcut
				})
				vv("no bridge [lastcut=%v, firstcut=%v) closest2lastcut = '%v'; closest2firstcut = '%v'", lastcut, firstcut, closest2lastcut, closest2firstcut)
			}
		}
		chunks0.Chunks = append(chunks0.Chunks, c...)
	}
	vv("len chunks0.Chunks = %v", len(chunks0.Chunks))
	vv("nlook = %v, and nbridge = %v", nlook, nbridge)
	// default min chunk 2K: nlook = 6811, and nbridge = 85
	// min chunk 2 bytes:    nlook = 6812, and nbridge = 86
	return
}
