package jsync

import (
	//"bytes"
	//"compress/gzip"
	//"encoding/csv"
	"fmt"
	"os"
	//"sort"
	//"math"
	//"strconv"
	//"strings"
	"sync"
	//"syscall"
	"time"
	//"unsafe"
	"io"

	cristalbase64 "github.com/cristalhq/base64"
	"lukechampine.com/blake3"
)

// to allow parallel processing, we might well need
// to define only 1MB chunk boundaries as being the
// part within which CDC is applied. That is, guarantee
// that all chunks end on 1MB boundaries, even if
// they would not naturally end there.

type job struct {
	beg  int64
	endx int64
}

func SlurpBlake(path string, numWorkers int) (blake3sum string, elap time.Duration, err0 error) {

	t0 := time.Now()

	// first argument is the path to the csv file to parse
	fd, err := os.Open(path)
	if err != nil {
		err0 = err
		return
	}
	defer fd.Close()

	fi, err := fd.Stat()
	panicOn(err)
	sz := fi.Size()
	_ = sz

	//	blake3sum = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by)
	//	return

	// 512KB buf for each of up to n goro.
	// This is faster on my mac than 1MB. See table below.
	const segment = 1 << 19

	mb := sz / segment
	if sz*segment < mb {
		mb++ // round up. any fraction left at the end still gets processed.
	}
	n := int64(numWorkers)

	if mb < n {
		n = mb // get smaller, but not larger.
	}

	h := make([]*blake3.Hasher, n)
	buf := make([][]byte, n)
	for i := range n {
		buf[i] = make([]byte, segment)
	}
	sum := make([][]byte, n)

	for i := range h {
		h[i] = blake3.New(64, nil)
	}

	//each := mb / n
	//left := mb - (each * n)

	work := make(chan *job)
	var wg sync.WaitGroup
	wg.Add(int(n))

	for i := range n {
		go func(i int) {
			defer func() {
				//vv("goro i = %v is done.", i)
				wg.Done()
			}()

			f, err := os.Open(path)
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
				//vv("goro i=%v starting on job at %v", i, job.beg)
				f.Seek(job.beg, 0)
				need := job.endx - job.beg
				nr, err := io.ReadFull(f, buf[i][:need])
				// either io.EOF (0 bytes) or
				// io.ErrUnexpectedEOF (nr<need) are problems.
				panicOn(err)

				if int64(nr) != need {
					panic(fmt.Sprintf("short read!?!: path = '%v'. expected = %v; got = %v; on worker i=%v", path, need, nr, i))
				}
				h[i].Write(buf[i][:need])
				sum[i] = h[i].Sum(nil)
			}
		}(int(i))
	}

	njob := 0
	for j := int64(0); j < sz; j += segment {
		beg := j
		endx := j + segment
		if endx > sz {
			endx = sz
		}
		job := &job{
			beg:  beg,
			endx: endx,
		}
		work <- job
		njob++
	}
	//vv("sent off njob = %v to be hashed", njob)
	close(work)
	wg.Wait()
	elap = time.Since(t0)
	vv("done after %v", elap)

	///	h.Write(buf)
	//	by := h.Sum(nil)

	//blake3sum = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33])

	return
}

// for 6.7GB file Ubuntu_24.04_VB_LinuxVMImages.COM.vdi, in 1MB chunks:
// on my mac:
// 1 thread: '4.521841836s'
// 8  goro in parallel: 631.446854ms on my (quad-core) mac. << fastest with 1MB buffers. 7x faster.
// 12 goro in parallel: 631.787537ms slower.
// 4  goro in parallel: 850.988804ms slower.
// 6  goro            : 709.321204ms slower

/* in 2MB chunks on my mac
with parallelism 1  elap = 3.184791242s
with parallelism 2  elap = 1.653944354s
with parallelism 3  elap = 1.134502528s
with parallelism 4  elap = 880.286746ms
with parallelism 5  elap = 796.953449ms
with parallelism 6  elap = 738.926453ms
with parallelism 7  elap = 694.493438ms
with parallelism 8  elap = 659.03217ms <<<<<< min here; 2MB slower than 1MB.
with parallelism 9  elap = 709.963668ms
with parallelism 10  elap = 722.737926ms
with parallelism 11  elap = 661.48533ms

In 1MB chunks on my mac:

with parallelism 1  elap = 3.181728135s
with parallelism 2  elap = 1.64416469s
with parallelism 3  elap = 1.113680367s
with parallelism 4  elap = 870.23991ms
with parallelism 5  elap = 803.798936ms
with parallelism 6  elap = 712.522304ms
with parallelism 7  elap = 665.325466ms
with parallelism 8  elap = 655.35979ms
with parallelism 9  elap = 631.725325ms <<<< min here, 1MB faster than 2MB
with parallelism 10  elap = 659.761366ms
with parallelism 11  elap = 655.26455ms

even faster using 512KB instead of 1MB:

with parallelism 1  elap = 3.142123283s
with parallelism 2  elap = 1.607804187s
with parallelism 3  elap = 1.09853213s
with parallelism 4  elap = 868.66514ms
with parallelism 5  elap = 761.105816ms
with parallelism 6  elap = 689.614737ms
with parallelism 7  elap = 639.682723ms
with parallelism 8  elap = 619.470931ms
with parallelism 9  elap = 617.620862ms
with parallelism 10  elap = 612.781212ms <<<<< fastest
with parallelism 11  elap = 632.990142ms

*/

func Blake3OfFileParallel(path string) (blake3sum string, err error) {
	fd, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fd.Close()

	/*
		bufsz := 1 << 20 // 1.901690203s fo N=1000;  N=2000 => 3.800150287s

		fd, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		defer fd.Close()

		k := 0 // chunk count
		addChunk := func(slc []byte, beg int) {
			hsh := hash.Blake3OfBytesString(slc)
			//fmt.Printf("[%03d]GetHashes hsh = %v\n", k, hsh)
			k++
			chunk := &Chunk{
				Beg:  beg,
				Endx: beg + len(slc),
				Cry:  hsh,
			}
			chunks.Chunks = append(chunks.Chunks, chunk)
			h.Write(slc)
		}

		//vv("using bufsz = %v in GetHashesOneByOne", bufsz)
		buf := make([]byte, bufsz)

		var totalread int // total bytes read from file so far.

		var bufbeg int  // offset where buf starts in the origin file.
		var bufendx int // offset where buf ends in the origin file.
		var dataoff int // offset where data starts in the original file. pass to addChunk()

		var nr int
		nr, err = io.ReadFull(fd, buf)
		if err == io.EOF {
			// no more bytes, exactly 0.
			// "The error is EOF only if no bytes were read."
			// -- https://pkg.go.dev/io#ReadFull
			err = nil
			return
		}
		bufendx += nr
		totalread += nr // gives offset of end of data

		if err == io.ErrUnexpectedEOF {
			err = nil
			// ignore short read error in next err != nil check.
		}
		if err != nil {
			//panicOn(err)
			return nil, nil, err
		}
		data := buf[:nr]
		// dataoff = 0, as it was.

		for j := 0; len(data) > 0; j++ {

			cut := cdc.NextCut(data)

			if len(data) >= cdcCfg.MaxSize {
				// legit cut
				addChunk(data[:cut], dataoff)
				//vv("j=%v  legit cut: '%v'", j, chunks.Chunks[len(chunks.Chunks)-1])
				data = data[cut:]
				dataoff += cut
				if len(data) > cdcCfg.MaxSize {
					continue
				}
			}
			// INVAR: len(data) < opts.MaxSize, we need to read from file, if we can.
			if bufendx == sz {
				//vv("no more data available, the last will be oddly truncated.")
				for len(data) > 0 {
					cut := cdc.NextCut(data)
					addChunk(data[:cut], dataoff)
					data = data[cut:]
					dataoff += cut
				}
				//vv("j=%v  no more data, last chunk: '%v'", j, chunks.Chunks[len(chunks.Chunks)-1])
				return
			}
			// data is out of space, read from file again

			// move what we have left in data to the beginning of buf,
			// before we read again, so we don't lose it.
			k := copy(buf, data)
			bufbeg = dataoff
			bufendx = dataoff + k
			//vv("data out of space, read from file. copied k = %v", k)

			nr, err = io.ReadFull(fd, buf[k:])
			if err == io.EOF {
				// no more bytes, exactly 0.
				// "The error is EOF only if no bytes were read."
				// -- https://pkg.go.dev/io#ReadFull
				err = nil
				//vv("we see EOF after totalread = %v", totalread)
				return
			}
			bufendx += nr
			totalread += nr

			if err == io.ErrUnexpectedEOF {
				// buf is less than full. meh. ignore this error.
				err = nil // prevent next if err != nil from returning.

				//vv("ignoring ErrUnexpectedEOF; nr = %v", nr)
			}
			if err != nil {
				//panicOn(err)
				return nil, nil, err
			}
			data = buf[:(bufendx - bufbeg)]
			// dataoff is fine.
			if len(data) == 0 && totalread != sz {
				panic("why is data len 0 when have not ready whole file?")
			}
		}
	*/

	h := blake3.New(64, nil)
	io.Copy(h, fd)
	by := h.Sum(nil)

	blake3sum = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33])
	return
}
