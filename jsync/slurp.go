package jsync

import (
	//"bytes"
	//"compress/gzip"
	//"encoding/csv"
	"fmt"
	"os"
	//"sort"
	"math"
	//"strconv"
	//"strings"
	"sync"
	"syscall"
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

	// 1MB buf for each of up to 8 goro.

	mb := int(math.Ceil(float64(sz) / float64(1<<20)))
	n := numWorkers

	if mb < n {
		n = mb // get smaller, but not larger.
	}

	h := make([]*blake3.Hasher, n)
	buf := make([][]byte, n)
	for i := range n {
		buf[i] = make([]byte, 1<<20)
	}
	sum := make([][]byte, n)

	for i := range h {
		h[i] = blake3.New(64, nil)
	}

	//each := mb / n
	//left := mb - (each * n)

	work := make(chan *job)
	var wg sync.WaitGroup
	wg.Add(n)

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
		}(i)
	}

	njob := 0
	for j := int64(0); j < sz; j += (1 << 20) {
		beg := j
		endx := j + (1 << 20)
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

// for 6.7GB file Ubuntu_24.04_VB_LinuxVMImages.COM.vdi
// 1 thread: '4.521841836s'
// 8  goro in parallel: 631.446854ms on my mac. << fastest with 1MB buffers. 7x faster.
// 12 goro in parallel: 631.787537ms slower.
// 4  goro in parallel: 850.988804ms slower.
// 6  goro            : 709.321204ms slower

func MemoryMapFile(fd *os.File) (mmap []byte) {
	var stat syscall.Stat_t
	err := syscall.Fstat(int(fd.Fd()), &stat)
	panicOn(err)
	sz := stat.Size
	vv("got sz = %v", sz)
	flags := syscall.MAP_SHARED
	prot := syscall.PROT_READ
	mmap, err = syscall.Mmap(int(fd.Fd()), 0, int(sz), prot, flags)
	panicOn(err)
	return
}

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

/*
// say in segsz = 1<<20 for 1MB chunks.
func GetHashesParallel(host, path string) (precis *FilePrecis, chunks *Chunks, err error) {

	if !fileExists(path) {
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false)
	}

	// These two different chunking approaches,
	// Jcdc and FastCDC_Stadia, need very different
	// parameter min/max/target settings in
	// order to give good chunking.

	cdc := jcdc.GetCutpointer(Default_CDC, Default_CDC_Config)
	cdcCfg := cdc.Config()

	sz64, modTime, err := FileSizeModTime(path)
	if err != nil {
		return nil, nil, err
	}
	sz := int(sz64)
	//vv("sz = %v", sz)

	precis = &FilePrecis{
		Host:     host,
		Path:     path,
		FileSize: sz,
		ModTime:  modTime,
		//FileCry:     hash.Blake3OfBytesString(data),
		ChunkerName: cdc.Name(),
		CDC_Config:  cdcCfg,
		HashName:    "blake3.33B",
	}
	chunks = NewChunks(path)
	chunks.FileSize = precis.FileSize

	var fi os.FileInfo
	fi, err = os.Stat(path)
	if err != nil {
		return
	}
	precis.FileMode = uint32(fi.Mode())
	precis.ModTime = modTime

	if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
		uid := stat_t.Uid
		precis.FileOwnerID = uid
		gid := stat_t.Gid
		precis.FileGroupID = gid

		owner, err := user.LookupId(fmt.Sprint(uid))
		if err == nil && owner != nil {
			precis.FileOwner = owner.Username
		}
		group, err := user.LookupGroupId(fmt.Sprint(gid))
		if err == nil && group != nil {
			precis.FileGroup = group.Name
		}
	}

	h := blake3.New(64, nil)

	defer func() {
		if precis != nil && chunks != nil {
			precis.FileCry = hash.SumToString(h)
			chunks.FileCry = precis.FileCry
		}
	}()

	if sz == 0 {
		return
	}

	// this was the fastest buffer size on my mac.
	// It was better than any higher or lower power of 2.
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
	//vv("GetHashesOneByOne returning")
	return
}
*/

/*
func parallelBlake3Hash(data []byte, numWorkers int) []byte {
	// Calculate chunk size - BLAKE3 uses 1KiB chunks internally
	chunkSize := 1024
	chunks := (len(data) + chunkSize - 1) / chunkSize

	// Create channels for workers
	results := make(chan struct {
		index int
		hash  []byte
	}, chunks)

	// Process chunks in parallel
	for i := 0; i < chunks; i++ {
		go func(chunkIndex int) {
			start := chunkIndex * chunkSize
			end := start + chunkSize
			if end > len(data) {
				end = len(data)
			}

			// Create a new hasher for this chunk
			h := blake3.New(64, nil)

			// Important: Set chunk info for proper tree mode
			//h.DeriveKey("chunk", uint64(chunkIndex), uint64(chunks))

			h.Write(data[start:end])
			results <- struct {
				index int
				hash  []byte
			}{chunkIndex, h.Sum(nil)}
		}(i)
	}

	// Collect results
	chunkHashes := make([][]byte, chunks)
	for i := 0; i < chunks; i++ {
		result := <-results
		chunkHashes[result.index] = result.hash
	}

	// Combine hashes using parent nodes
	rootHasher := blake3.New(64, nil)
	rootHasher.DeriveKey("parent", 0, 1)
	for _, hash := range chunkHashes {
		rootHasher.Write(hash)
	}

	var finalHash [33]byte
	copy(finalHash[:], rootHasher.Sum(nil))
	return finalHash[:]
}
*/
