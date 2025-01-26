package jcdc

import (
	//"crypto/sha256"
	//"encoding/binary"
	//cryrand "crypto/rand"
	//"crypto/sha256"
	//"encoding/binary"
	//"encoding/hex"
	"fmt"
	mathrand2 "math/rand/v2"
	//"os"
	//"strconv"
	"testing"
)

func getPseudoRandData(gen *mathrand2.ChaCha8, sz int) []byte {

	data := make([]byte, sz)
	gen.Read(data)
	return data

	// opt := Default_UltraCDC_Options()
	// opt.MinSize = 1
	// opt.MaxSize = 8000
	// opt.TargetSize = 24
	// u := NewUltraCDC(opt)
	// cuts, hashmap := getCuts("orig", data, u, opt)

	// // how many segments change if we alter the data? just by prepending 2 bytes.
	// differ := 0
	// data = append([]byte{0x39, 0x46}, data...)
	// cuts2, hashmap2 := getCuts("with prepend 2 bytes -- ", data, u, opt)
	// for j, cut := range cuts2 {
	// 	if cuts[j] != cut {
	// 		differ++
	// 	}
	// }
	// //fmt.Printf("after pre-pending 2 bytes, the number of cuts that differ = %v; out of %v\n", differ, len(cuts))

	// matchingHashes := 0
	// for hash0 := range hashmap {
	// 	if hashmap2[hash0] {
	// 		matchingHashes++
	// 	}
	// }
	// //fmt.Printf("matchingHashes = %v\n", matchingHashes)

	// // good: just the first segment changed.
	// if matchingHashes != 500 || len(cuts) != 501 {
	// 	t.Fatalf("should had 500 out of 501 matching hashes; matchingHashes = %v; len(cuts) = %v", matchingHashes, len(cuts))
	// }
}

// Vary the number and size of changes to a file,
// and measure how much each chunker needs to send.
// the first segment's hash.
func TestDiffSize(t *testing.T) {

	scenarios := 20
	nAlgo := 5
	totalByteDeltaOverAllScenario := make([]int, nAlgo)
	var names []string
	var cfg *CDC_Config

	for nChange := 1; nChange < scenarios; nChange++ {

		// deterministic pseudo-random numbers as data.
		var seed [32]byte
		seed[0] = byte(nChange)
		gen := mathrand2.NewChaCha8(seed)

		sz := 1 << 20
		data := getPseudoRandData(gen, sz)

		data2 := append([]byte{}, data...) // change this copy

		maxChangeLen := 1024
		maxByteDelta := 0
		for i := range nChange {
			_ = i
			amt := int(gen.Uint64() % uint64(maxChangeLen))
			loc := int(gen.Uint64() % uint64(len(data)-amt))
			change := make([]byte, amt)
			gen.Read(change)
			k := copy(data2[loc:], change)
			maxByteDelta += k
		}
		bytesDiff := 0
		for i := range data {
			if data[i] != data2[i] {
				bytesDiff++
			}
		}
		_ = maxByteDelta
		testName := fmt.Sprintf("1 MB file, %v planted changes of up to %v bytes;\n %v bytes was the total delta in bytes.\n", formatUnder(nChange), formatUnder(maxChangeLen), formatUnder(bytesDiff))

		cfg = &CDC_Config{
			MinSize:    512, //2 * 1024,
			TargetSize: 64 * 1024,
			MaxSize:    128 * 1024,
		}
		//cfg = nil // TODO vary! for now, defaults

		for algo := 0; algo < nAlgo; algo++ {
			cdc := GetCutpointer(CDCAlgo(algo), cfg)

			if nChange == 1 {
				names = append(names, cdc.Name())
			}
			cfg = cdc.Config() // overwrite with actually in use.
			sums0 := getCuts2(cdc.Name(), data, cdc, cfg)
			sums2 := getCuts2(cdc.Name(), data2, cdc, cfg)

			cuts0, cmap0 := sums0.cuts, sums0.cmap
			_ = cuts0
			ndup := 0
			bytedup := 0
			nnew := 0
			bytenew := 0

			cuts2, cmap2 := sums2.cuts, sums2.cmap
			_ = cuts2

			//vv("len cuts = %v; len cmap = %v", len(cuts), len(cmap))
			sdt0 := StdDevTracker{}
			// stats for orig data
			for _, v := range cmap0 {
				sdt0.AddObs(float64(v.sz), float64(v.n))
			}

			sdt2 := StdDevTracker{}
			for k, v := range cmap2 {
				data, ok := cmap0[k]
				if ok {
					// data is in the orignal
					ndup++
					bytedup += data.sz
				} else {
					// data is not in the orig
					nnew++
					bytenew += v.sz
				}
				sdt2.AddObs(float64(v.sz), float64(v.n))
			}

			if algo == 0 {
				fmt.Println()
				fmt.Printf("\n =======================\n scenario: %v"+
					"\n with cfg = '%#v'", testName, cfg)
				fmt.Printf("\n =======================\n")
				fmt.Println()
			}

			//vv("%v  orig    data sz = %v ;  nchunks = %v; meanChunkSz = %v; sd = %v", cdc.Name(), formatUnder(sz), formatUnder(len(cuts0)), formatUnder(int(sdt0.Mean())), formatUnder(int(sdt0.SampleStdDev())))
			//vv("%v  vs  data2 sz = %v ;  nchunks = %v; meanChunkSz = %v; sd = %v", cdc.Name(), formatUnder(sz), formatUnder(len(cuts2)), formatUnder(int(sdt2.Mean())), formatUnder(int(sdt2.SampleStdDev())))
			fmt.Printf("%v:\n", cdc.Name())
			fmt.Printf("ndup chunk = %3v ; bytedup = %9v; nnew chunk = %5v; bytenew = %9v\n", formatUnder(ndup), formatUnder(bytedup), formatUnder(nnew), formatUnder(bytenew))
			totalByteDeltaOverAllScenario[algo] += bytenew
		}
	} // end for nChange
	fmt.Println()
	fmt.Printf(" =================\n Over all scenarios:  min=%v; target=%v; max=%v\n", formatUnder(cfg.MinSize), formatUnder(cfg.TargetSize), formatUnder(cfg.MaxSize))
	fmt.Printf(" =================\n\n")
	for algo := range nAlgo {
		fmt.Printf("%10s bytes total in deltas  : %.40s\n", formatUnder(totalByteDeltaOverAllScenario[algo]), names[algo])
	}
	fmt.Println()

}

type seg struct {
	n  int
	sz int
}

type sum struct {
	cuts []int
	cmap map[string]*seg
}

func getCuts2(
	title string,
	data []byte,
	u Cutpointer,
	opt *CDC_Config,
) *sum {

	var cuts []int
	m := make(map[string]*seg)
	last := 0
	j := 0
	for len(data) > opt.MinSize {
		cutpoint := u.Algorithm(opt, data, len(data))
		if cutpoint == 0 {
			panic("should never get cutpoint 0 now")
		}
		cut := last + cutpoint
		cuts = append(cuts, cut)
		last = cut
		j++
		ha := hashOfBytes(data[:cutpoint])
		v, ok := m[ha]
		if !ok {
			v = &seg{
				sz: cutpoint,
			}
		}
		v.n++
		m[ha] = v
		data = data[cutpoint:]
	}

	return &sum{cuts: cuts, cmap: m}
}
