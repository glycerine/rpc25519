package main

import (
	"crypto/sha256"
	//"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/glycerine/rpc25519/jcdc"
	"github.com/glycerine/rpc25519/jsync"
)

var algo int

func setFlags(c *jcdc.CDC_Config, fs *flag.FlagSet) {
	fs.IntVar(&c.MinSize, "min", 2048, "min size chunk")
	fs.IntVar(&c.TargetSize, "t", 10*1024, "target size chunk")
	fs.IntVar(&c.MaxSize, "max", 64*1024, "max size chunk")
	fs.IntVar(&algo, "algo", 0, "algo: 0=>ultracdc, 1=>fastcdc_stadia; 2=>fastcdc_plakar; 4=>fnv1a")
}

func main() {
	cfg := &jcdc.CDC_Config{}

	fs := flag.NewFlagSet("chunk", flag.ExitOnError)
	setFlags(cfg, fs)
	fs.Parse(os.Args[1:])

	paths := fs.Args()
	vv("paths = '%#v'", paths)

	cdc, _ := jsync.GetCutpointer(jsync.CDCAlgo(algo))
	cdc.SetConfig(cfg)

	for i, path := range paths {
		_ = i
		data, err := os.ReadFile(path)
		panicOn(err)

		cuts, cmap := getCuts(cdc.Name(), data, cdc, cfg)
		ndup := 0
		savings := 0
		sdt := StdDevTracker{}
		for _, v := range cmap {
			if v.n > 1 {
				ndup++
				savings += v.sz
			}
			sdt.AddObs(float64(v.sz), float64(v.n))
		}

		fmt.Printf("algo = %v; ncut = %v; ndup = %v; savings = %v; mean=%v; sd=%v\n", algo, len(cuts), ndup, savings, sdt.Mean(), sdt.SampleStdDev())
	}
	//	fmt.Printf("cuts = '%#v'\n", cuts)
	/*
		// check that Cutpoints() gives the same.
		//fmt.Printf("len(data) = %v, N = %v\n", len(data), N)
		u.Opts = opt
		cuts2 := u.Cutpoints(data, 0)
		//fmt.Printf("len(cuts2) = %v\n", len(cuts2))
		//fmt.Printf("cuts2[len(cuts2)-1] = '%v'\n", cuts2[len(cuts2)-1])
		//fmt.Printf("len(expectedCuts) = %v\n", len(expectedCuts))
		if len(cuts2) != len(expectedCuts) {
			t.Fatalf(`Cutpoints(): expected len(cuts2)=%v to be %v`, len(cuts2), len(expectedCuts))
		}
		for j, cut := range cuts2 {
			if expectedCuts[j] != cut {
				t.Fatalf(`Cutpoints(): expected %v but got %v at j = %v`, expectedCuts[j], cut, j)
			}
		}
	*/
}

type seg struct {
	n  int
	sz int
}

func getCuts(
	title string,
	data []byte,
	u jcdc.Cutpointer,
	opt *jcdc.CDC_Config,
) (cuts []int, m map[string]*seg) {

	m = make(map[string]*seg)
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
		v, ok := m[hashOfBytes(data[:cutpoint])]
		if !ok {
			v = &seg{
				sz: cutpoint,
			}
		}
		v.n++
		data = data[cutpoint:]
	}
	return
}

func hashOfBytes(by []byte) string {
	h := sha256.New()
	h.Write(by)
	enchex := hex.EncodeToString(h.Sum(nil))
	return enchex
}
