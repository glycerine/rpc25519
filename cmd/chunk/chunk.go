package main

import (
	"crypto/sha256"
	//"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/glycerine/rpc25519/jcdc"
	//"github.com/glycerine/rpc25519/jsync"
)

// var algo int
var bits int

func setFlags(c *jcdc.CDC_Config, fs *flag.FlagSet) {
	fs.IntVar(&c.MinSize, "min", 4*1024, "min size chunk")
	fs.IntVar(&c.TargetSize, "t", 64*1024, "target size chunk")
	fs.IntVar(&c.MaxSize, "max", 256*1024, "max size chunk")
	// fs.IntVar(&algo, "algo", 0, "algo: 0=>ultracdc, 1=>fastcdc_stadia; 2=>fastcdc_plakar; 4=>fnv1a")
	fs.IntVar(&bits, "bits", 12, "how many 0 at the low end to declare a chunk, in FNV1a rollsumming")
}

func main() {
	cfg := &jcdc.CDC_Config{}

	fs := flag.NewFlagSet("chunk", flag.ExitOnError)
	setFlags(cfg, fs)
	fs.Parse(os.Args[1:])

	paths := fs.Args()
	//vv("paths = '%#v'", paths)
	vv("cfg = '%#v'", cfg)
	//vv("cdc = '%#v'", cdc)

	for algo := 0; algo < 5; algo++ {
		cdc := jcdc.GetCutpointer(jcdc.CDCAlgo(algo), cfg)
		//cdc := jcdc.NewRabinKarpCDC(cfg)

		//vv("before SetConfig with cfg = '%#v'", cfg)
		//cdc.SetConfig(cfg)
		//vv("after SetConfig with cfg = '%#v'", cfg)
		//seeWindow := 0
		//seeBits := 0
		/*switch x := cdc.(type) {
		case *jcdc.FNVCDC:
			//x.NumBitsZeroAtCut = uint32(b)
			//vv("set bits to %v", b)
			//vv("x.NumBitsZeroAtCut = %v", x.NumBitsZeroAtCut)
			seeBits = int(x.NumBitsZeroAtCut)
			_ = seeBits
		case *jcdc.RabinKarpCDC:
			seeWindow = x.WindowSize
			vv("have RabinKarpCDC with seeWindows = '%v'", seeWindow) //16 now
		}
		*/
		//seeWindow = cdc.WindowSize
		/*			fmt.Printf(`
					algo = %v (%v)
						`, algo, cdc.Name())
		*/
		sums := make([]*sum, len(paths))
		for i, path := range paths {
			_ = i
			fi, err := os.Stat(path)
			panicOn(err)

			data, err := os.ReadFile(path)
			panicOn(err)

			sums[i] = getCuts(cdc.Name(), data, cdc, cfg)
			cuts, cmap := sums[i].cuts, sums[i].cmap
			ndup := 0
			savings := 0

			var cmap0 map[string]*seg
			if i > 0 {
				cmap0 = sums[0].cmap
			}
			//vv("len cuts = %v; len cmap = %v", len(cuts), len(cmap))
			sdt := StdDevTracker{}
			for k, v := range cmap {
				if i > 0 {
					data, ok := cmap0[k]
					if ok {
						ndup++
						savings += data.sz
					}
				}
				//if v.n > 1 {
				//	ndup++
				//	savings += v.sz
				//}
				sdt.AddObs(float64(v.sz), float64(v.n))
			}
			if i > 0 {

				min, targ := cfg.MinSize, cfg.TargetSize
				//	fmt.Printf(`min=%v; targ = %v; see_window=%v => mean = %v   sd = %v
				//`, formatUnder(min), formatUnder(targ), seeWindow, formatUnder(int(sdt.Mean())), formatUnder(int(sdt.SampleStdDev())))
				//	continue

				fmt.Printf(`
 i=%v ... path = '%v'   vs  path[0]='%v'   algo='%v'
   min = %v;  target = %v;   max = %v
    ncut = %v; ndup = %v; savings = %v bytes of %v (%0.2f %%)
      diffbytes = %v
      mean = %v   sd = %v
`, i, path, paths[0], cdc.Name(),
					min, targ, cfg.MaxSize,
					len(cuts), ndup, formatUnder(savings), formatUnder(int(fi.Size())), float64(100*savings)/float64(fi.Size()), formatUnder(int(fi.Size())-savings), formatUnder(int(sdt.Mean())), formatUnder(int(sdt.SampleStdDev())))
			}
		}
	}
}

type seg struct {
	n  int
	sz int
}

type sum struct {
	cuts []int
	cmap map[string]*seg
}

func getCuts(
	title string,
	data []byte,
	u jcdc.Cutpointer,
	opt *jcdc.CDC_Config,
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

func hashOfBytes(by []byte) string {
	h := sha256.New()
	h.Write(by)
	enchex := hex.EncodeToString(h.Sum(nil))
	return enchex
}

func formatUnder(n int) string {
	// Convert to string first
	str := strconv.FormatInt(int64(n), 10)

	// Handle numbers less than 1000
	if len(str) <= 3 {
		return str
	}

	// Work from right to left, adding underscores
	var result []byte
	for i := len(str) - 1; i >= 0; i-- {
		if (len(str)-1-i)%3 == 0 && i != len(str)-1 {
			result = append([]byte{'_'}, result...)
		}
		result = append([]byte{str[i]}, result...)
	}

	return string(result)
}
