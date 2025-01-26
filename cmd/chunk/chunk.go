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

	vv("cfg = '%#v'", cfg)
	cdc, _ := jsync.GetCutpointer(jsync.CDCAlgo(algo))
	cdc.SetConfig(cfg)
	vv("cdc = '%#v'", cdc)

	for i, path := range paths {
		_ = i
		fi, err := os.Stat(path)
		panicOn(err)

		data, err := os.ReadFile(path)
		panicOn(err)

		cuts, cmap := getCuts(cdc.Name(), data, cdc, cfg)
		ndup := 0
		savings := 0
		vv("len cuts = %v; len cmap = %v", len(cuts), len(cmap))
		sdt := StdDevTracker{}
		for _, v := range cmap {
			if v.n > 1 {
				ndup++
				savings += v.sz
			}
			sdt.AddObs(float64(v.sz), float64(v.n))
		}

		fmt.Printf(`
algo = %v (%v)
 i=%v ... path = '%v'
   min = %v;  target = %v;   max = %v
    ncut = %v; ndup = %v; savings = %v bytes of %v (%0.2f %%)
      mean=%0.2f; sd=%0.2f
`, algo, cdc.Name(), i, path,
			cfg.MinSize, cfg.TargetSize, cfg.MaxSize,
			len(cuts), ndup, formatUnder(savings), formatUnder(int(fi.Size())), float64(savings)/float64(fi.Size()), sdt.Mean(), sdt.SampleStdDev())
	}
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
	return
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
