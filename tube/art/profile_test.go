package art

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	mathrand "math/rand"
	mathrand2 "math/rand/v2"
	"net/http"
	_ "net/http/pprof" // for web based profiling while running
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// profile to see were allocations are, write path.
func TestWriteForProfiling(t *testing.T) {
	return
	startOnlineWebProfiling()

	value := newValue(123)
	rng := mathrand.New(mathrand.NewSource(seed))
	var rkey [8]byte

	art := NewArtTree()

	for i := range 1_000_000_000 {
		_ = i
		rk := randomKey(rng, rkey[:])
		art.Insert(rk, value, "")
	}
}

// profile to see were allocations are, read path.
func TestReadForProfiling(t *testing.T) {
	return
	startOnlineWebProfiling()

	paths := loadTestFile("assets/linux.txt")
	tree := NewArtTree()

	var seed32 [32]byte
	chacha8 := mathrand2.NewChaCha8(seed32)

	for i, w := range paths {
		if tree.Insert(w, w, "") {
			t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
		}
	}

	for i := range 1_000_000_000 {
		_ = i
		j := int(chacha8.Uint64() % uint64(len(paths)))
		val, _, found, _ := tree.FindExact(paths[j])
		if !found {
			panic(fmt.Sprintf("key '%v' was not found", string(paths[j])))
		}
		v := val
		if bytes.Compare(v, paths[j]) != 0 {
			panic(fmt.Sprintf("key '%v' expected to give val (same) but wrongly found instead: '%v'", string(paths[j]), string(v)))

		}
	}
}

func startOnlineWebProfiling() (port int) {

	// To dump goroutine stack from a running program for debugging:
	// Start an HTTP listener if you do not have one already:
	// Then point a browser to http://127.0.0.1:9999/debug/pprof for a menu, or
	// curl http://127.0.0.1:9999/debug/pprof/goroutine?debug=2
	// for a full dump.
	//port = GetAvailPort()
	port = 9999
	go func() {
		err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%v", port), nil)
		if err != nil {
			panic(err)
		}
	}()
	fmt.Fprintf(os.Stderr, "\n for stack dump:\n\ncurl http://127.0.0.1:%v/debug/pprof/goroutine?debug=2\n\n for general debugging:\n\nhttp://127.0.0.1:%v/debug/pprof\n\n", port, port)
	return
}

func startProfilingCPU(path string) {
	// add randomness so two tests run at once don't overwrite each other.
	rnd8 := randomStringWithUp(8)
	fn := path + ".cpuprof." + rnd8
	f, err := os.Create(fn)
	stopOn(err)
	alwaysPrintf("will write cpu profile to '%v'", fn)
	go func() {
		pprof.StartCPUProfile(f)
		time.Sleep(time.Minute)
		pprof.StopCPUProfile()
		alwaysPrintf("stopped and wrote cpu profile to '%v'", fn)
	}()
}

func startProfilingMemory(path string, wait time.Duration) {
	// add randomness so two tests run at once don't overwrite each other.
	rnd8 := randomStringWithUp(8)
	fn := path + ".memprof." + rnd8
	if wait == 0 {
		wait = time.Minute // default
	}
	alwaysPrintf("will write mem profile to '%v'; after wait of '%v'", fn, wait)
	go func() {
		time.Sleep(wait)
		writeMemProfiles(fn)
	}()
}

func writeMemProfiles(fn string) {
	if !strings.HasSuffix(fn, ".") {
		fn += "."
	}
	h, err := os.Create(fn + "heap")
	panicOn(err)
	defer h.Close()
	a, err := os.Create(fn + "allocs")
	panicOn(err)
	defer a.Close()
	g, err := os.Create(fn + "goroutine")
	panicOn(err)
	defer g.Close()

	hp := pprof.Lookup("heap")
	ap := pprof.Lookup("allocs")
	gp := pprof.Lookup("goroutine")

	panicOn(hp.WriteTo(h, 1))
	panicOn(ap.WriteTo(a, 1))
	panicOn(gp.WriteTo(g, 2))
}

func TestWriteAndReadForProfiling(t *testing.T) {
	return
	startOnlineWebProfiling()

	value := newValue(123)
	rng := mathrand.New(mathrand.NewSource(seed))
	var rkey [8]byte

	tree := NewArtTree()
	readFrac := float32(7) / 10.0

	for i := range 1_000_000_000 {
		_ = i
		rk := randomKey(rng, rkey[:])
		if rng.Float32() < readFrac {
			tree.FindExact(rk)
		} else {
			tree.Insert(rk, value, "")
		}
	}
}

var chu = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomStringWithUp(n int) string {
	s := make([]byte, n)
	m := int64(len(chu))
	for i := 0; i < n; i++ {
		r := cryptoRandInt64()
		if r < 0 {
			r = -r
		}
		k := r % m
		a := chu[k]
		s[i] = a
	}
	return string(s)
}

// Use crypto/rand to get an random int64.
func cryptoRandInt64() int64 {
	b := make([]byte, 8)
	_, err := cryptorand.Read(b)
	if err != nil {
		panic(err)
	}
	r := int64(binary.LittleEndian.Uint64(b))
	return r
}
