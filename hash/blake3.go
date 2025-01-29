package hash

import (
	"io"
	"os"
	"sync"

	cristalbase64 "github.com/cristalhq/base64"
	"lukechampine.com/blake3"
)

// Blake3 provides Hash32 which is goroutine safe.
type Blake3 struct {
	mut    sync.Mutex
	hasher *blake3.Hasher // not used if Locked; see below.
}

// NewBlake3 creates a new Blake3.
func NewBlake3() *Blake3 {
	return &Blake3{
		hasher: blake3.New(64, nil),
	}
}

func (b *Blake3) Write(by []byte) {
	b.mut.Lock()
	b.hasher.Write(by)
	b.mut.Unlock()
}

func (b *Blake3) Reset() {
	b.mut.Lock()
	b.hasher.Reset()
	b.mut.Unlock()
}

func (b *Blake3) SumString() string {
	b.mut.Lock()
	sum := b.hasher.Sum(nil)
	b.mut.Unlock()
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
}

// UnlockedDigest512 is not goroutine safe; use
// the Locked version if this is needed.
// The output digest is 64 bytes (512 bits), and can
// be truncated to get smaller hashes.
func (b *Blake3) UnlockedDigest512(by []byte) (digest []byte) {
	b.hasher.Reset()
	b.hasher.Write(by)
	return b.hasher.Sum(nil)
}

func (b *Blake3) UnlockedDigest264(by []byte) (digest []byte) {
	b.hasher.Reset()
	b.hasher.Write(by)
	digest = b.hasher.Sum(nil)
	return digest[:33]
}

func (b *Blake3) LockedDigest264(by []byte) (digest []byte) {
	b.mut.Lock()
	b.hasher.Reset()
	b.hasher.Write(by)
	digest = b.hasher.Sum(nil)
	b.mut.Unlock()
	return digest[:33]
}

func (b *Blake3) LockedDigest512(by []byte) (digest []byte) {
	b.mut.Lock()
	b.hasher.Reset()
	b.hasher.Write(by)
	digest = b.hasher.Sum(nil)
	b.mut.Unlock()
	return digest
}

func (b *Blake3) Hash32(by []byte) string {
	sum := b.LockedDigest264(by)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:])
}

// Blake3OfBytes is goroutine safe and lock free, since
// it creates a new hasher every time. Benchmarks
// (see blake3_test.go herein) suggest all these
// methods are about the same speed.
func Blake3OfBytes(by []byte) []byte {
	h := blake3.New(64, nil)
	h.Write(by)
	return h.Sum(nil)
}

// Blake3OfBytesString calls Blake3OfBytes and
// is goroutine safe and lock free, since
// it creates a new hasher every time. Benchmarks
// (see blake3_test.go herein) suggest all these
// methods are about the same speed.
// The returned string starts with
// the "blake3.33B-" prefix.
func Blake3OfBytesString(by []byte) string {
	sum := Blake3OfBytes(by)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
}

func Blake3OfFile(path string) (blake3sum string, err error) {
	fd, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fd.Close()
	h := blake3.New(64, nil)
	io.Copy(h, fd)
	by := h.Sum(nil)

	blake3sum = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33])
	return
}

func SumToString(h *blake3.Hasher) string {
	by := h.Sum(nil)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33])
}
