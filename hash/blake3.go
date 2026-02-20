package hash

import (
	"fmt"
	"io"
	"os"
	"sync"

	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
)

const fRFC3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

// Blake3 provides Hash32 which is goroutine safe.
type Blake3 struct {
	mut        sync.Mutex
	hasher     *blake3.Hasher // not used if Locked; see below.
	readOffset int64
}

// NewBlake3 creates a new Blake3.
func NewBlake3() *Blake3 {
	return &Blake3{
		hasher: blake3.New(64, nil),
	}
}

func NewBlake3WithKey(key [32]byte) *Blake3 {
	return &Blake3{
		hasher: blake3.New(64, key[:]),
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

// read psuedo random bytes from b.hasher.XOF().
func (b *Blake3) ReadXOF(p []byte) (n int, err error) {
	b.mut.Lock()
	defer b.mut.Unlock()
	r := b.hasher.XOF()

	nr := int64(len(p))
	r.Seek(b.readOffset, io.SeekStart)
	b.readOffset += nr

	n, err = r.Read(p)
	if n != len(p) {
		panic("short read???")
	}
	return
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

func Blake3OfFileWithModtime(path string, includeModTime bool) (blake3sum string, err error) {

	sum, h, err := blake3.HashFile(path)
	if err != nil {
		return "", err
	}
	if includeModTime {
		fi, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		// put into a canonical format.
		s := fmt.Sprintf("%v", fi.ModTime().UTC().Format(fRFC3339NanoNumericTZ0pad))
		h.Write([]byte(s))
		sum = h.Sum(nil)
	}

	blake3sum = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
	return
}

func Blake3OfFile(path string) (blake3sum string, err error) {

	sum, _, err1 := blake3.HashFile(path)
	if err1 != nil {
		return "", err1
	}

	blake3sum = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
	return
}

func SumToString(h *blake3.Hasher) string {
	by := h.Sum(nil)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33])
}

// if you already have the Hasher.Sum() output:
func RawSumBytesToString(by []byte) string {
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33])
}
