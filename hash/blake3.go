package hash

import (
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

// UnlockedDigest512 is not goroutine safe; use
// the Locked version if this is needed.
// The output digest is 64 bytes (512 bits), and can
// be truncated to get smaller hashes.
func (b *Blake3) UnlockedDigest512(by []byte) (digest []byte) {
	b.hasher.Reset()
	b.hasher.Write(by)
	return b.hasher.Sum(nil)
}

func (b *Blake3) UnlockedDigest256(by []byte) (digest []byte) {
	b.hasher.Reset()
	b.hasher.Write(by)
	digest = b.hasher.Sum(nil)
	return digest[:32]
}

func (b *Blake3) LockedDigest256(by []byte) (digest []byte) {
	b.mut.Lock()
	b.hasher.Reset()
	b.hasher.Write(by)
	digest = b.hasher.Sum(nil)
	b.mut.Unlock()
	return digest[:32]
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
	sum := b.LockedDigest256(by)
	return "blake3.32B-" + cristalbase64.URLEncoding.EncodeToString(sum[:])
}
