package rpc25519

import (
	"fmt"
	"testing"

	cv "github.com/glycerine/goconvey/convey"
)

func Test020_nonce_sequence_not_reused(t *testing.T) {

	cv.Convey("the blabber encoder nonces should be different after each use, "+
		"so a nonce is never re-used, esp between client and server", t, func() {
		var key [32]byte

		bcli := newBlabber("test", key, nil, true, 1024, false)
		bsrv := newBlabber("test", key, nil, true, 1024, true)
		n := 1100
		m := make(map[[12]byte]int)

		var last [12]byte
		if len(last) != bcli.enc.noncesize {
			panic("need to update this test, nonce size is no longer 12")
		}
		var i int
		add := func(by []byte, offset int) {
			copy(last[:], by)
			prev, dup := m[last]
			if dup {
				panic(fmt.Sprintf("saw duplicated nonce '%x' at i=%v, "+
					"must never happen. prev seen at %v", last[:], i, prev))
			}
			m[last] = i + offset
		}

		add(bcli.enc.writeNonce, 0)
		//vv("first = '%x'", last[:])
		add(bsrv.enc.writeNonce, 1e6)
		//vv("second = '%x'", last[:])
		for i = 0; i < n; i++ {
			bcli.enc.moveToNextNonce()
			bsrv.enc.moveToNextNonce()

			add(bcli.enc.writeNonce, 0)

			//vv("last[%v] = '%x'", i, last[:])
			add(bsrv.enc.writeNonce, 1e6)

			//vv("last[%v] = '%x'", i, last[:])
		}
		vv("checked up to %v-1 for collision in the first (cli+server) %v values.", n, len(m))
		cv.So(true, cv.ShouldBeTrue)
	})
}
