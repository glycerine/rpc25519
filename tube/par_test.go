package tube

import (
	"fmt"
	"math"
	"testing"

	"github.com/glycerine/blake3"
	rpc "github.com/glycerine/rpc25519"
)

func TestParRecord(t *testing.T) {

	// make sure our ParRecord always fits in <= maxParRecord bytes.

	finPRbytes := make([]byte, maxParRecord)

	entry := &RaftLogEntry{}
	b, err := entry.MarshalMsg(nil)
	panicOn(err)
	_ = b

	for i := range 2 {
		checkEach := blake3.New(64, nil)

		var e *ParRecord
		switch i {
		case 0:
			e = &ParRecord{
				Offset:    math.MinInt64,
				Len0:      math.MinInt64,
				Len2:      math.MinInt64,
				Index:     math.MinInt64,
				Term:      math.MinInt64,
				Epoch:     math.MinInt64,
				ClusterID: rpc.NewCallID(""),
				TicketID:  rpc.NewCallID(""),
			}
		case 1:
			e = &ParRecord{
				Offset:    math.MaxInt64,
				Len0:      math.MaxInt64,
				Len2:      math.MaxInt64,
				Index:     math.MaxInt64,
				Term:      math.MaxInt64,
				Epoch:     math.MaxInt32,
				ClusterID: rpc.NewCallID(""),
				TicketID:  rpc.NewCallID(""),
			}
		}

		by, err := e.MarshalMsg(nil)
		panicOn(err)

		// really the hash will be of the RaftLogEntry bytes we protect.
		// but anything will do for checking size of final bytes.
		checkEach.Write(by)
		e.RLEblake3 = blake3ToString33B(checkEach)

		pre, err := e.MarshalMsg(nil)
		panicOn(err)

		bs := ByteSlice(pre)
		took, err := bs.MarshalMsg(finPRbytes[:0])
		panicOn(err)

		if len(took) > maxParRecord-8 { // -8 for the 8 bytes of crc64.ECMA
			panic(fmt.Sprintf("took %v must be <= maxParRecord(%v)", len(took), maxParRecord)) // panic: took 327 must be <= maxParRecord(320)
		}
		//vv("took len %v", len(took)) // took len 274 + 8 = 280 < 320. good.
		//vv("e = '%v'", e.Gstring())
		maxsz := e.Msgsize()
		vv("e.Msgsize = '%v', and maxsize+8 < maxParRecord(%v) == %v", maxsz, maxParRecord, maxsz+8 < maxParRecord) // 282 + 8 = 290 < 320. good.
		if maxsz > maxParRecord-8 {
			panic(fmt.Sprintf("maxsz+8= %v must be <= maxParRecord(%v)", maxsz+8, maxParRecord))
		}
	}
}
