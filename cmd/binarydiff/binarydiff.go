package main

import (
	"fmt"
	"os"

	// check-summing utilities.
	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/rpc25519/jcdc"
	"lukechampine.com/blake3"
)

// cut a file, hash all the segments, print them
// out in order, so we can do a sane diff on a
// binary file.
func main() {
	path := os.Args[1]
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	cfg := jcdc.Default_UltraCDC_Options()
	u := jcdc.NewUltraCDC(cfg)

	cuts := u.Cutpoints(data, 0)

	prev := 0
	for i, c := range cuts {
		fmt.Printf("%v\n", blake3OfBytesString(data[prev:cuts[i]]))
		prev = c
	}
}

func blake3OfBytes(by []byte) []byte {
	h := blake3.New(64, nil)
	h.Write(by)
	return h.Sum(nil)
}

func blake3OfBytesString(by []byte) string {
	sum := blake3OfBytes(by)
	return "blake3.32B-" + cristalbase64.URLEncoding.EncodeToString(sum[:32])
}
