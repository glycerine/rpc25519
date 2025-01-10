package rpc25519

import (
	"fmt"
)

// magic is always the first 8 bytes of the plaintext
// unencrypted message on the wire. Thus
// magic lets us detect when message boundaries
// have been corrupted (also encrypted vs not mismatches).
// It was chosen randomly and should remain
// constant, or else clients and servers will not
// be able to talk to each other.
// magic[7] (the last byte 0x00 here) can vary,
// it indicates the compression in use:
// 00 => no compression
// 01 => s2
// 02 => lz4
// 03 => zstd:11 (best compression)
// 04 => zstd:07
// 05 => zstd:03
// 06 => zstd:01 (fastest)
var magic = [8]byte{0xb3, 0x5d, 0x18, 0x39, 0xac, 0x8e, 0x1d, 0x00}

var ErrMagicWrong = fmt.Errorf("error: magic bytes not found at start of message")

// which compression is to be/was used.
func mustDecodeMagic7(magic7 byte) (magicCompressAlgo string) {
	var err error
	magicCompressAlgo, err = decodeMagic7(magic7)
	panicOn(err)
	return
}

func decodeMagic7(magic7 byte) (magicCompressAlgo string, err error) {
	switch magic7 {
	// magic[7] (the last byte 0x00 here) can vary,
	// it indicates the compression in use:
	case 0:
		// no compression
		return "", nil
	case 1:
		return "s2", nil
	case 2:
		return "lz4", nil
	case 3:
		return "zstd:11", nil
	case 4:
		return "zstd:07", nil
	case 5:
		return "zstd:03", nil
	case 6:
		return "zstd:01", nil
	}
	return "", fmt.Errorf("unrecognized magic7: '%v'", magic7)
}

func encodeMagic7(magicCompressAlgo string) (magic7 byte, err error) {
	switch magicCompressAlgo {
	case "":
		// no compression
		return 0, nil
	case "s2":
		return 1, nil
	case "lz4":
		return 2, nil
	case "zstd:11":
		return 3, nil
	case "zstd:07":
		return 4, nil
	case "zstd:03":
		return 5, nil
	case "zstd:01":
		return 6, nil
	}
	return 0, fmt.Errorf("unrecognized magic7 "+
		"compress algo string '%v'", magicCompressAlgo)
}

func setMagicCheckWord(pressAlgo string, magicCheck []byte) byte {
	// write the compression algo in use to the magic[7]
	// of the first 8 magic message bytes.

	magic7, err := encodeMagic7(pressAlgo)
	panicOn(err)
	copy(magicCheck, magic[:])
	magicCheck[7] = magic7 // set the compression used.
	return magic7
}
