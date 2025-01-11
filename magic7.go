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
// 06 => zstd:01 (fastest time, least compression for zstd)
var magic = [8]byte{0xb3, 0x5d, 0x18, 0x39, 0xac, 0x8e, 0x1d, 0x00}

var ErrMagicWrong = fmt.Errorf("error: magic bytes not found at start of message")

// which compression is to be/was used.
func mustDecodeMagic7(magic7 magic7b) (magicCompressAlgo string) {
	var err error
	magicCompressAlgo, err = decodeMagic7(magic7)
	panicOn(err)
	return
}

type magic7b byte

const (
	magic7b_none   magic7b = 0 // no compression
	magic7b_s2     magic7b = 1
	magic7b_lz4    magic7b = 2
	magic7b_zstd01 magic7b = 3
	magic7b_zstd03 magic7b = 4
	magic7b_zstd07 magic7b = 5
	magic7b_zstd11 magic7b = 6

	// per Message flag in HDR to have no compression.
	// It is distinct from 0 so we can not cache it
	// and not have the server match it.
	magic7b_no_system_compression magic7b = 7

	// keep this as the last number, just above all
	// the rest, if you add more legit magic7b values above.
	magic7b_out_of_bounds magic7b = 8
)

func (m magic7b) String() (s string) {
	s, _ = decodeMagic7(m)
	return
}

func decodeMagic7(magic7 magic7b) (magicCompressAlgo string, err error) {
	switch magic7 {
	// magic[7] (the last byte 0x00 here) can vary,
	// it indicates the compression in use:
	case magic7b_none:
		// no compression
		return "", nil
	case magic7b_s2:
		return "s2", nil
	case magic7b_lz4:
		return "lz4", nil
	case magic7b_zstd01:
		return "zstd:01", nil
	case magic7b_zstd03:
		return "zstd:03", nil
	case magic7b_zstd07:
		return "zstd:07", nil
	case magic7b_zstd11:
		return "zstd:11", nil
	case magic7b_no_system_compression:
		// separate from 0 so we can not cache it and not have the server match it.
		return "no-system-compression", nil // per Message flag in HDR.
	}
	return "", fmt.Errorf("unrecognized magic7: '%v' ; valid choices: s2, lz4, zstd:01, zstd:03, zstd:07, zstd:11", magic7)
}

func encodeMagic7(magicCompressAlgo string) (magic7 magic7b, err error) {
	switch magicCompressAlgo {
	case "":
		// no compression
		return magic7b_none, nil
	case "s2":
		return magic7b_s2, nil
	case "lz4":
		return magic7b_lz4, nil
	case "zstd:01":
		return magic7b_zstd01, nil
	case "zstd:03":
		return magic7b_zstd03, nil
	case "zstd:07":
		return magic7b_zstd07, nil
	case "zstd:11":
		return magic7b_zstd11, nil

	case "no-system-compression":

		// Per Message flag in HDR.
		// This is separate from 0 so we can recognize it
		// not match it in replies; so it doesn't mess
		// up the compression on the rest of the stream.
		return magic7b_no_system_compression, nil
	}
	return 0, fmt.Errorf("unrecognized magicCompressAlgo: '%v' ; "+
		"valid choices: s2, lz4, zstd:01, zstd:03, zstd:07, zstd:11",
		magicCompressAlgo)
}

func setMagicCheckWord(pressAlgo string, magicCheck []byte) magic7b {
	// write the compression algo in use to the magic[7]
	// of the first 8 magic message bytes.

	magic7, err := encodeMagic7(pressAlgo)
	panicOn(err)
	copy(magicCheck, magic[:])
	magicCheck[7] = byte(magic7) // set the compression used.
	return magic7
}
