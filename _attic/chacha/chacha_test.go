package main

/*
import (
	"bytes"
	"io"
	//"net"
	//"fmt"
	"testing"

	"github.com/acomagu/bufpipe"
	cv "github.com/glycerine/goconvey/convey"
)

func TestReadEncryptedStream(t *testing.T) {

	// client plaintext ->  encode  -> ciphertext on network -> decode  -> server plaintext
	// client plaintext <-  decode  <- ciphertext on network <- encode  <- server plaintext

	// so      write to an encoder takes in plaintext.  encoder.Write(plain) (encryption)
	//        read from an encoder produces ciphertext. encoder.Read() gets the encrypted text.
	//
	//         write to a decoder takes in ciphertext.  decoder.Write(cipher) (decryption)
	//        read from a decoder produces plaintext.   decoder.Read() gets the plaintext out.

	cv.Convey("layer2.Encoder.Write(plain) followed by a Read(), the read should get ciphertext.", t, func() {
		key := NewXChaCha20CryptoRandKey()
		r, w := bufpipe.New(nil)
		enc, dec := NewEncoderDecoderPair(key, r)

		orig := []byte("this is my view, gophers, gophers, everywhere!")
		nw, err := enc.Write(orig)
		panicOn(err)
		if nw != len(orig) {
			panic("short write")
		}
		vv("wrote to enc this orig plaintext: '%v'", orig)

//			ciph := bytes.NewBuffer(nil)
//			nr, err := io.Copy(ciph, enc)
//			panicOn(err)
//			_ = nr

		vv("cipher = '%x'", ciph.Bytes())

		cb := ciph.Bytes()
		nw2, err := dec.Write(cb)
		panicOn(err)
		if nw2 != len(cb) {
			panic("short write")
		}

		plain := bytes.NewBuffer(nil)
		nr2, err := io.Copy(plain, dec)
		panicOn(err)
		if int(nr2) != len(orig) {
			panic("different length after decode")
		}
		vv("plain = '%v'", string(plain.Bytes()))

		cv.So(string(plain.Bytes()), cv.ShouldEqual, string(orig))

	})
}
*/
