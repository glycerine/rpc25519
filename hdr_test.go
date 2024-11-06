package rpc25519

import (
	"testing"

	cv "github.com/glycerine/goconvey/convey"
)

func Test010_HDR_generation(t *testing.T) {

	cv.Convey("A HDR should look nice in both String() and JSON() output", t, func() {
		from := "client"
		to := "server"
		subject := "myRPC_call_name()"
		//isRPC := true
		//isLeg2 := false

		// call:
		hdr := NewHDR(from, to, subject, CallRPC)

		shdr := hdr.String()
		jhdr := hdr.JSON()

		vv("shdr = '%v'", shdr)
		vv("jhdr = '%v'", string(jhdr))

		// response:
		hdr2 := NewHDR(to, from, subject, CallRPC)
		shdr2 := hdr2.String()
		jhdr2 := hdr2.JSON()

		vv("shdr2 = '%v'", shdr2)
		vv("jhdr2 = '%v'", string(jhdr2))

		friendly := hdr2.OpaqueURLFriendly()

		vv("friendly = '%v'", friendly)

		hdr2back, err := HDRFromOpaqueURLFriendly(friendly)
		panicOn(err)

		//vv("hdr2back = '%v'", hdr2back)
		//vv("hdr2 = '%v'", hdr2)
		cv.So(hdr2back.Equal(hdr2), cv.ShouldBeTrue)

		vv("back from friendly: '%v'", hdr2back.String())
		vv("pretty: '%v'", hdr2back.Pretty())

		// json serz
		by1 := hdr.Bytes()
		un1 := Unbytes(by1)
		cv.So(un1.Equal(hdr), cv.ShouldBeTrue)

		// greenpack serz
		w := newWorkspace("hdr_test", 4096) // max possible message len, to pre-allocate memory.
		green, err := hdr.AsGreenpack(w.buf)
		panicOn(err)
		hdr3, err := HDRFromGreenpack(green)
		panicOn(err)
		cv.So(hdr3.Equal(hdr), cv.ShouldBeTrue)

	})
}
