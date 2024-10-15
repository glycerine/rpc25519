package rpc25519

import (
	"testing"

	cv "github.com/glycerine/goconvey/convey"
)

func Test010_MID_generation(t *testing.T) {

	cv.Convey("A MID should look nice in both String() and JSON() output", t, func() {
		from := "client"
		to := "server"
		subject := "myRPC_call_name()"
		isRPC := true
		//isLeg2 := false

		// call:
		mid := NewHDR(from, to, subject, isRPC, false)

		smid := mid.String()
		jmid := mid.JSON()

		vv("smid = '%v'", smid)
		vv("jmid = '%v'", string(jmid))

		// response:
		mid2 := NewHDR(to, from, subject, isRPC, true)
		smid2 := mid2.String()
		jmid2 := mid2.JSON()

		vv("smid2 = '%v'", smid2)
		vv("jmid2 = '%v'", string(jmid2))

		friendly := mid2.OpaqueURLFriendly()

		vv("friendly = '%v'", friendly)

		mid2back, err := HDRFromOpaqueURLFriendly(friendly)
		panicOn(err)

		cv.So(mid2back.Equal(mid2), cv.ShouldBeTrue)

		vv("back from friendly: '%v'", mid2back.String())
		vv("pretty: '%v'", mid2back.Pretty())

		// json serz
		by1 := mid.Bytes()
		un1 := Unbytes(by1)
		cv.So(un1.Equal(mid), cv.ShouldBeTrue)

		// greenpack serz
		w := newWorkspace()
		green, err := mid.AsGreenpack(w.buf)
		panicOn(err)
		mid3, err := HDRFromGreenpack(green)
		panicOn(err)
		cv.So(mid3.Equal(mid), cv.ShouldBeTrue)

	})
}
