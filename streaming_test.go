package rpc25519

import (
	//"context"
	//"fmt"
	//"os"
	//"strings"
	"testing"
	//"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test300_streaming_test_of_upload(t *testing.T) {

	cv.Convey("before we add compression, we want to take the cli -sendfile, cli -echofile, and cli -download commands and turn them into test(s) that verify quickly that they are all still working. The srv -readfile, srv -echo, and srv -serve are the corresponding server side operations. Test300 is for upload of a file (multiple parts bigger than our max Message size.", t, func() {
	})
}

func Test301_streaming_test_of_download(t *testing.T) {

	cv.Convey("before we add compression, we want to take the cli -sendfile, cli -echofile, and cli -download commands and turn them into test(s) that verify quickly that they are all still working. The srv -readfile, srv -echo, and srv -serve are the corresponding server side operations. Test301 is for download of a file (multiple parts)", t, func() {
	})
}

func Test302_streaming_test_of_bistream(t *testing.T) {

	cv.Convey("before we add compression, we want to take the cli -sendfile, cli -echofile, and cli -download commands and turn them into test(s) that verify quickly that they are all still working. The srv -readfile, srv -echo, and srv -serve are the corresponding server side operations. Test302 is for bistreaming (simultaneous upload and download of files bigger than max Message size)", t, func() {

	})
}
