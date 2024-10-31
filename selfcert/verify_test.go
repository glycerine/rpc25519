package selfcert

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test101_certificate_signing_check(t *testing.T) {

	cv.Convey("A certificate signed by a different CA should fail to verify.", t, func() {

		// cd to tmpdir as side effect
		origdir, tmpdir, err := MakeAndMoveToTempDir()
		_ = tmpdir
		panicOn(err)
		defer func() {
			os.Chdir(origdir)
			os.RemoveAll(tmpdir)
		}()
		verbose := true

		// make CA0
		encrypt := false
		pathCA0 := "ca0"
		caCertPath0 := pathCA0 + "/ca.crt"
		odirCerts0 := "certs0"
		caValidForDur := time.Hour

		caKey0, err := Step1_MakeCertificateAuthority(pathCA0, verbose, encrypt, caValidForDur)
		panicOn(err)

		// make client0 signed by CA0
		clientKey0, err := Step2_MakeEd25519PrivateKey("client0", pathCA0, verbose, encrypt)
		panicOn(err)
		Step3_MakeCertSigningRequest(clientKey0, "client0", "client0@email", odirCerts0)
		goodForDur := time.Hour
		Step4_MakeCertificate(caKey0, pathCA0, "client0", odirCerts0, goodForDur, verbose)
		verifyMeCertPath0 := odirCerts0 + "/client0.crt"

		// same check: client0 should verify with ca0
		err = Step7_VerifyCertIsSignedByCertificateAuthority(verifyMeCertPath0, caCertPath0, verbose)
		if err != nil {
			panic(fmt.Sprintf("Bad: expected no error back trying to verify '%v' with same CA '%v': '%v'", verifyMeCertPath0, caCertPath0, err))
		} else {
			fmt.Printf("Good: expected err == nil. '%v' did sign '%v'.\n", caCertPath0, verifyMeCertPath0)
		}

		// make CA1: a different CA that should not verify with above client cert
		pathCA1 := "ca1"
		caCertPath1 := pathCA1 + "/ca.crt"
		odirCerts1 := "certs1"
		caKey1, err := Step1_MakeCertificateAuthority(pathCA1, verbose, encrypt, caValidForDur)
		panicOn(err)
		_ = caKey1

		// make client1 signed by CA1
		clientKey1, err := Step2_MakeEd25519PrivateKey("client1", pathCA1, verbose, encrypt)
		panicOn(err)
		Step3_MakeCertSigningRequest(clientKey1, "client1", "client1@email", odirCerts1)

		Step4_MakeCertificate(caKey1, pathCA1, "client1", odirCerts1, goodForDur, verbose)
		verifyMeCertPath1 := odirCerts1 + "/client1.crt"

		// same check: client1 should verify with ca1
		err = Step7_VerifyCertIsSignedByCertificateAuthority(verifyMeCertPath1, caCertPath1, verbose)
		if err != nil {
			panic(fmt.Sprintf("Bad: expected no error back trying to verify '%v' with same CA '%v': '%v'", verifyMeCertPath1, caCertPath1, err))
		} else {
			fmt.Printf("Good: expected err == nil. '%v' did sign '%v'.\n", caCertPath1, verifyMeCertPath1)
		}

		// cross check client0 vs CA1
		err = Step7_VerifyCertIsSignedByCertificateAuthority(verifyMeCertPath0, caCertPath1, verbose)
		if err == nil {
			panic(fmt.Sprintf("expected an error back trying to verify '%v' with different CA '%v'", verifyMeCertPath0, caCertPath1))
		} else {
			fmt.Printf("Good: expected err != nil, and got: '%v'", err)
		}

		// cross check client1 vs CA0
		err = Step7_VerifyCertIsSignedByCertificateAuthority(verifyMeCertPath1, caCertPath0, verbose)
		if err == nil {
			panic(fmt.Sprintf("expected an error back trying to verify '%v' with different CA '%v'", verifyMeCertPath1, caCertPath0))
		} else {
			fmt.Printf("Good: expected err != nil, and got: '%v'", err)
		}

	})
}

func MakeAndMoveToTempDir() (origdir string, tmpdir string, err error) {

	origdir, err = os.Getwd()
	panicOn(err)
	tmpdir, err = ioutil.TempDir(origdir, "temptestdir")
	if err != nil {
		return
	}
	err = os.Chdir(tmpdir)
	if err != nil {
		return
	}

	return origdir, tmpdir, nil
}
