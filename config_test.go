package rpc25519

import (
	"fmt"
	"os"
	"testing"
)

var _ = fmt.Printf

func Test000_config_directories_from_env_var(t *testing.T) {

	// since it changes the XDG_CONFIG_HOME,
	// this messes with TestMain which
	// sets it to isolate the test certs, etc.
	return

	tmp := "tmp-000-test-dir"
	defer os.RemoveAll(tmp)

	os.Setenv("XDG_CONFIG_HOME", tmp)
	fmt.Printf("GetPrivateCertificateAuthDir() = '%v'\n",
		GetPrivateCertificateAuthDir())
	fmt.Printf("GetCertsDir() = '%v'\n", GetCertsDir())

	if !dirExists(tmp + "/.config/rpc25519/certs") {
		panic("certs dir not made!")
	}
	if !dirExists(tmp + "/.config/rpc25519/my-keep-private-dir") {
		panic("CA dir not made!")
	}
}
