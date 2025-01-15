package rpc25519

import (
	"os"
)

// Store config files in standard locations. Per
// https://unix.stackexchange.com/questions/312988/understanding-home-configuration-file-locations-config-and-local-sha
//
// $HOME/.config is where per-user configuration
// files go if there is no $XDG_CONFIG_HOME

// GetCertsDir tells us where to generate/look
// for certificates and key pairs,
// including the a nodes private keys.
// It also creates the directory if it
// does not exist, and panics if it cannot.
//
// Use $HOME/.config/rpc25519/certs to store keys now,
// so we can find them in one location.
// (Actually use $XDG_CONFIG_HOME/rpc25519/certs
// if XDG_CONFIG_HOME is set, but that is less
// common).
//
// If we cannot find either of those, we
// use the current working directory.
//
// We will panic if we cannot make this
// essential directory.
func GetCertsDir() (path string) {
	defer os.MkdirAll(path, 0700)
	dir := os.Getenv("XDG_CONFIG_HOME")
	home := os.Getenv("HOME")
	base := "certs"
	suffix := sep + ".config" + sep + "rpc25519" + sep + base
	switch {
	case dir != "":
		path = dir + suffix
	case home != "":
		path = home + suffix
	default:
		path = base
	}
	err := os.MkdirAll(path, 0700)
	panicOn(err)
	//vv("no error back doing MkdirAll path = '%v'", path)
	return path
}

// GetPrivateCertificateAuthDir says where
// to store the CA master private key,
// which should typically not be
// distributed with the working node key-pairs.
// It also creates the directory if it
// does not exist, and panics if it cannot.
//
// Use $HOME/.config/rpc25519/certs to store keys now,
// so we can find them in one location.
// (Actually use
// $XDG_CONFIG_HOME/rpc25519/certs/my-keep-private-dir
// if XDG_CONFIG_HOME is set, but that is less common).
//
// If we cannot find either of those, we
// use the current working directory.
//
// We will panic if we cannot make this
// essential directory.
func GetPrivateCertificateAuthDir() (path string) {

	dir := os.Getenv("XDG_CONFIG_HOME")
	home := os.Getenv("HOME")
	base := "my-keep-private-dir"
	suffix := sep + ".config" + sep + "rpc25519" + sep + base
	switch {
	case dir != "":
		path = dir + suffix
	case home != "":
		path = home + suffix
	default:
		path = base
	}
	err := os.MkdirAll(path, 0700)
	panicOn(err)
	//vv("no error back doing MkdirAll path = '%v'", path)
	return path
}
