package rpc25519

import (
	"fmt"
	"os"
	"strings"
)

func init() {
	HostCID = createHostCID_onDisk()
	hostname, err := os.Hostname()
	panicOn(err)
	Hostname = HostCID + ":" + hostname
	//fmt.Printf("Hostname is '%v'\n", Hostname)
	//fmt.Printf("HostCID  is '%v'\n", HostCID)
}

var Hostname string // in memory cache. hostCID : local machines hostname.
var HostCID string  // stable identifier, even if machine name changes.

func createHostCID_onDisk() string {
	// ensure there is a host identifier.
	path := GetHostCIDpath()
	if fileExists(path) {
		sz, err := fileSize(path)
		panicOn(err)
		if sz != 37 {
			panic(fmt.Sprintf("unexpected size for '%v'", path))
		}
		by, err := os.ReadFile(path)
		panicOn(err)
		return strings.TrimSpace(string(by))
	}
	// not found, make anew.
	cid := NewCryRandCallID()
	cid = "hostCID-" + cid + "\n"
	fd, err := os.Create(path)
	panicOn(err)
	_, err = fd.WriteString(cid)
	panicOn(err)
	return cid
}

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

// GetConfigDir returns the config dir
// where we look for our .host.cid file
// to uniquely identify the host, so
// that we don't accidentally overwrite
// a file with itself.
func GetConfigDir() (path string) {
	dir := os.Getenv("XDG_CONFIG_HOME")
	home := os.Getenv("HOME")
	suffix := sep + ".config" + sep + "rpc25519"
	switch {
	case dir != "":
		path = dir + suffix
	case home != "":
		path = home + suffix
	default:
		return "." // use cwd
	}
	err := os.MkdirAll(path, 0700)
	panicOn(err)
	return
}

// GetHostCIDpath returns the path to the host.cid file.
func GetHostCIDpath() string {
	return GetConfigDir() + sep + "host.cid"
}

// GetServerDataDir tells the Server where to store its
// data files. We prefer the RPC25519_SERVER_DATA_DIR
// environment variable, if set, for this directory.
//
// Quoting from the XDG Base Directory Specification:
//
// "There is a single base directory relative to
// which user-specific data files should be written.
// This directory is defined by the environment
// variable $XDG_DATA_HOME"
//
// "$XDG_DATA_HOME defines the base directory relative to which
// user-specific data files should be stored. If $XDG_DATA_HOME
// is either not set or empty, a default equal
// to $HOME/.local/share should be used."
// -- https://specifications.freedesktop.org/basedir-spec/latest/
// (as of 2025 January 22).
//
// So we return, in order of preference:
//
//	$RPC25519_SERVER_DATA_DIR
//	$XDG_DATA_HOME/rpc25519
//	$HOME/.local/share/rpc25519
//
// If none of these is available, we will report an error
// to let the user know how to set the server's data directory.
func GetServerDataDir() (path string, err error) {
	app := os.Getenv("RPC25519_SERVER_DATA_DIR")
	dir := os.Getenv("XDG_DATA_HOME")
	home := os.Getenv("HOME")
	suffix := sep + "rpc25519"
	switch {
	case app != "":
		path = app
	case dir != "":
		path = dir + suffix
	case home != "":
		path = home + sep + ".local" + sep + "share" + suffix
	default:
		return "", fmt.Errorf("rpc25519.GetDataDir() error: " +
			"could not determine server data directory. " +
			"We must have one of RPC25519_SERVER_DATA_DIR, XDG_DATA_HOME," +
			" or HOME environment variable set (in that order of preference).")
	}
	err = os.MkdirAll(path, 0700)
	return
}
