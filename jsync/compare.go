package jsync

// mostly for tests. utility func to
// diff/compare files and directories
// for expected structure.

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var sep = string(os.PathSeparator)

// err indicates filesystem operation error, not difference.
func compareDirs(expected string, observed string) (diff string, err error) {
	obs, err := listFilesUnderDir(observed, false, "")
	panicOn(err)
	//vv("obs = '%#v'", obs)
	exp, err := listFilesUnderDir(expected, false, "")
	panicOn(err)

	//vv("observed:")
	omap := make(map[string]bool)
	emap := make(map[string]bool)
	for _, o := range obs {
		omap[o] = true
		//fmt.Println(o)
	}
	//vv("expected:")
	for _, e := range exp {
		emap[e] = true
		//fmt.Println(e)
	}
	// compare trees on the surface
	if stringSlicesDiffer(exp, obs) {
		return fmt.Sprintf("compareDirs difference: expected='%v' (%v files); observed='%v' (%v files) had different surface tree structure. e-o='%v'; o-e='%v'", expected, len(exp), observed, len(obs), stringSliceSub(exp, obs, expected+sep), stringSliceSub(obs, exp, observed+sep)), nil
	}

	// check contents
	for i := range exp {
		e := expected + sep + exp[i]
		o := observed + sep + obs[i]
		//vv("e = '%v'", e)
		//vv("o = '%v'", o)
		n, diff, err := diffDontStop(e, o)
		if err != nil || n > 0 {
			return fmt.Sprintf("compareDirs difference in content between expected file '%v' and observed file '%v': '%v' (/usr/bin/diff err='%v')", e, o, string(diff), err), nil
		}
	}
	return
}

// if includeRoot, return the full path, otherwise reltaive to root.
// if requriedSuffix supplied, files must end in that.
func listFilesUnderDir(root string, includeRoot bool, requiredSuffix string) (files []string, err error) {
	if !dirExists(root) {
		return
	}
	root = filepath.Clean(root) + sep // just 1 trailing slash
	n := len(root)
	if includeRoot {
		n = 0
	}
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if len(path) < n {
			//vv("short path='%v'; len=%v", path, len(path))
		} else {
			if info == nil {
				vv("info was nil for path = '%v'", path)
			}
			if info.IsDir() {
				// skip directories.
			} else {
				if requiredSuffix == "" || strings.HasSuffix(path, requiredSuffix) {
					files = append(files, path[n:])
				}
			}
		}
		return nil
	})
	return
}

func compareFilesDiffLen(expected string, observed string) (diffLineCount int) {
	diffLineCount, _ = compareFiles(expected, observed)
	return
}

// returns length of diff
func compareFiles(expected string, observed string) (int, []byte) {
	cmd := exec.Command("diff", "-b", observed, expected)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		if false { // don't stop now.
			stop(fmt.Sprintf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err))
		}
		fmt.Printf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err)
		return -1, nil // unknown, but not 0
	}

	N := len(out.Bytes())
	if N != 0 {
		// too verbose.
		//fmt.Printf("\nexpected 0-length diff, but got:\n%s\n", string(out.Bytes()))
	}

	return N, out.Bytes()
}

func compareFilesFirstNLines(diffFirstNlines int, expected string, observed string) (int, []byte) {
	alwaysPrintf("doing:\ndiff %v %v\n", observed, expected)

	x, err := ioutil.ReadFile(expected)
	if err != nil {
		fmt.Printf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err)
		return -2, nil // unknown, but not 0
	}
	o, err := ioutil.ReadFile(observed)
	if err != nil {
		fmt.Printf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err)
		return -2, nil // unknown, but not 0
	}

	xl := strings.Split(string(x), "\n")
	ol := strings.Split(string(o), "\n")

	if len(xl) < diffFirstNlines {
		return 1, []byte("expected did not have enough lines")
	}
	if len(ol) < diffFirstNlines {
		return 1, []byte("observed did not have enough lines")
	}
	for i := 0; i < diffFirstNlines; i++ {
		if xl[i] != ol[i] {
			return i + 1, []byte(fmt.Sprintf("first difference on line %v:\n ===== expected:\n\n%v\n\n ===== observed:\n\n%v\n\n", i+1, xl[i], ol[i]))
		}
	}
	return 0, nil
}

func stringSlicesDiffer(as, bs []string) bool {
	if len(as) != len(bs) {
		return true
	}
	for i := range as {
		if as[i] != bs[i] {
			return true
		}
	}
	return false
}

func diffDontStop(expected string, observed string) (int, []byte, error) {
	cmd := exec.Command("diff", "-r", "-b", "-B", observed, expected)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	N := len(out.Bytes())
	return N, out.Bytes(), err
}

// subtract bs from as and return the difference. Preserves
// the order given by as. aprefix is pre-pended to as returned.
func stringSliceSub(as, bs []string, aprefix string) (d []string) {
	amap := make(map[string]bool)
	for _, a := range as {
		amap[a] = true
	}
	for _, b := range bs {
		delete(amap, b)
	}
	for _, a := range as {
		if amap[a] {
			d = append(d, aprefix+a)
		}
	}
	return
}
