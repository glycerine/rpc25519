package jsync

import (
	"context"
	"os"
	"time"

	"github.com/glycerine/idem"
)

func walkDirTree(root string, gotDir chan os.DirEntry) (err0 error) {

	fd, err := os.Open(root)
	if err != nil {
		return err
	}

	n := 100 // only read n at a time.

	for {
		dirents, derr := fd.ReadDir(n)
		if derr == io.EOF {
			// its fine, we have just read all
			// of the records in this first batch.
		} else if derr != nil {
			return fmt.Errorf("ReadDir error on path '%v': '%v'", root, derr)
		}

		//mypre := root + sep + de.Name()

		for _, de := range dirents {
			if de.IsDir() {
				walkDirTree(root+sep+de.Name(), de, gotDir)
				gotDir <- de // or yeild for iter, later.
			}
		}
	}
	return nil
}
