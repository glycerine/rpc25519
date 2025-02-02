package jsync

import (
	"os"

	_ "golang.org/x/sys/unix"
)

//var _ = unix.Fadvise

// do we want MADV_SEQUENTIAL with madvise()?

func adviseKernelSequentialReadComing(f *os.File) {
	//Linux:
	// Advise the kernel that we'll be reading this file sequentially
	/*
	   fd := f.Fd()
	   panicOn(unix.Fadvise(int(fd), 0, 0, unix.FADV_SEQUENTIAL))
	   // If you want to also indicate that you'll only read
	   // the file once, you can combine it with FADV_NOREUSE:
	   // Advise sequential access and that we won't reuse the data
	   panicOn(unix.Fadvise(int(fd), 0, 0, unix.FADV_SEQUENTIAL|unix.FADV_NOREUSE))

	      // macOS version suggested by AI:
	               package main

	               import (
	                   "golang.org/x/sys/unix"
	                   "os"
	               )

	               func readFileSequentially(path string) error {
	                   file, err := os.Open(path)
	                   if err != nil {
	                       return err
	                   }
	                   defer file.Close()

	                   // Advise the kernel that we'll be reading this file sequentially
	                   fd := file.Fd()
	                   err = unix.Fadvise(int(fd), 0, 0, unix.FADV_SEQUENTIAL)
	                   if err != nil {
	                       return err
	                   }

	                   // Now proceed with reading your file...
	                   return nil
	               }
	*/
}
