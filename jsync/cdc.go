package jsync

import (
	"fmt"

	"github.com/glycerine/rpc25519/jcdc"
)

// UsingCDCAlgo is a package global to allow
// benchmarks comparing CDC chunkers and settings.
//
// At the moment, the choice must match on the Client and
// Server pair actually in use, as we have not
// implemented any reader-makes-right switching.
var UsingCDCAlgo CDCAlgo = UltraCDCAlgo

type CDCAlgo int

const (
	UltraCDCAlgo       CDCAlgo = 0
	FastCDC_StadiaAlgo CDCAlgo = 1
	FastCDC_PlakarAlgo CDCAlgo = 2
	FNVAlgo            CDCAlgo = 3
	RabinKarpAlgo      CDCAlgo = 4
)

func GetCutpointer(choice CDCAlgo) (cdc jcdc.Cutpointer, opts *jcdc.CDC_Config) {
	//vv("choice = %v", choice)
	switch choice {
	case FastCDC_StadiaAlgo:
		// my take on the Stadia improved version of FastCDC
		opts = &jcdc.CDC_Config{
			MinSize:    4 * 1024,
			TargetSize: 60 * 1024,
			MaxSize:    80 * 1024,
		}
		cdc = jcdc.NewFastCDC_Stadia(opts)

	case FastCDC_PlakarAlgo:

		// my take on the Stadia improved version of FastCDC
		opts = &jcdc.CDC_Config{
			MinSize:    4 * 1024,
			TargetSize: 60 * 1024,
			MaxSize:    80 * 1024,
		}
		cdc = jcdc.NewFastCDC_Plakar(opts)

	case UltraCDCAlgo:
		// UltraCDC that I implemented.
		opts = &jcdc.CDC_Config{
			MinSize:    2 * 1024,
			TargetSize: 10 * 1024,
			MaxSize:    64 * 1024,
		}
		cdc = jcdc.NewUltraCDC(opts)
	case FNVAlgo:
		opts = &jcdc.CDC_Config{
			MinSize:    4 * 1024,
			TargetSize: 60 * 1024,
			MaxSize:    80 * 1024,
		}
		cdc = jcdc.NewFNVCDC(opts)
	case RabinKarpAlgo:
		opts = &jcdc.CDC_Config{
			MinSize:    4 * 1024,
			TargetSize: 60 * 1024,
			MaxSize:    80 * 1024,
		}
		cdc = jcdc.NewRabinKarpCDC(opts)

	default:
		panic(fmt.Sprintf("unknown CDCAlgo: %v", choice))
	}
	return
}
