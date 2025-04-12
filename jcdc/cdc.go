package jcdc

import (
	"fmt"
)

type CDCAlgo int

const (
	UltraCDC_Algo      CDCAlgo = 0
	FastCDC_StadiaAlgo CDCAlgo = 1
	FastCDC_PlakarAlgo CDCAlgo = 2
	FNV_Algo           CDCAlgo = 3
	RabinKarp_Algo     CDCAlgo = 4
	ResticRabin_Algo   CDCAlgo = 8
)

// cfg = &CDC_Config{
// 	MinSize:    4 * 1024,
// 	TargetSize: 60 * 1024,
// 	MaxSize:    80 * 1024,
// }

func GetCutpointer(choice CDCAlgo, cfg *CDC_Config) (cdc Cutpointer) {
	//vv("choice = %v", choice)
	switch choice {
	case FastCDC_StadiaAlgo:
		// my take on the Stadia improved version of FastCDC
		cdc = NewFastCDC_Stadia(cfg)
	case FastCDC_PlakarAlgo:
		// Plakar version of FastCDC
		cdc = NewFastCDC_Plakar(cfg)
	case UltraCDC_Algo:
		// UltraCDC that I implemented.
		cdc = NewUltraCDC(cfg)
	case FNV_Algo:
		// exponential with alterntive FNV1a rolling hash
		cdc = NewFNVCDC(cfg)
	case RabinKarp_Algo:
		// traditional exponential e.g. rsync
		cdc = NewRabinKarpCDC(cfg)
	case ResticRabin_Algo:
		cdc = NewResticRabinCDC(cfg)
	default:
		panic(fmt.Sprintf("unknown CDCAlgo: %v", choice))
	}
	return
}
