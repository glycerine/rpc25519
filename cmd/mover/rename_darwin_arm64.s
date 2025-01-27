// +build darwin
// +build arm64

#include "textflag.h"

// func renamex_np(oldpath string, newpath string, flags uint) (err syscall.Errno)
TEXT ·renamex_np(SB),NOSPLIT,$0
    // Load string data pointers (first word of string header)
    MOVD    oldpath+0(FP), R0
    MOVD    newpath+16(FP), R1
    MOVW    flags+32(FP), R2
    MOVD    $435, R16         // syscall #435 = renamex_np
    SVC
    BMI     fail
    MOVD    $0, R0
    MOVD    R0, err+40(FP)
    RET
fail:
    NEG     R0, R0
    MOVD    R0, err+40(FP)
    RET
