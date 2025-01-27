// +build darwin
// +build amd64

#include "textflag.h"

// func renamex_np(oldpath string, newpath string, flags uint) (err syscall.Errno)
TEXT ·renamex_np(SB),NOSPLIT,$0
    // Load string data pointers (first word of string header)
    MOVQ    oldpath+0(FP), DI
    MOVQ    newpath+16(FP), SI
    MOVL    flags+32(FP), DX
    MOVQ    $435, AX          // syscall #435 = renamex_np
    SYSCALL
    JCC     ok
    MOVQ    AX, err+40(FP)    // return error code
    RET
ok:
    MOVQ    $0, err+40(FP)    // return 0 (no error)
    RET
