// +build linux
// +build amd64

#include "textflag.h"

// func renameat2(olddirfd int, oldpath string, newdirfd int, newpath string, flags uint) (err syscall.Errno)
TEXT ·renameat2(SB),NOSPLIT,$0
    MOVQ    olddirfd+0(FP), DI     // first argument: old dir fd
    MOVQ    oldpath+8(FP), SI      // second argument: old path string pointer
    MOVQ    newdirfd+32(FP), DX    // third argument: new dir fd
    MOVQ    newpath+40(FP), R10    // fourth argument: new path string pointer
    MOVQ    flags+64(FP), R8       // fifth argument: flags
    MOVQ    $316, AX               // syscall number for renameat2
    SYSCALL
    NEGQ    AX                     // negate return for error
    MOVQ    AX, err+72(FP)         // store error code
    RET
