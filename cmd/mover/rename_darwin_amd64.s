// +build darwin
// +build amd64

#include "textflag.h"

// func renamex_np(oldpath string, newpath string, flags uint) (err syscall.Errno)
TEXT ·renamex_np(SB),NOSPLIT,$0
    // Darwin syscall convention for renameatx_np:
    // olddir fd in DI (-2 for AT_FDCWD)
    // oldpath in SI
    // newdir fd in DX (-2 for AT_FDCWD)
    // newpath in R10
    // flags in R8
    MOVQ    $-2, DI              // AT_FDCWD for old directory
    MOVQ    oldpath+0(FP), SI    // old path string pointer
    MOVQ    $-2, DX              // AT_FDCWD for new directory
    MOVQ    newpath+16(FP), R10  // new path string pointer
    MOVL    flags+32(FP), R8     // flags
    MOVQ    $476, AX             // syscall #476 = renameatx_np
    SYSCALL
    JCC     ok                    // jump if carry clear (no error)
    NEGQ    AX                   // make error code positive
    MOVQ    AX, err+40(FP)       // return error code
    RET
ok:
    MOVQ    $0, err+40(FP)       // return 0 (no error)
    RET 
