#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"

// func allZeroAVX512(b []byte) bool
TEXT ·allZeroAVX512(SB), NOSPLIT, $0-32
    MOVQ b_base+0(FP), SI    // load slice base address
    MOVQ b_len+8(FP), R9     // load slice length
    
    // If length is zero, return true
    CMPQ R9, $0
    JE   avx512_true

    // Clear counter
    XORQ R8, R8

    // If length < 64, skip to AVX2 path
    CMPQ R9, $64
    JB   avx512_small

    // Zero ZMM0 for comparison
    VPXORQ Z0, Z0, Z0

avx512_loop:
    // Process 64 bytes at a time with AVX-512
    VPCMPB $0, (SI)(R8*1), Z0, K1  // Compare 64 bytes with zero
    KORTESTQ K1, K1                 // Test if all bytes were zero
    JNZ avx512_false               // If any non-zero, return false
    
    ADDQ $64, R8                   // Increment counter
    SUBQ $64, R9                   // Decrement remaining length
    CMPQ R9, $64                   // Check if we have 64 more bytes
    JAE  avx512_loop

avx512_small:
    // Handle remaining bytes with AVX2
    JMP ·allZeroAVX2(SB)

avx512_true:
    MOVB $1, ret+24(FP)
    RET

avx512_false:
    MOVB $0, ret+24(FP)
    RET

// func allZeroAVX2(b []byte) bool
TEXT ·allZeroAVX2(SB), NOSPLIT, $0-32
    MOVQ b_base+0(FP), SI
    MOVQ b_len+8(FP), R9
    
    CMPQ R9, $0
    JE   avx2_true

    XORQ R8, R8

    // If length < 32, skip to SSE2 path
    CMPQ R9, $32
    JB   avx2_small

    // Zero YMM0 for comparison
    VPXOR Y0, Y0, Y0

avx2_loop:
    // Process 32 bytes at a time with AVX2
    VPCMPEQB (SI)(R8*1), Y0, Y1
    VPMOVMSKB Y1, AX
    CMPL AX, $0xffffffff
    JNE  avx2_false
    
    ADDQ $32, R8
    SUBQ $32, R9
    CMPQ R9, $32
    JAE  avx2_loop

avx2_small:
    // Handle remaining bytes with SSE2
    JMP ·allZeroSSE2(SB)

avx2_true:
    MOVB $1, ret+24(FP)
    VZEROUPPER                    // Clear upper YMM registers
    RET

avx2_false:
    MOVB $0, ret+24(FP)
    VZEROUPPER
    RET

// func allZeroSSE2(b []byte) bool
TEXT ·allZeroSSE2(SB), NOSPLIT, $0-32
    MOVQ b_base+0(FP), SI
    MOVQ b_len+8(FP), R9
    
    CMPQ R9, $0
    JE   sse2_true

    XORQ R8, R8

    // If length < 16, skip to scalar
    CMPQ R9, $16
    JB   scalar

    // Zero XMM0 for comparison
    PXOR X0, X0

sse2_loop:
    // Process 16 bytes at a time with SSE2
    MOVOU (SI)(R8*1), X1
    PCMPEQB X0, X1
    PMOVMSKB X1, AX
    CMPL AX, $0xffff
    JNE  sse2_false
    
    ADDQ $16, R8
    SUBQ $16, R9
    CMPQ R9, $16
    JAE  sse2_loop

scalar:
    // Handle remaining bytes one at a time
    CMPQ R9, $0
    JE   sse2_true

scalar_loop:
    MOVB (SI)(R8*1), AL
    CMPB AL, $0
    JNE  sse2_false
    INCQ R8
    DECQ R9
    JNE  scalar_loop

sse2_true:
    MOVB $1, ret+24(FP)
    RET

sse2_false:
    MOVB $0, ret+24(FP)
    RET

// func AllZeroSIMD(b []byte) bool
TEXT ·AllZeroSIMD(SB), NOSPLIT, $0-32
    // Check CPU features and jump to appropriate implementation
    MOVB ·x86HasAVX512(SB), AX
    TESTB AX, AX
    JZ noAVX512                  // Jump if Zero (ZF=1)
    JMP ·allZeroAVX512(SB)

noAVX512:
    MOVB ·x86HasAVX2(SB), AX
    TESTB AX, AX
    JZ noAVX2                    // Jump if Zero (ZF=1)
    JMP ·allZeroAVX2(SB)

noAVX2:
    // SSE2 is baseline for amd64
    JMP ·allZeroSSE2(SB)
    RET

