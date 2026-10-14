#include "textflag.h"

// func AllZeroSIMD(b []byte) bool
TEXT ·AllZeroSIMD(SB), NOSPLIT, $0-25
	MOVD b_base+0(FP), R0
	MOVD b_len+8(FP), R1

vector:
	CMP $16, R1
	BLO tail
	// Unaligned loads are supported. Only load complete vectors in the slice.
	VLD1.P (R0), [V0.B16]
	VMOV V0.D[0], R2
	VMOV V0.D[1], R3
	ORR R2, R3, R2
	CBNZ R2, nonzero
	SUB $16, R1, R1
	B vector

tail:
	CBZ R1, zero
	MOVBU.P 1(R0), R2
	CBNZ R2, nonzero
	SUB $1, R1, R1
	B tail

zero:
	MOVD $1, R0
	MOVB R0, ret+24(FP)
	RET

nonzero:
	MOVB ZR, ret+24(FP)
	RET
