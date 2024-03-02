// +build amd64,gc

#include "textflag.h"

TEXT ·bitmaskAVX(SB), NOSPLIT, $0-12
	MOVQ         data+0(FP), AX

	VMOVDQU      (AX), Y0
	VMOVMSKPS    Y0, BX

	VMOVDQU      32(AX), Y1
	VMOVMSKPS    Y1, CX

	VMOVDQU      64(AX), Y2
	VMOVMSKPS    Y2, DX

	VMOVDQU      96(AX), Y3
	VMOVMSKPS    Y3, SI

	SHLL         $24, SI
	SHLL         $16, DX
	ORL          DX, SI
	SHLL         $8, CX
	ORL          CX, SI
	ORL          BX, SI

	MOVL         SI, ret+8(FP)

	VZEROUPPER
	RET
