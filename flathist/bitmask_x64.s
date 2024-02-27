// +build amd64,gc

#include "textflag.h"

TEXT ·bitmaskAVX(SB), NOSPLIT, $0-12
	MOVQ         data+0(FP), AX
	VMOVDQU      (AX), Y0
	VMOVMSKPS    Y0, CX
	VMOVDQU      32(AX), Y1
	VMOVMSKPS    Y1, BX
	SHLQ         $8, BX
	ORQ          BX, CX
	MOVL         CX, ret+8(FP)
	VZEROUPPER
	RET
