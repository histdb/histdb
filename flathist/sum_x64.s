//go:build amd64 && gc
// +build amd64,gc

#include "textflag.h"

// func sumLayer2SmallAVX2(data *layer2Small) uint64
TEXT ·sumLayer2SmallAVX2(SB), NOSPLIT, $0-16
	MOVQ         data+0(FP), AX

	VPMOVZXDQ    (AX), Y0
	VPMOVZXDQ    16(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    32(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    48(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    64(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    80(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    96(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    112(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    128(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    144(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    160(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    176(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    192(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    208(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    224(AX), Y1
	VPADDQ       Y0, Y1, Y0
	VPMOVZXDQ    240(AX), Y1
	VPADDQ       Y0, Y1, Y0

	VEXTRACTI128 $1, Y0, X1
	VPADDQ       X1, X0, X0
	VPSRLDQ	     $8, X0, X1
	VPADDQ       X1, X0, X0
	VMOVQ        X0, BX

	MOVQ         BX, ret+8(FP)
	VZEROUPPER
	RET

// func sumLayer2LargeAVX2(data *layer2Large) uint64
TEXT ·sumLayer2LargeAVX2(SB), NOSPLIT, $0-16
	MOVQ         data+0(FP), AX

	VMOVDQU      (AX), Y0
	VPADDQ       32(AX), Y0, Y0
	VPADDQ       64(AX), Y0, Y0
	VPADDQ       96(AX), Y0, Y0
	VPADDQ       128(AX), Y0, Y0
	VPADDQ       160(AX), Y0, Y0
	VPADDQ       192(AX), Y0, Y0
	VPADDQ       224(AX), Y0, Y0
	VPADDQ       256(AX), Y0, Y0
	VPADDQ       288(AX), Y0, Y0
	VPADDQ       320(AX), Y0, Y0
	VPADDQ       352(AX), Y0, Y0
	VPADDQ       384(AX), Y0, Y0
	VPADDQ       416(AX), Y0, Y0
	VPADDQ       448(AX), Y0, Y0
	VPADDQ       480(AX), Y0, Y0

	VEXTRACTI128 $1, Y0, X1
	VPADDQ       X1, X0, X0
	VPSRLDQ	     $8, X0, X1
	VPADDQ       X1, X0, X0
	VMOVQ        X0, BX

	MOVQ         BX, ret+8(FP)
	VZEROUPPER
	RET

