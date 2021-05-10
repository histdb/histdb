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

	VEXTRACTI128 $0x01, Y0, X1
	VPADDQ       Y0, Y1, Y0
	VPSHUFD      $0x4e, X0, X1
	VPADDQ       X0, X1, X0
	VMOVQ        X0, BX

	MOVQ         BX, ret+8(FP)
	VZEROUPPER
	RET

// func sumLayer2LargeAVX2(data *layer2Large) uint64
TEXT ·sumLayer2LargeAVX2(SB), NOSPLIT, $0-16
	MOVQ         data+0(FP), AX

	VMOVDQU      192(AX), Y0
	VMOVDQU      128(AX), Y1
	VMOVDQU      224(AX), Y2
	VMOVDQU      (AX), Y3
	VMOVDQU      32(AX), Y4
	VMOVDQU      64(AX), Y5
	VMOVDQU      96(AX), Y6
	VMOVDQU      160(AX), Y7

	VPADDQ       416(AX), Y7, Y7
	VPADDQ       288(AX), Y4, Y4
	VPADDQ       480(AX), Y2, Y2
	VPADDQ       352(AX), Y6, Y6
	VPADDQ       Y7, Y4, Y4
	VPADDQ       Y6, Y2, Y2
	VPADDQ       384(AX), Y1, Y1
	VPADDQ       Y4, Y2, Y2
	VPADDQ       256(AX), Y3, Y3
	VPADDQ       Y3, Y1, Y1
	VPADDQ       448(AX), Y0, Y0
	VPADDQ       320(AX), Y5, Y3
	VPADDQ       Y3, Y0, Y0
	VPADDQ       Y1, Y0, Y0
	VPADDQ       Y2, Y0, Y0

	VEXTRACTI128 $0x01, Y0, X1
	VPADDQ       X1, X0, X0
	VPSHUFD      $0x4e, X0, X1
	VPADDQ       X0, X0, X1
	VMOVQ        X0, BX

	MOVQ         BX, ret+8(FP)
	VZEROUPPER
	RET
