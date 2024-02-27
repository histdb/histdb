package num

import "github.com/histdb/histdb/rwutils"

type T interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

type E struct{}

func (E) ReadFrom(r *rwutils.R)  {}
func (*E) AppendTo(w *rwutils.W) {}

type U64 uint64

func (u U64) Digest() uint64         { return uint64(u) }
func (u U64) Equal(v U64) bool       { return u == v }
func (u *U64) ReadFrom(r *rwutils.R) { *u = U64(r.Uint64()) }
func (u U64) AppendTo(w *rwutils.W)  { w.Uint64(uint64(u)) }

type U32 uint32

func (u U32) Digest() uint64         { return uint64(u) }
func (u U32) Equal(v U32) bool       { return u == v }
func (u *U32) ReadFrom(r *rwutils.R) { *u = U32(r.Uint32()) }
func (u U32) AppendTo(w *rwutils.W)  { w.Uint32(uint32(u)) }

type U16 uint16

func (u U16) Digest() uint64         { return uint64(u) }
func (u U16) Equal(v U16) bool       { return u == v }
func (u *U16) ReadFrom(r *rwutils.R) { *u = U16(r.Uint16()) }
func (u U16) AppendTo(w *rwutils.W)  { w.Uint16(uint16(u)) }

type U8 uint8

func (u U8) Digest() uint64         { return uint64(u) }
func (u U8) Equal(v U8) bool        { return u == v }
func (u *U8) ReadFrom(r *rwutils.R) { *u = U8(r.Uint8()) }
func (u U8) AppendTo(w *rwutils.W)  { w.Uint8(uint8(u)) }
