package val

import (
	"fmt"
	"math"
	"unsafe"
)

type (
	ptr = unsafe.Pointer
	u64 = uint64
	i64 = int64
)

var tags [4]byte

var (
	TagInvalid = ptr(nil)
	TagInt     = ptr(&tags[0])
	TagBool    = ptr(&tags[1])
	TagFloat   = ptr(&tags[2])
	TagStr     = ptr(&tags[3])
)

var emptyStrPtr = func() ptr {
	// 8 bytes in case it gets dereferenced somehow
	return ptr(unsafe.StringData("\x00\x00\x00\x00\x00\x00\x00\x00"))
}()

type T struct {
	p ptr
	v u64
}

func (v T) Tag() ptr {
	if v.p == nil || uintptr(v.p)-uintptr(TagInt) < 3 {
		return v.p
	}
	return TagStr
}

func Int(val i64) T {
	return T{p: TagInt, v: u64(val)}
}

func (v T) AsInt() i64 {
	if uintptr(v.p)-uintptr(TagInt) < 2 {
		return i64(v.v)
	}
	return 0
}

func (v T) AsUint() u64 {
	if uintptr(v.p)-uintptr(TagInt) < 2 {
		return v.v
	}
	return 0
}

func Bool(val bool) T {
	var v uint64
	if val {
		v = 1
	}
	return T{p: TagBool, v: v}
}

func (v T) AsBool() bool { return v.v != 0 }

func Float(val float64) T {
	return T{p: TagFloat, v: math.Float64bits(val)}
}

func (v T) AsFloat() float64 { return math.Float64frombits(v.v) }

func Str(val string) T {
	v := T{p: ptr(unsafe.StringData(val)), v: u64(len(val))}
	if v.p == nil {
		v.p = emptyStrPtr
	}
	return v
}

func Bytes(val []byte) T {
	v := T{p: ptr(unsafe.SliceData(val)), v: u64(len(val))}
	if v.p == nil {
		v.p = emptyStrPtr
	}
	return v
}

func (v T) AsString() (x string) {
	if v.Tag() == TagStr {
		x = unsafe.String((*byte)(v.p), int(v.v))
	}
	return x
}

func (v T) String() string {
	switch v.Tag() {
	case TagInvalid:
		return "invalid"
	case TagInt:
		return fmt.Sprintf("int(%d)", v.AsInt())
	case TagBool:
		return fmt.Sprintf("bool(%t)", v.AsBool())
	case TagFloat:
		return fmt.Sprintf("float(%f)", v.AsFloat())
	case TagStr:
		return fmt.Sprintf("str(%q)", v.AsString())
	default:
		return "unknown"
	}
}

func GT(x, y T) bool {
	switch x.Tag() {
	case TagInt:
		return x.AsInt() > y.AsInt()
	case TagFloat:
		return x.AsFloat() > y.AsFloat()
	default:
		return x.AsString() > y.AsString()
	}
}

func GTE(x, y T) bool {
	switch x.Tag() {
	case TagInt:
		return x.AsInt() >= y.AsInt()
	case TagFloat:
		return x.AsFloat() >= y.AsFloat()
	default:
		return x.AsString() >= y.AsString()
	}
}

func LT(x, y T) bool {
	switch x.Tag() {
	case TagInt:
		return x.AsInt() < y.AsInt()
	case TagFloat:
		return x.AsFloat() < y.AsFloat()
	default:
		return x.AsString() < y.AsString()
	}
}

func LTE(x, y T) bool {
	switch x.Tag() {
	case TagInt:
		return x.AsInt() <= y.AsInt()
	case TagFloat:
		return x.AsFloat() <= y.AsFloat()
	default:
		return x.AsString() <= y.AsString()
	}
}
