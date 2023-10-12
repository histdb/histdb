package query

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
	tagInvalid = ptr(nil)
	tagInt     = ptr(&tags[0])
	tagBool    = ptr(&tags[1])
	tagFloat   = ptr(&tags[2])
	tagStr     = ptr(&tags[3])
)

var emptyStrPtr = func() ptr {
	// 8 bytes in case it gets dereferenced somehow
	return ptr(unsafe.StringData("\x00\x00\x00\x00\x00\x00\x00\x00"))
}()

type value struct {
	p ptr
	v u64
}

func (v value) Tag() ptr {
	if v.p == nil || uintptr(v.p)-uintptr(tagInt) < 3 {
		return v.p
	}
	return tagStr
}

func valInt(val i64) value {
	return value{p: tagInt, v: u64(val)}
}

func (v value) AsInt() i64 {
	if uintptr(v.p)-uintptr(tagInt) < 2 {
		return i64(v.v)
	}
	return 0
}

func (v value) AsUint() u64 {
	if uintptr(v.p)-uintptr(tagInt) < 2 {
		return v.v
	}
	return 0
}

func valBool(val bool) value {
	var v uint64
	if val {
		v = 1
	}
	return value{p: tagBool, v: v}
}

func (v value) AsBool() bool { return v.v != 0 }

func valFloat(val float64) value {
	return value{p: tagFloat, v: math.Float64bits(val)}
}

func (v value) AsFloat() float64 { return math.Float64frombits(v.v) }

func valStr(val string) value {
	v := value{p: ptr(unsafe.StringData(val)), v: u64(len(val))}
	if v.p == nil {
		v.p = emptyStrPtr
	}
	return v
}

func valBytes(val []byte) value {
	v := value{p: ptr(unsafe.SliceData(val)), v: u64(len(val))}
	if v.p == nil {
		v.p = emptyStrPtr
	}
	return v
}

func (v value) AsString() (x string) {
	if v.Tag() == tagStr {
		x = unsafe.String((*byte)(v.p), int(v.v))
	}
	return x
}

func (v value) String() string {
	switch v.Tag() {
	case tagInvalid:
		return "invalid"
	case tagInt:
		return fmt.Sprintf("int(%d)", v.AsInt())
	case tagBool:
		return fmt.Sprintf("bool(%t)", v.AsBool())
	case tagFloat:
		return fmt.Sprintf("float(%f)", v.AsFloat())
	case tagStr:
		return fmt.Sprintf("str(%q)", v.AsString())
	default:
		return "unknown"
	}
}

func valueGT(x, y value) bool {
	switch x.Tag() {
	case tagInt:
		return x.AsInt() > y.AsInt()
	case tagFloat:
		return x.AsFloat() > y.AsFloat()
	default:
		return x.AsString() > y.AsString()
	}
}

func valueGTE(x, y value) bool {
	switch x.Tag() {
	case tagInt:
		return x.AsInt() >= y.AsInt()
	case tagFloat:
		return x.AsFloat() >= y.AsFloat()
	default:
		return x.AsString() >= y.AsString()
	}
}

func valueLT(x, y value) bool {
	switch x.Tag() {
	case tagInt:
		return x.AsInt() < y.AsInt()
	case tagFloat:
		return x.AsFloat() < y.AsFloat()
	default:
		return x.AsString() < y.AsString()
	}
}

func valueLTE(x, y value) bool {
	switch x.Tag() {
	case tagInt:
		return x.AsInt() <= y.AsInt()
	case tagFloat:
		return x.AsFloat() <= y.AsFloat()
	default:
		return x.AsString() <= y.AsString()
	}
}
