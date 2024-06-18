package val

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestValue(t *testing.T) {
	assert.Equal(t, Int(0).Tag(), TagInt)
	assert.Equal(t, Bool(false).Tag(), TagBool)
	assert.Equal(t, Float(0).Tag(), TagFloat)
	assert.Equal(t, Str("").Tag(), TagStr)
	assert.Equal(t, T{}.Tag(), TagInvalid)

	assert.Equal(t, Int(2).Tag(), TagInt)
	assert.Equal(t, Bool(true).Tag(), TagBool)
	assert.Equal(t, Float(1.5).Tag(), TagFloat)
	assert.Equal(t, Str("foo").Tag(), TagStr)
	assert.Equal(t, Bytes([]byte("foo")).Tag(), TagStr)

	assert.Equal(t, Int(2).AsString(), "")
	assert.Equal(t, Bool(true).AsString(), "")
	assert.Equal(t, Float(1.5).AsString(), "")
	assert.Equal(t, T{}.AsString(), "")

	assert.Equal(t, Int(0).AsBool(), false)
	assert.Equal(t, Int(1).AsBool(), true)
	assert.Equal(t, Float(0).AsBool(), false)
	assert.Equal(t, Float(1.5).AsBool(), true)
	assert.Equal(t, Str("").AsBool(), false)
	assert.Equal(t, Str("bar").AsBool(), true)
	assert.Equal(t, T{}.AsBool(), false)

	assert.Equal(t, Bool(false).AsInt(), 0)
	assert.Equal(t, Bool(true).AsInt(), 1)
	assert.Equal(t, Float(0).AsInt(), 0)
	assert.Equal(t, Float(1.5).AsInt(), 0)
	assert.Equal(t, Str("").AsInt(), 0)
	assert.Equal(t, Str("bar").AsInt(), 0)
	assert.Equal(t, T{}.AsInt(), 0)
}
