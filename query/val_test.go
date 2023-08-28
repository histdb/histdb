package query

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestValue(t *testing.T) {
	assert.Equal(t, valInt(0).Tag(), tagInt)
	assert.Equal(t, valBool(false).Tag(), tagBool)
	assert.Equal(t, valFloat(0).Tag(), tagFloat)
	assert.Equal(t, valStr("").Tag(), tagStr)
	assert.Equal(t, value{}.Tag(), tagInvalid)

	assert.Equal(t, valInt(2).Tag(), tagInt)
	assert.Equal(t, valBool(true).Tag(), tagBool)
	assert.Equal(t, valFloat(1.5).Tag(), tagFloat)
	assert.Equal(t, valStr("foo").Tag(), tagStr)
	assert.Equal(t, valBytes([]byte("foo")).Tag(), tagStr)

	assert.Equal(t, valInt(2).AsString(), "")
	assert.Equal(t, valBool(true).AsString(), "")
	assert.Equal(t, valFloat(1.5).AsString(), "")

	assert.Equal(t, valInt(0).AsBool(), false)
	assert.Equal(t, valInt(1).AsBool(), true)
	assert.Equal(t, valFloat(0).AsBool(), false)
	assert.Equal(t, valFloat(1.5).AsBool(), true)
	assert.Equal(t, valStr("").AsBool(), false)
	assert.Equal(t, valStr("bar").AsBool(), true)

	assert.Equal(t, valBool(false).AsInt(), 0)
	assert.Equal(t, valBool(true).AsInt(), 1)
	assert.Equal(t, valFloat(0).AsInt(), 0)
	assert.Equal(t, valFloat(1.5).AsInt(), 0)
	assert.Equal(t, valStr("").AsInt(), 0)
	assert.Equal(t, valStr("bar").AsInt(), 0)
}
