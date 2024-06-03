package metrics

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestPopTags(t *testing.T) {
	check := func(tags string, tkey, tag string, rest string) {
		t.Helper()
		gtkey, gtag, grest := PopTag([]byte(tags))
		assert.Equal(t, tkey, string(gtkey))
		assert.Equal(t, tag, string(gtag))
		assert.Equal(t, rest, string(grest))
	}

	check("foo=bar,foo=bar", "foo", "foo=bar", "foo=bar")
	check("foo=bar", "foo", "foo=bar", "")
	check("foo=", "foo", "foo", "")
	check("foo", "foo", "foo", "")

	check(`foo\=bar=bar`, `foo\=bar`, `foo\=bar=bar`, "")
	check(`foo\\=bar=bar`, `foo\\`, `foo\\=bar=bar`, "")
	check(`foo\\\=bar=bar`, `foo\\\=bar`, `foo\\\=bar=bar`, "")
	check(`foo\\\\=bar=bar`, `foo\\\\`, `foo\\\\=bar=bar`, "")

	check(`foo=bar\,baz,bar=bif`, `foo`, `foo=bar\,baz`, "bar=bif")
	check(`foo=bar\\,baz,bar=bif`, `foo`, `foo=bar\\`, "baz,bar=bif")
	check(`foo=bar\\\,baz,bar=bif`, `foo`, `foo=bar\\\,baz`, "bar=bif")
	check(`foo=bar\\\\,baz,bar=bif`, `foo`, `foo=bar\\\\`, "baz,bar=bif")

	check(`0\=0\=0,00,0`, `0\=0\=0`, `0\=0\=0`, "00,0")
}
