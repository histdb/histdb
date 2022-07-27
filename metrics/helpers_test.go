package metrics

import (
	"testing"

	"gotest.tools/assert"
)

func TestPopTags(t *testing.T) {
	check := func(tags string, tkey, tag string, iskey bool, rest string) {
		t.Helper()
		gtkey, gtag, giskey, grest := PopTag(tags)
		assert.Equal(t, tkey, gtkey)
		assert.Equal(t, tag, gtag)
		assert.Equal(t, iskey, giskey)
		assert.Equal(t, rest, grest)
	}

	check("foo=bar,foo=bar", "foo", "foo=bar", false, "foo=bar")
	check("foo=bar", "foo", "foo=bar", false, "")
	check("foo=", "foo", "foo", false, "")
	check("foo", "foo", "foo", true, "")

	check(`foo\=bar=bar`, `foo\=bar`, `foo\=bar=bar`, false, "")
	check(`foo\\=bar=bar`, `foo\\`, `foo\\=bar=bar`, false, "")
	check(`foo\\\=bar=bar`, `foo\\\=bar`, `foo\\\=bar=bar`, false, "")
	check(`foo\\\\=bar=bar`, `foo\\\\`, `foo\\\\=bar=bar`, false, "")

	check(`foo=bar\,baz,bar=bif`, `foo`, `foo=bar\,baz`, false, "bar=bif")
	check(`foo=bar\\,baz,bar=bif`, `foo`, `foo=bar\\`, false, "baz,bar=bif")
	check(`foo=bar\\\,baz,bar=bif`, `foo`, `foo=bar\\\,baz`, false, "bar=bif")
	check(`foo=bar\\\\,baz,bar=bif`, `foo`, `foo=bar\\\\`, false, "baz,bar=bif")
}
