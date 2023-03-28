package memindex

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/histdb/histdb/rwutils"
)

func (t *T) AppendTo(w *rwutils.W) {
	w.Uint64(0) // version

	w.Uint64(uint64(t.card))

	t.metrics.AppendTo(w)
	t.tag_names.AppendTo(w)
	t.tkey_names.AppendTo(w)

	var buf bytes.Buffer
	appendBitmaps := func(bms []*roaring.Bitmap) {
		w.Varint(uint64(len(bms)))
		for _, bm := range bms {
			buf.Reset()
			bm.WriteTo(&buf)
			w.Varint(uint64(buf.Len()))
			w.Bytes(buf.Bytes())
		}
	}

	appendBitmaps(t.tag_to_metrics)
	appendBitmaps(t.tag_to_tkeys)
	appendBitmaps(t.tag_to_tags)
	appendBitmaps(t.tkey_to_metrics)
	appendBitmaps(t.tkey_to_tkeys)
	appendBitmaps(t.tkey_to_tags)
	appendBitmaps(t.tkey_to_tvals)
}

func (t *T) ReadFrom(r *rwutils.R) {
	_ = r.Uint64() // version

	t.card = int(r.Uint64())

	t.metrics.ReadFrom(r)
	t.tag_names.ReadFrom(r)
	t.tkey_names.ReadFrom(r)

	readBitmaps := func() []*roaring.Bitmap {
		bms := make([]*roaring.Bitmap, r.Varint())
		for i := range bms {
			bm := roaring.New()
			_, err := bm.FromBuffer(r.Bytes(int(r.Varint())))
			if err != nil {
				r.Invalid(err)
				break
			}
			bms[i] = bm
		}
		return bms
	}

	t.tag_to_metrics = readBitmaps()
	t.tag_to_tkeys = readBitmaps()
	t.tag_to_tags = readBitmaps()
	t.tkey_to_metrics = readBitmaps()
	t.tkey_to_tkeys = readBitmaps()
	t.tkey_to_tags = readBitmaps()
	t.tkey_to_tvals = readBitmaps()
}
