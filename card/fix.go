package card

import "bytes"

type pred struct {
	tag    []byte
	action []byte
}

type Fixer struct{ preds map[string][]pred }

func (f *Fixer) DropTagKey(tkey []byte) { f.RewriteTag(tkey, nil, nil) }

func (f *Fixer) RewriteTag(tkey, tag, action []byte) {
	if f.preds == nil {
		f.preds = make(map[string][]pred)
	}
	f.preds[string(tkey)] = append(f.preds[string(tkey)], pred{
		tag:    tag,
		action: action,
	})
}

func (f *Fixer) Fix(tkey, tag []byte) []byte {
	for _, p := range f.preds[string(tkey)] {
		if bytes.Contains(tag, p.tag) {
			return p.action
		}
	}
	return tag
}
