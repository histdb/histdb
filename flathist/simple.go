package flathist

import (
	"sync"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/rwutils"
)

var storePool = sync.Pool{New: func() any { return new(S) }}

type Histogram struct {
	s *S
	h H
}

func NewHistogram() *Histogram {
	s := storePool.Get().(*S)
	h := &Histogram{s: s, h: s.New()}
	if s.l0.Allocated() < 1024 {
		storePool.Put(s)
	}
	return h
}

func (h *Histogram) Finalize() { h.s.Finalize() }

func (h *Histogram) Merge(other *Histogram)      { Merge(h.s, h.h, other.s, other.h) }
func (h *Histogram) Equal(other *Histogram) bool { return Equal(h.s, h.h, other.s, other.h) }
func (h *Histogram) Observe(v float32)           { h.s.Observe(h.h, v) }
func (h *Histogram) Min() float32                { return h.s.Min(h.h) }
func (h *Histogram) Max() float32                { return h.s.Max(h.h) }
func (h *Histogram) Reset()                      { h.s.Reset(h.h) }
func (h *Histogram) Total() uint64               { return h.s.Total(h.h) }
func (h *Histogram) Quantile(q float64) float32  { return h.s.Quantile(h.h, q) }
func (h *Histogram) CDF(q float32) float64       { return h.s.CDF(h.h, q) }

func (h *Histogram) Summary() (total, sum, avg, vari float64) {
	return h.s.Summary(h.h)
}

func (h *Histogram) Distribution(cb func(value float32, count, total uint64)) {
	h.s.Distribution(h.h, cb)
}

func (h *Histogram) AppendTo(buf []byte) []byte {
	var w rwutils.W
	w.Init(buffer.OfCap(buf).SetPos(uintptr(len(buf))))
	AppendTo(h.s, h.h, &w)
	return w.Done().Prefix()
}

func (h *Histogram) ReadFrom(buf []byte) error {
	var r rwutils.R
	r.Init(buffer.OfLen(buf))
	ReadFrom(h.s, h.h, &r)
	rem, err := r.Done()
	if err != nil {
		return err
	} else if n := rem.Remaining(); n != 0 {
		return errs.Errorf("trailing data after hist read: %d bytes", n)
	}
	return nil
}
