package mergeiter

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/testhelp"
	"github.com/zeebo/pcg"
)

func TestMergedIterator(t *testing.T) {
	check := func(t *testing.T, keys string, mis ...Iterator) {
		var mi T
		mi.Init(mis)

		for mi.Next() {
			assert.Equal(t, mi.Key().String(), newFixedKey(keys[0:1]).String())
			keys = keys[1:]
		}

		assert.NoError(t, mi.Err())
		assert.Equal(t, keys, "")
	}

	t.Run("Basic", func(t *testing.T) {
		check(t, "0559aacdeqrstuuz",
			newFakeMergableIter("ace"),
			newFakeMergableIter("drsu"),
			newFakeMergableIter(""),
			newFakeMergableIter("059qz"),
			newFakeMergableIter("5atu"),
		)
	})

	t.Run("Fuzz", func(t *testing.T) {
		for c := 0; c < 1000; c++ {
			var exp []byte
			keys := "abcdefghijklmnopqrstuvwxyz"
			fmis := make([]fakeMergableIter, 1+pcg.Uint32n(32))
			for i := range fmis {
				fmis[i] = append(fmis[i], "")
			}

			for i := range keys {
				for j := 0; j < len(fmis); j++ {
					if pcg.Uint32n(2) == 0 {
						fmis[j] = append(fmis[j], keys[i:i+1])
						exp = append(exp, keys[i])
					}
				}
			}

			mis := make([]Iterator, len(fmis))
			for i := range mis {
				mis[i] = &fmis[i]
			}

			check(t, string(exp), mis...)
		}
	})
}

func BenchmarkMergedIterator(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		run := func(b *testing.B, l int) {
			mis := make([]Iterator, l)
			vs := make([][]string, l)
			is := make([]*fakeMergableIter, l)
			for i := range is {
				vs[i] = splitString(string(testhelp.Value(1 << 16)))
				is[i] = new(fakeMergableIter)
			}

			var mi T
			var keys uint64

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := range is {
					*is[j] = fakeMergableIter(vs[j])
					mis[j] = is[j]
				}
				mi.Init(mis)
				for mi.Next() {
					_, _ = mi.Key(), mi.Value()
					keys++
				}
				assert.NoError(b, mi.Err())
			}

			b.StopTimer()
			b.ReportMetric(float64(keys)/time.Since(now).Seconds(), "keys/sec")
			b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(keys), "ns/key")
		}

		b.Run("2", func(b *testing.B) { run(b, 2) })
		b.Run("4", func(b *testing.B) { run(b, 4) })
		b.Run("16", func(b *testing.B) { run(b, 16) })
	})
}

//////////////

func newFixedKey(x string) (k lsm.Key) {
	copy(k[:], x)
	return k
}

func splitString(x string) []string {
	out := make([]string, 1+len(x))
	for i := 0; i < len(x); i++ {
		out[i+1] = x[i : i+1]
	}
	return out
}

type fakeMergableIter []string

func newFakeMergableIter(x string) *fakeMergableIter {
	fmi := fakeMergableIter(splitString(x))
	return &fmi
}

func (f fakeMergableIter) Err() error        { return nil }
func (f fakeMergableIter) Key() lsm.Key      { return newFixedKey(f[0]) }
func (f fakeMergableIter) Timestamp() uint32 { return 0 }
func (f fakeMergableIter) Value() []byte     { return nil }

func (f *fakeMergableIter) Next() bool {
	if len(*f) > 0 {
		*f = (*f)[1:]
	}
	return len(*f) > 0
}
