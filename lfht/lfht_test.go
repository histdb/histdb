package lfht

import (
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestTable(t *testing.T) {
	var ta T[int, int]
	for i := range uint32(100) {
		ta.Insert(getKey(i), getHash(i), getValue)
		if v, ok := ta.Find(getKey(i), getHash(i)); !ok || v != 1 {
			ta.Dump(os.Stderr)
			t.Fatal(i)
		}
	}
	for i := range uint32(100) {
		if v, ok := ta.Find(getKey(i), getHash(i)); !ok || v != 1 {
			ta.Dump(os.Stderr)
			t.Fatal(i)
		}
	}
	for iter := ta.Iterator(); iter.Next(); {
		if v, ok := ta.Find(iter.Key(), iter.Hash()); !ok || v != iter.Value() {
			ta.Dump(os.Stderr)
			t.Fatal(iter.Key(), iter.Value())
		}
	}
}

func TestTable_Iterator(t *testing.T) {
	for range 1 {
		var ta T[int, int]
		for i := range uint32(100) {
			ta.Insert(getKey(i), getHash(i), getValue)
		}

		var (
			done  = make(chan struct{})
			count = make(chan int, runtime.GOMAXPROCS(-1)/2+1)
		)

		for i := 0; i < cap(count); i++ {
			go func() {
				rng := mwc.Rand()
				total := 0
			inserting:
				for {
					select {
					case <-done:
						break inserting
					default:
						n := rng.Uint32n(kSize)
						ta.Insert(getKey(n), getHash(n), func() int {
							total++
							runtime.Gosched()
							return 1
						})
					}
				}
				count <- total
			}()
		}

		got := make(map[int]struct{})
		for iter := ta.Iterator(); iter.Next(); {
			got[iter.Key()] = struct{}{}
			runtime.Gosched()
		}
		close(done)

		total := 0
		for i := 0; i < cap(count); i++ {
			total += <-count
		}

		for i := range uint32(100) {
			if _, ok := got[getKey(i)]; !ok {
				t.Fatal(total, len(got), i)
			}
		}

		got = make(map[int]struct{})
		for iter := ta.Iterator(); iter.Next(); {
			got[iter.Key()] = struct{}{}
		}

		assert.That(t, total+100 >= len(got))
	}
}

func BenchmarkLFHT(b *testing.B) {
	b.Run("UpsertFull", func(b *testing.B) {
		rng := mwc.Rand()
		b.ReportAllocs()

		for b.Loop() {
			var t T[int, int]
			for range kSize {
				n := rng.Uint32n(kSize)
				t.Insert(getKey(n), getHash(n), getValue)
			}
		}
	})

	b.Run("Upsert", func(b *testing.B) {
		rng := mwc.Rand()
		var t T[int, int]
		b.ReportAllocs()

		for b.Loop() {
			n := rng.Uint32n(kSize)
			t.Insert(getKey(n), getHash(n), getValue)
		}
	})

	b.Run("Lookup", func(b *testing.B) {
		rng := mwc.Rand()
		var t T[int, int]
		var sink1 int
		var sink2 bool

		for i := range uint32(kSize) {
			t.Insert(getKey(i), getHash(i), getValue)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			n := rng.Uint32n(kSize)
			sink1, sink2 = t.Find(getKey(n), getHash(n))
		}

		runtime.KeepAlive(sink1)
		runtime.KeepAlive(sink2)
	})

	b.Run("UpsertParallel", func(b *testing.B) {
		var t T[int, int]
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			rng := mwc.Rand()
			for pb.Next() {
				n := rng.Uint32n(kSize)
				t.Insert(getKey(n), getHash(n), getValue)
			}
		})
	})

	b.Run("UpsertFullParallel", func(b *testing.B) {
		procs := runtime.GOMAXPROCS(-1)
		iters := kSize / procs
		b.ReportAllocs()

		for b.Loop() {
			var t T[int, int]
			var wg sync.WaitGroup

			for range procs {
				wg.Add(1)
				go func() {
					rng := mwc.Rand()
					for range iters {
						n := rng.Uint32n(kSize)
						t.Insert(getKey(n), getHash(n), getValue)
					}
					wg.Done()
				}()
			}
			wg.Wait()
		}
	})

	b.Run("Iterate", func(b *testing.B) {
		var t T[int, int]
		for i := range uint32(kSize) {
			t.Insert(getKey(i), getHash(i), getValue)
		}
		b.ReportAllocs()
		b.ResetTimer()

		iter := t.Iterator()
		for b.Loop() {
			if !iter.Next() {
				iter = t.Iterator()
			}
		}
	})
}
