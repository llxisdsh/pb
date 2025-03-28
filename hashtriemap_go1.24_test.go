//go:build go1.24

package pb

import (
	"runtime"
	"sync"
	"testing"
	"weak"
)

// TestConcurrentCache tests HashTrieMap in a scenario where it is used as
// the basis of a memory-efficient concurrent cache. We're specifically
// looking to make sure that CompareAndSwap and CompareAndDelete are
// atomic with respect to one another. When competing for the same
// key-value pair, they must not both succeed.
//
// This test is a regression test for issue #70970.
func TestConcurrentCache(t *testing.T) {
	type dummy [32]byte

	var m HashTrieMap[int, weak.Pointer[dummy]]

	type cleanupArg struct {
		key   int
		value weak.Pointer[dummy]
	}
	cleanup := func(arg cleanupArg) {
		m.CompareAndDelete(arg.key, arg.value)
	}
	get := func(m *HashTrieMap[int, weak.Pointer[dummy]], key int) *dummy {
		nv := new(dummy)
		nw := weak.Make(nv)
		for {
			w, loaded := m.LoadOrStore(key, nw)
			if !loaded {
				runtime.AddCleanup(nv, cleanup, cleanupArg{key, nw})
				return nv
			}
			if v := w.Value(); v != nil {
				return v
			}

			// Weak pointer was reclaimed, try to replace it with nw.
			if m.CompareAndSwap(key, w, nw) {
				runtime.AddCleanup(nv, cleanup, cleanupArg{key, nw})
				return nv
			}
		}
	}

	const N = 100_000
	const P = 5_000

	var wg sync.WaitGroup
	wg.Add(N)
	for i := range N {
		go func() {
			defer wg.Done()
			a := get(&m, i%P)
			b := get(&m, i%P)
			if a != b {
				t.Errorf("consecutive cache reads returned different values: a != b (%p vs %p)\n", a, b)
			}
		}()
	}
	wg.Wait()
}
