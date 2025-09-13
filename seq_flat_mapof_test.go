package pb

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSeqFlatMapOf_BasicOperations(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	if _, ok := m.Load(1); ok {
		t.Fatalf("expected empty")
	}
	m.Store(1, 42)
	if v, ok := m.Load(1); !ok || v != 42 {
		t.Fatalf("load got %v %v", v, ok)
	}
	m.Store(1, 43)
	if v, ok := m.Load(1); !ok || v != 43 {
		t.Fatalf("load2 got %v %v", v, ok)
	}
	m.Delete(1)
	if _, ok := m.Load(1); ok {
		t.Fatalf("expected deleted")
	}
}

func TestSeqFlatMapOf_MultipleKeys(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	for i := 0; i < 1000; i++ {
		m.Store(i, i+10)
	}
	for i := 0; i < 1000; i++ {
		if v, ok := m.Load(i); !ok || v != i+10 {
			t.Fatalf("k=%d got %v %v", i, v, ok)
		}
	}
}

func TestSeqFlatMapOf_Concurrent(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	var wg sync.WaitGroup
	n := runtime.GOMAXPROCS(0)
	wg.Add(n * 2)
	for g := 0; g < n; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				m.Store(base*10000+i, i)
			}
		}(g)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				m.Load(base*10000 + i)
			}
		}(g)
	}
	wg.Wait()
}

func TestSeqFlatMapOf_Range(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	for i := 0; i < 1000; i++ {
		m.Store(i, i)
	}
	seen := 0
	m.Range(func(k, v int) bool {
		if v == 0 && k != 0 {
			t.Fatalf("unexpected zero value published k=%d", k)
		}
		seen++
		return true
	})
	if seen == 0 {
		t.Fatalf("expected some entries")
	}
}

func TestSeqFlatMapOf_LoadOrStore(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	v, loaded := m.LoadOrStore(1, 10)
	if loaded || v != 10 {
		t.Fatalf("first store got %v %v", v, loaded)
	}
	v, loaded = m.LoadOrStore(1, 11)
	if !loaded || v != 10 {
		t.Fatalf("second los got %v %v", v, loaded)
	}
}

func TestSeqFlatMapOf_Size_IsZero(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	if !m.IsZero() {
		t.Fatalf("expected zero")
	}
	m.Store(1, 2)
	if m.Size() != 1 {
		t.Fatalf("size got %d", m.Size())
	}
}

// ---------------- Additional boundary/concurrency tests ----------------

func TestSeqFlatMapOf_LoadOrStoreFn(t *testing.T) {
	m := NewSeqFlatMapOf[string, int]()
	var calls int32

	// first: should call fn and store result
	actual, loaded := m.LoadOrStoreFn("key1", func() int {
		atomic.AddInt32(&calls, 1)
		return 100
	})
	if loaded || actual != 100 {
		t.Fatalf("first call: got (%v,%v)", actual, loaded)
	}

	// second: should not call fn
	actual, loaded = m.LoadOrStoreFn("key1", func() int {
		atomic.AddInt32(&calls, 1)
		return 200
	})
	if !loaded || actual != 100 {
		t.Fatalf("second call: got (%v,%v)", actual, loaded)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("valueFn called %d times, want 1", got)
	}
}

func TestSeqFlatMapOf_LoadOrStoreFn_OnceUnderRace(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	var called int32
	var wg sync.WaitGroup
	workers := max(2, runtime.GOMAXPROCS(0)) // Reduce Concurrency​
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, _ = m.LoadOrStoreFn(999, func() int {
				// widen race window
				atomic.AddInt32(&called, 1)
				time.Sleep(1 * time.Millisecond)
				return 777
			})
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&called); got != 1 {
		t.Fatalf("LoadOrStoreFn invoked %d times; want 1", got)
	}
	if v, ok := m.Load(999); !ok || v != 777 {
		t.Fatalf("post state: got (%v,%v), want (777,true)", v, ok)
	}
}

// Stress ABA-like rapid flips on the same key and ensure seqlock prevents torn
// reads.
func TestSeqFlatMapOf_SeqlockConsistency_StressABA(t *testing.T) {
	type pair struct{ X, Y uint64 }
	m := NewSeqFlatMapOf[int, pair]()

	m.Store(0, pair{X: 0, Y: ^uint64(0)})

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		seq  uint32
	)

	writerN := 4 // Reduce Concurrency​
	for w := 0; w < writerN; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s := atomic.AddUint32(&seq, 1)
					val := pair{X: uint64(s), Y: ^uint64(s)}
					m.Process(
						0,
						func(old pair, loaded bool) (pair, ComputeOp, pair, bool) {
							return val, UpdateOp, val, true
						},
					)
				}
			}
		}()
	}

	readerN := 4 // Reduce Concurrency​
	for r := 0; r < readerN; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v, ok := m.Load(0)
					if !ok {
						t.Errorf("key missing while stressed")
						return
					}
					if v.Y != ^v.X {
						t.Errorf("torn read detected: %+v", v)
						return
					}
				}
			}
		}()
	}

	time.Sleep(150 * time.Millisecond) // Reduce Duration​
	close(stop)
	wg.Wait()
}

// Load/Delete race semantics: under seqlock, readers should not observe
// ok==true with zero value.
func TestSeqFlatMapOf_LoadDeleteRace_Semantics(t *testing.T) {
	m := NewSeqFlatMapOf[string, uint32]()
	const key = "k"
	const insertedVal uint32 = 1

	var (
		anom uint64
		stop uint32
	)

	readers := max(2, runtime.GOMAXPROCS(0)) // Reduce Concurrency​
	dur := 500 * time.Millisecond            // Reduce Duration​

	var wg sync.WaitGroup
	wg.Add(readers + 1)

	go func() {
		defer wg.Done()
		deadline := time.Now().Add(dur)
		for time.Now().Before(deadline) {
			m.Store(key, insertedVal)
			runtime.Gosched()
			m.Delete(key)
			runtime.Gosched()
		}
		atomic.StoreUint32(&stop, 1)
	}()

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				v, ok := m.Load(key)
				if ok && v == 0 {
					atomic.AddUint64(&anom, 1)
				}
			}
		}()
	}

	wg.Wait()
	if anom > 0 {
		t.Fatalf(
			"Load/Delete race: observed ok==true && value==0 %d times",
			anom,
		)
	}
}

// Key torn-read stress: delete/re-insert big keys under load and ensure key
// invariants hold.
func TestSeqFlatMapOf_KeyTornRead_Stress_Heavy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy stress in -short mode")
	}

	type bigKey struct{ A, B uint64 }
	m := NewSeqFlatMapOf[bigKey, int]()

	const N = 4096
	keys := make([]bigKey, N)
	for i := 0; i < N; i++ {
		ai := uint64(i*2147483647 + 987654321)
		keys[i] = bigKey{A: ai, B: ^ai}
		m.Store(keys[i], i)
	}

	var (
		wg           sync.WaitGroup
		stop         = make(chan struct{})
		foundTornKey atomic.Bool
	)

	readerN := max(8, runtime.GOMAXPROCS(0)*2)
	wg.Add(readerN)
	for r := 0; r < readerN; r++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.Range(func(k bigKey, _ int) bool {
						if k.B != ^k.A {
							foundTornKey.Store(true)
							return false
						}
						return true
					})
				}
			}
		}()
	}

	loadN := max(4, runtime.GOMAXPROCS(0))
	wg.Add(loadN)
	for r := 0; r < loadN; r++ {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := id; i < N; i += loadN {
						m.Load(keys[i])
					}
				}
			}
		}(r)
	}

	writerN := max(8, runtime.GOMAXPROCS(0)*2)
	wg.Add(writerN)
	for w := 0; w < writerN; w++ {
		go func(offset int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := offset; i < N; i += writerN {
						k := keys[i]
						m.Process(k, func(old int, loaded bool) (int, ComputeOp, int, bool) { return 0, DeleteOp, 0, false })
						m.Process(k, func(old int, loaded bool) (int, ComputeOp, int, bool) { return i, UpdateOp, i, true })
					}
					runtime.Gosched()
				}
			}
		}(w)
	}

	dur := 2 * time.Second
	timer := time.NewTimer(dur)
	<-timer.C
	close(stop)
	wg.Wait()

	if foundTornKey.Load() {
		t.Fatalf("detected possible torn read of key: invariant k.B == ^k.A violated under concurrency (heavy)")
	}
}

func TestSeqFlatMapOf_Range_NoDuplicateVisit_Heavy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy stress in -short mode")
	}

	m := NewSeqFlatMapOf[int, int]()
	const N = 2048 * 50 // ~100K keys to cover chains
	for i := 0; i < N; i++ {
		m.Store(i, i)
	}

	var stop uint32
	writerN := max(4, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	wg.Add(writerN)
	for w := 0; w < writerN; w++ {
		go func(offset int) {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				for i := offset; i < N; i += writerN {
					m.Process(i, func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return old + 1, UpdateOp, old + 1, true
					})
				}
				runtime.Gosched()
			}
		}(w)
	}

	// Multiple Range passes; each pass must not yield duplicates
	rounds := 20
	for r := 0; r < rounds; r++ {
		seen := make([]uint8, N) // compact bitset
		m.Range(func(k, v int) bool {
			if k >= 0 && k < N {
				if seen[k] != 0 {
					t.Fatalf("Range yielded duplicate key within a single pass (heavy): %d", k)
				}
				seen[k] = 1
			}
			return true
		})
	}

	atomic.StoreUint32(&stop, 1)
	wg.Wait()
}

// New test: verify a single Range call never yields duplicate keys, even under
// concurrent writers causing seqlock retries.
func TestSeqFlatMapOf_Range_NoDuplicateVisit(t *testing.T) {
	m := NewSeqFlatMapOf[int, int]()
	const N = 2048 * 100 // cover multiple buckets and chains
	for i := 0; i < N; i++ {
		m.Store(i, i)
	}

	var stop uint32
	writerN := max(2, runtime.GOMAXPROCS(0)/2) // Reduce Concurrency​
	var wg sync.WaitGroup
	wg.Add(writerN)
	for w := 0; w < writerN; w++ {
		go func(offset int) {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				for i := offset; i < N; i += writerN {
					// mutate value to force seq changes
					m.Process(i, func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return old + 1, UpdateOp, old + 1, true
					})
				}
				runtime.Gosched()
			}
		}(w)
	}

	// Run several Range passes under writer churn; each pass must not see duplicates
	rounds := 10
	for r := 0; r < rounds; r++ {
		seen := make(map[int]struct{}, N)
		m.Range(func(k, v int) bool {
			if _, dup := seen[k]; dup {
				t.Fatalf("Range yielded duplicate key within a single pass: %d", k)
			}
			seen[k] = struct{}{}
			return true
		})
	}

	atomic.StoreUint32(&stop, 1)
	wg.Wait()
}
