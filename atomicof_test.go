package pb

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type bigSeq struct {
	A uint64
	B uint64
	X [32]uint64
	C uint64
	D uint64
}

func TestAtomicOfSeq_NoTornRead(t *testing.T) {
	var a atomicOf[bigSeq]
	var seq uint32

	x0 := uint64(3)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0xAA, D: ^(x0 ^ 0xAA)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	a.StoreWithSeq(&seq, v0)

	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	writers := 6
	readers := 12

	wg.Add(writers)
	for w := range writers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					x := uint64(rand.Int64()) ^ uint64(id)*0x9e3779b97f4a7bb1
					v := bigSeq{A: x, B: ^x, C: x ^ 0xAA, D: ^(x ^ 0xAA)}
					for i := range v.X {
						v.X[i] = x + uint64(i)
					}
					a.StoreWithSeq(&seq, v)
					runtime.Gosched()
				}
			}
		}(w)
	}

	wg.Add(readers)
	for r := range readers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.LoadWithSeq(&seq)
					if v.B != ^v.A || v.D != ^v.C {
						errors.Add(1)
					}
					for i := range v.X {
						if v.X[i] != v.A+uint64(i) {
							errors.Add(1)
							break
						}
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()

	if errors.Load() != 0 {
		t.Fatalf("torn reads: %d", errors.Load())
	}
}

func TestAtomicOfSeq_ContinuousWritersProgress(t *testing.T) {
	var a atomicOf[bigSeq]
	var seq uint32

	x0 := uint64(11)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0x33, D: ^(x0 ^ 0x33)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	a.StoreWithSeq(&seq, v0)

	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	writers := 8
	readers := 12

	wg.Add(writers)
	for w := range writers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					x := uint64(rand.Int64()) ^ uint64(id)*0x9e3779b97f4a7bb1
					v := bigSeq{A: x, B: ^x, C: x ^ 0x33, D: ^(x ^ 0x33)}
					for i := range v.X {
						v.X[i] = x + uint64(i)
					}
					a.StoreWithSeq(&seq, v)
					runtime.Gosched()
				}
			}
		}(w)
	}

	counts := make([]atomic.Int64, readers)
	wg.Add(readers)
	for r := range readers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.LoadWithSeq(&seq)
					if v.B != ^v.A || v.D != ^v.C {
						errors.Add(1)
					}
					for i := range v.X {
						if v.X[i] != v.A+uint64(i) {
							errors.Add(1)
							break
						}
					}
					counts[id].Add(1)
					runtime.Gosched()
				}
			}
		}(r)
	}

	time.Sleep(800 * time.Millisecond)
	close(stop)
	wg.Wait()

	var total int64
	for i := range counts {
		total += counts[i].Load()
	}

	if errors.Load() != 0 {
		t.Fatalf("invariants broken: %d", errors.Load())
	}
	_ = total
}

func TestAtomicOfSeq_OddHoldSpin(t *testing.T) {
	var a atomicOf[bigSeq]
	var seq uint32

	x0 := uint64(21)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0x77, D: ^(x0 ^ 0x77)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	a.StoreWithSeq(&seq, v0)

	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for {
					select {
					case <-stop:
						return
					default:
					}
					s := atomic.LoadUint32(&seq)
					if s&1 != 0 {
						runtime.Gosched()
						continue
					}
					if atomic.CompareAndSwapUint32(&seq, s, s|1) {
						time.Sleep(1 * time.Millisecond)
						x := uint64(rand.Int64())
						v := bigSeq{A: x, B: ^x, C: x ^ 0x77, D: ^(x ^ 0x77)}
						for i := range v.X {
							v.X[i] = x + uint64(i)
						}
						a.buf = v
						atomic.StoreUint32(&seq, s+2)
						break
					}
				}
			}
		}
	}()

	readers := 8
	counts := make([]atomic.Int64, readers)
	wg.Add(readers)
	for r := range readers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.LoadWithSeq(&seq)
					if v.B != ^v.A || v.D != ^v.C {
						errors.Add(1)
					}
					for i := range v.X {
						if v.X[i] != v.A+uint64(i) {
							errors.Add(1)
							break
						}
					}
					counts[id].Add(1)
					runtime.Gosched()
				}
			}
		}(r)
	}

	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()

	var total int64
	for i := range counts {
		total += counts[i].Load()
	}

	if errors.Load() != 0 {
		t.Fatalf("invariants broken: %d", errors.Load())
	}
	_ = total
}

func TestAtomicOfSeq_AddStyleWriterProducesTornReads(t *testing.T) {
	var a atomicOf[bigSeq]
	var seq uint32

	x0 := uint64(31)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0xCC, D: ^(x0 ^ 0xCC)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	a.StoreWithSeq(&seq, v0)

	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	writers := 8
	readers := 12

	wg.Add(writers)
	for w := range writers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					atomic.AddUint32(&seq, 1)
					x := uint64(rand.Int64()) ^ uint64(id)*0x9e3779b97f4a7bb1
					v := bigSeq{A: x, B: ^x, C: x ^ 0xCC, D: ^(x ^ 0xCC)}
					for i := range v.X {
						v.X[i] = x + uint64(i)
					}
					*a.Raw() = v
					atomic.AddUint32(&seq, 1)
					runtime.Gosched()
				}
			}
		}(w)
	}

	wg.Add(readers)
	for r := range readers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.LoadWithSeq(&seq)
					if v.B != ^v.A || v.D != ^v.C {
						errors.Add(1)
					}
					for i := range v.X {
						if v.X[i] != v.A+uint64(i) {
							errors.Add(1)
							break
						}
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	time.Sleep(800 * time.Millisecond)
	close(stop)
	wg.Wait()

	if c := errors.Load(); c == 0 {
		t.Fatalf("no torn reads observed")
	}
}

func TestAtomicOfSeq_AddStyleWriterWithLock_NoTornRead(t *testing.T) {
	var a atomicOf[bigSeq]
	var seq uint32
	var mu sync.Mutex

	x0 := uint64(41)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0x99, D: ^(x0 ^ 0x99)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	a.StoreWithSeq(&seq, v0)

	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	writers := 8
	readers := 12

	wg.Add(writers)
	for w := range writers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					mu.Lock()
					atomic.AddUint32(&seq, 1)
					x := uint64(rand.Int64()) ^ uint64(id)*0x9e3779b97f4a7bb1
					v := bigSeq{A: x, B: ^x, C: x ^ 0x99, D: ^(x ^ 0x99)}
					for i := range v.X {
						v.X[i] = x + uint64(i)
					}
					*a.Raw() = v
					atomic.AddUint32(&seq, 1)
					mu.Unlock()
					runtime.Gosched()
				}
			}
		}(w)
	}

	wg.Add(readers)
	for r := range readers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.LoadWithSeq(&seq)
					if v.B != ^v.A || v.D != ^v.C {
						errors.Add(1)
					}
					for i := range v.X {
						if v.X[i] != v.A+uint64(i) {
							errors.Add(1)
							break
						}
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	time.Sleep(800 * time.Millisecond)
	close(stop)
	wg.Wait()

	if c := errors.Load(); c != 0 {
		t.Fatalf("torn reads observed under lock: %d", c)
	}
}

func TestAtomicOf_DirtyLoadProducesTornReads(t *testing.T) {
	var a atomicOf[bigSeq]

	x0 := uint64(51)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0xAB, D: ^(x0 ^ 0xAB)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	a.store(v0)

	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	writers := 8
	readers := 12

	wg.Add(writers)
	for w := range writers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					x := uint64(rand.Int64()) ^ uint64(id)*0x9e3779b97f4a7bb1
					v := bigSeq{A: x, B: ^x, C: x ^ 0xAB, D: ^(x ^ 0xAB)}
					for i := range v.X {
						v.X[i] = x + uint64(i)
					}
					a.store(v)
					runtime.Gosched()
				}
			}
		}(w)
	}

	wg.Add(readers)
	for r := range readers {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.load()
					if v.B != ^v.A || v.D != ^v.C {
						errors.Add(1)
					}
					for i := range v.X {
						if v.X[i] != v.A+uint64(i) {
							errors.Add(1)
							break
						}
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()

	if c := errors.Load(); c == 0 {
		t.Fatalf("no torn reads observed for dirty load")
	}
}

func TestAtomicOf_DirtyLoad_Zero_NoTear(t *testing.T) {
	type z struct{}
	var a atomicOf[z]
	stop := make(chan struct{})
	var wg sync.WaitGroup
	writers := 4
	readers := 8
	var errors atomic.Int64
	wg.Add(writers)
	for range writers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					a.store(z{})
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.load()
					_ = v
					runtime.Gosched()
				}
			}
		}()
	}
	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()
	if c := errors.Load(); c != 0 {
		t.Fatalf("zero-size torn reads: %d", c)
	}
}

func TestAtomicOf_DirtyLoad_Uint32_NoTear(t *testing.T) {
	var a atomicOf[uint32]
	stop := make(chan struct{})
	var wg sync.WaitGroup
	v1 := uint32(0xAAAAAAAA)
	v2 := uint32(0x55555555)
	var errors atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		cur := v1
		for {
			select {
			case <-stop:
				return
			default:
				if cur == v1 {
					cur = v2
				} else {
					cur = v1
				}
				a.store(cur)
				runtime.Gosched()
			}
		}
	}()
	wg.Add(4)
	for range 4 {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.load()
					if v != v1 && v != v2 {
						errors.Add(1)
					}
					runtime.Gosched()
				}
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	if c := errors.Load(); c != 0 {
		t.Logf("uint32 torn reads: %d", c)
	}
}

func TestAtomicOf_DirtyLoad_Uint64_ArchDependent(t *testing.T) {
	var a atomicOf[uint64]
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var errors atomic.Int64
	// Pre-initialize to a consistent value to avoid early zero reads
	x0 := uint32(0x13579BDF)
	v0 := (uint64(^x0) << 32) | uint64(x0)
	a.store(v0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				x := uint32(rand.Int())
				lo := x
				hi := ^x
				v := (uint64(hi) << 32) | uint64(lo)
				a.store(v)
				runtime.Gosched()
			}
		}
	}()
	wg.Add(4)
	for range 4 {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := a.load()
					lo := uint32(v & 0xffffffff)
					hi := uint32(v >> 32)
					if hi != ^lo {
						errors.Add(1)
					}
					runtime.Gosched()
				}
			}
		}()
	}
	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()
	if isTSO {
		if c := errors.Load(); c != 0 {
			t.Fatalf("uint64 torn reads on TSO: %d", c)
		}
	} else {
		if c := errors.Load(); c == 0 {
			t.Logf("no torn reads observed on weak model")
		} else {
			t.Logf("torn reads observed: %d", c)
		}
	}
}
