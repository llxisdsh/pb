//go:build !race

package pb

import (
	"math/rand/v2"
	"runtime"
	"runtime/debug"
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

func TestSeqlock_NoTornRead(t *testing.T) {
	var a seqlockSlot[bigSeq]
	var sl seqlock[uint32, bigSeq]

	x0 := uint64(3)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0xAA, D: ^(x0 ^ 0xAA)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	sl.Write(&a, v0)

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
					sl.Write(&a, v)
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
					v := sl.Read(&a)
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

func TestSeqlock_ContinuousWritersProgress(t *testing.T) {
	var a seqlockSlot[bigSeq]
	var sl seqlock[uint32, bigSeq]

	x0 := uint64(11)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0x33, D: ^(x0 ^ 0x33)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	sl.Write(&a, v0)

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
					sl.Write(&a, v)
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
					v := sl.Read(&a)
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

func TestSeqlock_AddStyleWriterProducesTornReads(t *testing.T) {
	var a seqlockSlot[bigSeq]
	var sl seqlock[uint32, bigSeq]

	x0 := uint64(31)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0xCC, D: ^(x0 ^ 0xCC)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	sl.Write(&a, v0)

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
					sl.BeginWriteLocked()
					x := uint64(rand.Int64()) ^ uint64(id)*0x9e3779b97f4a7bb1
					v := bigSeq{A: x, B: ^x, C: x ^ 0xCC, D: ^(x ^ 0xCC)}
					for i := range v.X {
						v.X[i] = x + uint64(i)
					}
					a.WriteUnfenced(v)
					sl.EndWriteLocked()
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
					v := sl.Read(&a)
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

func TestSeqlock_AddStyleWriterWithLock_NoTornRead(t *testing.T) {
	var a seqlockSlot[bigSeq]
	var sl seqlock[uint32, bigSeq]
	var mu sync.Mutex

	x0 := uint64(41)
	v0 := bigSeq{A: x0, B: ^x0, C: x0 ^ 0x99, D: ^(x0 ^ 0x99)}
	for i := range v0.X {
		v0.X[i] = x0 + uint64(i)
	}
	sl.Write(&a, v0)

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
					sl.BeginWriteLocked()
					x := uint64(rand.Int64()) ^ uint64(id)*0x9e3779b97f4a7bb1
					v := bigSeq{A: x, B: ^x, C: x ^ 0x99, D: ^(x ^ 0x99)}
					for i := range v.X {
						v.X[i] = x + uint64(i)
					}
					a.WriteUnfenced(v)
					sl.EndWriteLocked()
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
					v := sl.Read(&a)
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

type ptrSeq struct {
	I   uint64
	P1  *uint64
	P2  *uint64
	Arr [4]*uint64
	B   []byte
	M   map[string]*uint64
	S   *string
}

func TestSeqlock_UnfencedCopy_PointerStruct_NoTornRead(t *testing.T) {
	var a seqlockSlot[ptrSeq]
	var sl seqlock[uint32, ptrSeq]

	x0 := uint64(7)
	p1 := new(uint64)
	*p1 = x0 ^ 0xAA
	p2 := new(uint64)
	*p2 = ^(*p1)
	s0 := "x"
	v0 := ptrSeq{
		I:   x0,
		P1:  p1,
		P2:  p2,
		Arr: [4]*uint64{p1, p2, p1, p2},
		B:   []byte{byte(x0), byte(x0 ^ 0x1)},
		M:   map[string]*uint64{"k": p1, "k2": p2},
		S:   &s0,
	}
	if s1, ok := sl.BeginWrite(); ok {
		a.WriteUnfenced(v0)
		sl.EndWrite(s1)
	}

	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	writers := 4
	readers := 8

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
					p1 := new(uint64)
					*p1 = x ^ 0xAA
					p2 := new(uint64)
					*p2 = ^(*p1)
					s := x ^ 0x55
					sv := []byte{byte(x), byte(x ^ 1), byte(x ^ 2)}
					str := string([]byte{byte(s)})
					v := ptrSeq{
						I:   x,
						P1:  p1,
						P2:  p2,
						Arr: [4]*uint64{p1, p2, p1, p2},
						B:   sv,
						M:   map[string]*uint64{"k": p1, "k2": p2},
						S:   &str,
					}
					if s1, ok := sl.BeginWrite(); ok {
						a.WriteUnfenced(v)
						sl.EndWrite(s1)
					}
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
					s1, ok := sl.BeginRead()
					if !ok {
						continue
					}
					v := a.ReadUnfenced()
					if !sl.EndRead(s1) {
						continue
					}
					if v.P1 == nil || v.P2 == nil || v.M == nil || v.S == nil || len(v.B) < 2 {
						errors.Add(1)
						continue
					}
					if *v.P2 != ^(*v.P1) {
						errors.Add(1)
					}
					if v.Arr[0] == nil || v.Arr[1] == nil || *v.Arr[1] != ^(*v.Arr[0]) {
						errors.Add(1)
					}
					if kv, ok := v.M["k"]; !ok || kv == nil || *kv != *v.P1 {
						errors.Add(1)
					}
					if kv2, ok := v.M["k2"]; !ok || kv2 == nil || *kv2 != *v.P2 {
						errors.Add(1)
					}
					if v.B[0] != byte(v.I) {
						errors.Add(1)
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	gcStop := make(chan struct{})
	go func() {
		for {
			select {
			case <-gcStop:
				return
			default:
				runtime.GC()
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	dur := 800 * time.Millisecond
	if testing.CoverMode() != "" {
		dur = 400 * time.Millisecond
	}
	time.Sleep(dur)
	close(stop)
	close(gcStop)
	wg.Wait()

	if errors.Load() != 0 {
		t.Fatalf("pointer torn/dangling detected: %d", errors.Load())
	}
}

type fobj struct {
	V uint64
	B []byte
}

type ptrSeqF struct {
	O   *fobj
	Arr [2]*fobj
	S   *string
	M   map[string]*fobj
	Z   uintptr
}

func TestSeqlock_UnfencedCopy_PointerStruct_Finalizer_Safety(t *testing.T) {
	var a seqlockSlot[ptrSeqF]
	var sl seqlock[uint32, ptrSeqF]

	s0 := "init"
	v0 := ptrSeqF{S: &s0, M: make(map[string]*fobj), Z: 1}
	if s1, ok := sl.BeginWrite(); ok {
		a.WriteUnfenced(v0)
		sl.EndWrite(s1)
	}

	var dangling atomic.Int64
	var triggered atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	writers := 4
	readers := 8

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
					o := &fobj{V: x, B: make([]byte, 1<<15)}
					runtime.SetFinalizer(o, func(obj *fobj) {
						triggered.Add(1)
						for range 10 {
							s1, ok := sl.BeginRead()
							if !ok {
								runtime.Gosched()
								continue
							}
							v := a.ReadUnfenced()
							if sl.EndRead(s1) {
								if v.O == obj || v.Arr[0] == obj || v.Arr[1] == obj || v.M["o"] == obj {
									dangling.Add(1)
								}
								break
							}
						}
					})
					v := ptrSeqF{
						O:   o,
						Arr: [2]*fobj{o, o},
						S:   &s0,
						M:   map[string]*fobj{"o": o},
						Z:   2,
					}
					if s1, ok := sl.BeginWrite(); ok {
						a.WriteUnfenced(v)
						sl.EndWrite(s1)
					}
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
					s1, ok := sl.BeginRead()
					if !ok {
						continue
					}
					v := a.ReadUnfenced()
					if !sl.EndRead(s1) {
						continue
					}
					if v.O == nil || v.Arr[0] == nil || v.Arr[1] == nil || v.M == nil || v.M["o"] == nil || v.S == nil {
						continue
					}
					runtime.GC()
					runtime.Gosched()
				}
			}
		}(r)
	}

	gcStop := make(chan struct{})
	go func() {
		for {
			select {
			case <-gcStop:
				return
			default:
				runtime.GC()
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	dur2 := 1200 * time.Millisecond
	if testing.CoverMode() != "" {
		dur2 = 600 * time.Millisecond
	}
	time.Sleep(dur2)
	close(stop)
	close(gcStop)
	wg.Wait()

	if dangling.Load() != 0 {
		t.Fatalf("dangling finalized while referenced: %d (triggered=%d)", dangling.Load(), triggered.Load())
	}
}

func TestSeqlock_UnfencedCopy_PointerStruct_Finalizer_HardPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping hard-pressure finalizer test in short mode")
	}
	var a seqlockSlot[ptrSeqF]
	var sl seqlock[uint32, ptrSeqF]

	old := debug.SetGCPercent(10)
	defer debug.SetGCPercent(old)

	s0 := "init"
	v0 := ptrSeqF{S: &s0, M: make(map[string]*fobj), Z: 1}
	if s1, ok := sl.BeginWrite(); ok {
		a.WriteUnfenced(v0)
		sl.EndWrite(s1)
	}

	var dangling atomic.Int64
	var triggered atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	allocators := runtime.GOMAXPROCS(0)
	if testing.CoverMode() != "" {
		allocators = 1
	}
	wg.Add(allocators)
	for range allocators {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = make([]byte, 1<<18)
					runtime.Gosched()
				}
			}
		}()
	}

	writers := runtime.GOMAXPROCS(0)
	readers := runtime.GOMAXPROCS(0) * 2

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
					o := &fobj{V: x, B: make([]byte, 1<<16)}
					runtime.SetFinalizer(o, func(obj *fobj) {
						triggered.Add(1)
						for range 50 {
							s1, ok := sl.BeginRead()
							if !ok {
								runtime.Gosched()
								continue
							}
							v := a.ReadUnfenced()
							if sl.EndRead(s1) {
								if v.O == obj || v.Arr[0] == obj || v.Arr[1] == obj || v.M["o"] == obj {
									dangling.Add(1)
								}
								break
							}
						}
					})
					v := ptrSeqF{
						O:   o,
						Arr: [2]*fobj{o, o},
						S:   &s0,
						M:   map[string]*fobj{"o": o},
						Z:   2,
					}
					if s1, ok := sl.BeginWrite(); ok {
						a.WriteUnfenced(v)
						sl.EndWrite(s1)
					}
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
					s1, ok := sl.BeginRead()
					if !ok {
						continue
					}
					v := a.ReadUnfenced()
					if !sl.EndRead(s1) {
						continue
					}
					if v.O == nil || v.Arr[0] == nil || v.Arr[1] == nil || v.M == nil || v.M["o"] == nil || v.S == nil {
						continue
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	run := 3 * time.Second
	if testing.CoverMode() != "" {
		run = 1500 * time.Millisecond
	}
	time.Sleep(run)
	close(stop)
	wg.Wait()

	if dangling.Load() != 0 {
		t.Fatalf("dangling finalized while referenced: %d (triggered=%d)", dangling.Load(), triggered.Load())
	}
}
