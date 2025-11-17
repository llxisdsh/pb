package pb

import (
	"sync/atomic"
	"unsafe"
)

// atomicOf[T] is a flat, single-slot publisher for typed T.
//
// Purpose:
//   - Inline storage: no pointer chasing; cache-friendly locality.
//   - External sequence (uint32) acts like a seqlock (odd=write, even=stable)
//     to provide tear-free snapshots without pointer indirection.
//
// Differences:
//   - vs atomic.Pointer[T]: avoids heap allocation and indirection; reduces
//     GC write barriers and improves locality.
//   - vs atomic.Value: typed, no interface boxing; avoids extra allocation;
//     uses external sequence for multi-word publication on weak memory models.
//
// Concurrency:
//   - Writers: StoreWithSeq(seq, v) → CAS seq to odd → copy v → store even.
//   - Readers: LoadWithSeq(seq) → read s1 → copy → read s2 → if equal/even,
//     snapshot is stable.
//
// Platform notes:
//   - On TSO (amd64/386/s390x), plain copies are sufficient.
//   - On weak memory models, per-uint32 atomics are used inside the stable
//     window to ensure visibility for multi-word values.
type atomicOf[T any, SEQ ~uint32 | ~uint64] struct {
	_   [0]atomic.Uintptr
	buf T
}

// Copies buf into v using uintptr-sized atomic loads when alignment
// and size permit; otherwise falls back to a typed copy. This must be
// called within a stable window (LoadWithSeq) on weak memory models.
//
//go:nosplit
func (a *atomicOf[T, SEQ]) load() (v T) {
	ws := unsafe.Sizeof(uintptr(0))
	sz := unsafe.Sizeof(a.buf)
	al := unsafe.Alignof(a.buf)
	if al >= ws && sz%ws == 0 {
		n := sz / ws
		for i := range n {
			off := i * ws
			src := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&a.buf)) + off,
			))
			dst := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&v)) + off,
			))
			*dst = atomic.LoadUintptr(src)
		}
		return v
	}
	v = a.buf
	return v
}

// Writes v into buf using uintptr-sized atomic stores when alignment
// and size permit; otherwise falls back to a typed copy. Publication
// is completed by StoreWithSeq using odd/even fencing.
//
//go:nosplit
func (a *atomicOf[T, SEQ]) store(v T) {
	ws := unsafe.Sizeof(uintptr(0))
	sz := unsafe.Sizeof(a.buf)
	al := unsafe.Alignof(a.buf)
	if al >= ws && sz%ws == 0 {
		n := sz / ws
		for i := range n {
			off := i * ws
			src := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&v)) + off,
			))
			dst := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&a.buf)) + off,
			))
			atomic.StoreUintptr(dst, *src)
		}
		return
	}
	a.buf = v
}

// Returns the address of the inline buffer. Mutations through this
// pointer must be guarded by an external lock or odd/even sequence;
// otherwise readers may observe torn data.
//
//go:nosplit
func (a *atomicOf[T, SEQ]) Raw() *T {
	return &a.buf
}

// LoadWithSeq atomically loads a tear-free snapshot guarded by the external sequence.
// Spins until seq is even and unchanged across two reads; copying
// uses plain copy on TSO, and per-word atomics on weak models.
//
//go:nosplit
func (a *atomicOf[T, SEQ]) LoadWithSeq(seq *SEQ) (v T) {
	for {
		if v, ok := a.TryLoadWithSeq(seq); ok {
			return v
		}
	}
}

//go:nosplit
func (a *atomicOf[T, SEQ]) TryLoadWithSeq(seq *SEQ) (v T, ok bool) {
	switch unsafe.Sizeof(a.buf) {
	case 0:
		return v, true
	case unsafe.Sizeof(uintptr(0)):
		if unsafe.Alignof(a.buf) >= unsafe.Sizeof(uintptr(0)) {
			u := atomic.LoadUintptr((*uintptr)(unsafe.Pointer(&a.buf)))
			*(*uintptr)(unsafe.Pointer(&v)) = u
			return v, true
		}
		// fall through to stable window copy
	}
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		s1 := atomic.LoadUint32((*uint32)(unsafe.Pointer(seq)))
		if s1&1 != 0 {
			return v, false
		}
		if isTSO {
			v = a.buf
		} else {
			v = a.load()
		}
		s2 := atomic.LoadUint32((*uint32)(unsafe.Pointer(seq)))
		if s1 == s2 {
			return v, true
		}
	} else {
		s1 := atomic.LoadUint64((*uint64)(unsafe.Pointer(seq)))
		if s1&1 != 0 {
			return v, false
		}
		if isTSO {
			v = a.buf
		} else {
			v = a.load()
		}
		s2 := atomic.LoadUint64((*uint64)(unsafe.Pointer(seq)))
		if s1 == s2 {
			return v, true
		}
	}

	return v, false
}

// StoreWithSeq publishes v guarded by the external sequence.
// CAS seq to odd (enter write), copy v, then store seq+2 (even)
// to publish a stable snapshot with release-acquire ordering.
//
//go:nosplit
func (a *atomicOf[T, SEQ]) StoreWithSeq(seq *SEQ, v T) {
	for {
		if ok := a.TryStoreWithSeq(seq, v); ok {
			return
		}
	}
}

//go:nosplit
func (a *atomicOf[T, SEQ]) TryStoreWithSeq(seq *SEQ, v T) (ok bool) {
	switch unsafe.Sizeof(a.buf) {
	case 0:
		return true
	case unsafe.Sizeof(uintptr(0)):
		if unsafe.Alignof(a.buf) >= unsafe.Sizeof(uintptr(0)) {
			u := *(*uintptr)(unsafe.Pointer(&v))
			atomic.StoreUintptr((*uintptr)(unsafe.Pointer(&a.buf)), u)
			return true
		}
		// fall through to fenced publication
	}
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		s := atomic.LoadUint32((*uint32)(unsafe.Pointer(seq)))
		if s&1 != 0 {
			return false
		}
		if atomic.CompareAndSwapUint32((*uint32)(unsafe.Pointer(seq)), s, s|1) {
			if isTSO {
				a.buf = v
			} else {
				a.store(v)
			}
			atomic.StoreUint32((*uint32)(unsafe.Pointer(seq)), s+2)
			return true
		}
		return false
	} else {
		s := atomic.LoadUint64((*uint64)(unsafe.Pointer(seq)))
		if s&1 != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64((*uint64)(unsafe.Pointer(seq)), s, s|1) {
			if isTSO {
				a.buf = v
			} else {
				a.store(v)
			}
			atomic.StoreUint64((*uint64)(unsafe.Pointer(seq)), s+2)
			return true
		}
		return false
	}
}
