package pb

import (
	"sync/atomic"
	"unsafe"
)

// seqlock[SEQ, T] coordinates tear-free publication for a single slot.
//
// Role:
//   - Sequence guard only: holds odd/even counter, no payload.
//   - Stable-window reads: readers copy from seqlockSlot[T] when seq is
//     even and unchanged.
//   - Non-blocking publishes: writers CAS to odd, copy, then publish by
//     storing seq+2 (even).
//
// Differences:
//   - vs atomic.Pointer[T]: avoids heap indirection; improves locality.
//   - vs atomic.Value: typed publication without boxing; supports
//     multi-word snapshots on weak memory via the sequence.
//
// Usage:
//
//	Pair with seqlockSlot[T]. Use Read/Write helpers or the Locked
//	variants when an external lock is held.
type seqlock[SEQ ~uint32 | ~uint64 | ~uintptr, T any] struct {
	seq SEQ
}

// Read atomically loads a tear-free snapshot using the external seqlock.
// Spins until seq is even and unchanged across two reads; copies the value
// within the stable window.
func (sl *seqlock[SEQ, T]) Read(slot *seqlockSlot[T]) (v T) {
	if s1, ok := sl.BeginRead(); ok {
		v = slot.ReadUnfenced()
		if ok = sl.EndRead(s1); ok {
			return v
		}
	}
	return sl.slowRead(slot)
}

func (sl *seqlock[SEQ, T]) slowRead(slot *seqlockSlot[T]) (v T) {
	var spins int
	for {
		if s1, ok := sl.BeginRead(); ok {
			v = slot.ReadUnfenced()
			if ok = sl.EndRead(s1); ok {
				return v
			}
		}
		delay(&spins)
	}
}

// Write publishes v guarded by the external seqlock.
// Enters odd, copies v, then exits to even to publish a stable snapshot.
func (sl *seqlock[SEQ, T]) Write(slot *seqlockSlot[T], v T) {
	if s1, ok := sl.BeginWrite(); ok {
		slot.WriteUnfenced(v)
		sl.EndWrite(s1)
		return
	}
	sl.slowWrite(slot, v)
}

func (sl *seqlock[SEQ, T]) slowWrite(slot *seqlockSlot[T], v T) {
	var spins int
	for {
		if s1, ok := sl.BeginWrite(); ok {
			slot.WriteUnfenced(v)
			sl.EndWrite(s1)
			return
		}
		delay(&spins)
	}
}

// WriteLocked publishes v using in-lock odd/even increments.
// Only safe when an external lock is held; avoids CAS by using add.
func (sl *seqlock[SEQ, T]) WriteLocked(slot *seqlockSlot[T], v T) {
	sl.BeginWriteLocked()
	slot.WriteUnfenced(v)
	sl.EndWriteLocked()
}

// BeginRead enters the reader window if seq is even.
// Returns the observed sequence and ok=true when even.
//
//go:nosplit
func (sl *seqlock[SEQ, T]) BeginRead() (s1 SEQ, ok bool) {
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		s1 = SEQ(atomic.LoadUint32((*uint32)(unsafe.Pointer(&sl.seq))))
		return s1, s1&1 == 0
	} else {
		s1 = SEQ(atomic.LoadUint64((*uint64)(unsafe.Pointer(&sl.seq))))
		return s1, s1&1 == 0
	}
}

// EndRead verifies the window stability: returns true if seq unchanged.
//
//go:nosplit
func (sl *seqlock[SEQ, T]) EndRead(s1 SEQ) (ok bool) {
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		s2 := SEQ(atomic.LoadUint32((*uint32)(unsafe.Pointer(&sl.seq))))
		return s1 == s2
	} else {
		s2 := SEQ(atomic.LoadUint64((*uint64)(unsafe.Pointer(&sl.seq))))
		return s1 == s2
	}
}

// BeginWrite enters the writer window by CASing seq to odd.
// Returns previous sequence and ok=true on success.
//
//go:nosplit
func (sl *seqlock[SEQ, T]) BeginWrite() (s1 SEQ, ok bool) {
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		s1 = SEQ(atomic.LoadUint32((*uint32)(unsafe.Pointer(&sl.seq))))
		if s1&1 != 0 {
			return s1, false
		}
		return s1, atomic.CompareAndSwapUint32((*uint32)(unsafe.Pointer(&sl.seq)), uint32(s1), uint32(s1)|1)
	} else {
		s1 = SEQ(atomic.LoadUint64((*uint64)(unsafe.Pointer(&sl.seq))))
		if s1&1 != 0 {
			return s1, false
		}
		return s1, atomic.CompareAndSwapUint64((*uint64)(unsafe.Pointer(&sl.seq)), uint64(s1), uint64(s1)|1)
	}
}

// EndWrite exits the writer window by storing seq+2 (even).
//
//go:nosplit
func (sl *seqlock[SEQ, T]) EndWrite(s1 SEQ) {
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		atomic.StoreUint32((*uint32)(unsafe.Pointer(&sl.seq)), uint32(s1)+2)
	} else {
		atomic.StoreUint64((*uint64)(unsafe.Pointer(&sl.seq)), uint64(s1)+2)
	}
}

// BeginWriteLocked enters the writer window using add (odd).
// Only safe when an external lock is held.
//
//go:nosplit
func (sl *seqlock[SEQ, T]) BeginWriteLocked() {
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		atomic.AddUint32((*uint32)(unsafe.Pointer(&sl.seq)), 1)
	} else {
		atomic.AddUint64((*uint64)(unsafe.Pointer(&sl.seq)), 1)
	}
}

// EndWriteLocked exits the writer window using add (even).
// Only safe when an external lock is held.
//
//go:nosplit
func (sl *seqlock[SEQ, T]) EndWriteLocked() {
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		atomic.AddUint32((*uint32)(unsafe.Pointer(&sl.seq)), 1)
	} else {
		atomic.AddUint64((*uint64)(unsafe.Pointer(&sl.seq)), 1)
	}
}

// WriteCompleted returns true if seq is non-zero and even.
//
//go:nosplit
func (sl *seqlock[SEQ, T]) WriteCompleted() (ok bool) {
	if unsafe.Sizeof(SEQ(0)) == unsafe.Sizeof(uint32(0)) {
		s1 := SEQ(atomic.LoadUint32((*uint32)(unsafe.Pointer(&sl.seq))))
		return s1 != 0 && s1&1 == 0
	} else {
		s1 := SEQ(atomic.LoadUint64((*uint64)(unsafe.Pointer(&sl.seq))))
		return s1 != 0 && s1&1 == 0
	}
}

// seqlockSlot[T] holds an inline buffer of T. Used with seqlock to
// publish tear-free snapshots.
//
// Copy semantics:
//   - On TSO (amd64/386/s390x), plain typed copies are sufficient.
//   - On weak models, uintptr-sized atomics are used inside the stable
//     window when alignment/size permit; otherwise a typed copy is used.
//
// Safety:
//   - ReadUnfenced/WriteUnfenced must run under a seqlock-stable window or
//     an external lock; otherwise torn reads/writes are possible.
type seqlockSlot[T any] struct {
	_   [0]atomic.Uintptr
	buf T
}

// ReadUnfenced copies buf into v using uintptr-sized atomic loads when
// alignment and size permit; otherwise falls back to a typed copy.
// Must be called under a lock or within a seqlock-stable window.
func (slot *seqlockSlot[T]) ReadUnfenced() (v T) {
	if isTSO {
		return slot.buf
	}

	switch unsafe.Sizeof(slot.buf) {
	case 0:
		return v
	case unsafe.Sizeof(uintptr(0)):
		if unsafe.Alignof(slot.buf) >= unsafe.Sizeof(uintptr(0)) {
			u := atomic.LoadUintptr((*uintptr)(unsafe.Pointer(&slot.buf)))
			*(*uintptr)(unsafe.Pointer(&v)) = u
			return v
		}
		// fall through to stable window copy
	}

	ws := unsafe.Sizeof(uintptr(0))
	sz := unsafe.Sizeof(slot.buf)
	al := unsafe.Alignof(slot.buf)
	if al >= ws && sz%ws == 0 {
		n := sz / ws
		for i := range n {
			off := i * ws
			src := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&slot.buf)) + off,
			))
			dst := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&v)) + off,
			))
			*dst = atomic.LoadUintptr(src)
		}
		return v
	}
	v = slot.buf
	return v
}

// WriteUnfenced writes v into buf using uintptr-sized atomic stores when
// alignment and size permit; otherwise falls back to a typed copy.
// Must be called under a lock or within a seqlock-stable window.
func (slot *seqlockSlot[T]) WriteUnfenced(v T) {
	if isTSO {
		slot.buf = v
		return
	}
	switch unsafe.Sizeof(slot.buf) {
	case 0:
		return
	case unsafe.Sizeof(uintptr(0)):
		if unsafe.Alignof(slot.buf) >= unsafe.Sizeof(uintptr(0)) {
			u := *(*uintptr)(unsafe.Pointer(&v))
			atomic.StoreUintptr((*uintptr)(unsafe.Pointer(&slot.buf)), u)
			return
		}
		// fall through to fenced publication
	}

	ws := unsafe.Sizeof(uintptr(0))
	sz := unsafe.Sizeof(slot.buf)
	al := unsafe.Alignof(slot.buf)
	if al >= ws && sz%ws == 0 {
		n := sz / ws
		for i := range n {
			off := i * ws
			src := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&v)) + off,
			))
			dst := (*uintptr)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&slot.buf)) + off,
			))
			atomic.StoreUintptr(dst, *src)
		}
		return
	}
	slot.buf = v
}

// Ptr returns the address of the inline buffer. Mutations through this
// pointer must be guarded by an external lock or odd/even sequence;
// otherwise readers may observe torn data.
//
//go:nosplit
func (slot *seqlockSlot[T]) Ptr() *T {
	return &slot.buf
}
