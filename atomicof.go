package pb

import (
	"sync/atomic"
	"unsafe"
)

const directLoadStore = true

type atomicOf[T any] struct {
	_   [0]atomic.Uint32
	buf T
}

func (a *atomicOf[T]) load() (v T) {
	for i := range unsafe.Sizeof(a.buf) / 4 {
		src := (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&a.buf)) + i*4))
		dst := (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&v)) + i*4))
		*dst = atomic.LoadUint32(src)
	}
	return v
}

func (a *atomicOf[T]) store(v T) {
	for i := range unsafe.Sizeof(a.buf) / 4 {
		src := (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&v)) + i*4))
		dst := (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&a.buf)) + i*4))
		atomic.StoreUint32(dst, *src)
	}
}

func (a *atomicOf[T]) Raw() *T {
	return &a.buf
}

func (a *atomicOf[T]) LoadWithSeq(seq *uint32) (v T) {
	for {
		s1 := atomic.LoadUint32(seq)
		if s1&1 != 0 {
			continue
		}
		if directLoadStore || isTSO {
			v = a.buf
		} else {
			v = a.load()
		}
		s2 := atomic.LoadUint32(seq)
		if s1 == s2 {
			return v
		}
	}
}

func (a *atomicOf[T]) StoreWithSeq(seq *uint32, v T) {
	for {
		s := atomic.LoadUint32(seq)
		if s&1 != 0 {
			continue
		}
		if atomic.CompareAndSwapUint32(seq, s, s|1) {
			if directLoadStore || isTSO {
				a.buf = v
			} else {
				a.store(v)
			}
			atomic.StoreUint32(seq, s+2)
			return
		}
	}
}
