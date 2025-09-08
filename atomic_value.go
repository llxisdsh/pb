package pb

import (
	"unsafe"
)

type atomicValue[T any] struct {
	_   [0]uint64
	raw T
}

//go:nosplit
func (a *atomicValue[T]) Load() T {
	src := (*uint64)(unsafe.Pointer(&a.raw))
	u := loadUint64(src)
	return *(*T)(unsafe.Pointer(&u))
}

//go:nosplit
func (a *atomicValue[T]) Store(v T) {
	dst := (*uint64)(unsafe.Pointer(&a.raw))
	storeUint64(dst, *(*uint64)(unsafe.Pointer(&v)))
}
