package pb

import (
	"unsafe"
)

type atomicValue[T any] struct {
	_   [0]uint64
	Val T
}

//go:nosplit
func (a *atomicValue[T]) Load() T {
	src := (*uint64)(unsafe.Pointer(&a.Val))
	u := loadUint64(src)
	return *(*T)(unsafe.Pointer(&u))
}

//go:nosplit
func (a *atomicValue[T]) Store(v T) {
	dst := (*uint64)(unsafe.Pointer(&a.Val))
	storeUint64(dst, *(*uint64)(unsafe.Pointer(&v)))
}
