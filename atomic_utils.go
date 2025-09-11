package pb

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// atomicValue provides lock-free atomic operations for generic
// types. It uses unsafe pointer conversion to enable atomic
// access to any type T.
type atomicValue[T any] struct {
	_   [0]uintptr
	raw T
}

//go:nosplit
func (a *atomicValue[T]) Load() T {
	src := (*uintptr)(unsafe.Pointer(&a.raw))
	u := atomic.LoadUintptr(src)
	return *(*T)(unsafe.Pointer(&u))
}

//go:nosplit
func (a *atomicValue[T]) Store(v T) {
	dst := (*uintptr)(unsafe.Pointer(&a.raw))
	atomic.StoreUintptr(dst, *(*uintptr)(unsafe.Pointer(&v)))
}

func checkAtomicValueSize[T any]() {
	if unsafe.Sizeof(atomicValue[T]{}) != unsafe.Sizeof(uintptr(0)) {
		panic(fmt.Sprintf("value size must be <= %d bytes, got %d",
			unsafe.Sizeof(uintptr(0)),
			unsafe.Sizeof(atomicValue[T]{})),
		)
	}
}

// atomicUint64 wraps atomic.Uint64 to leverage its built-in
// alignment capabilities. The primary purpose is to ensure
// 8-byte alignment on 32-bit architectures, where atomic.Uint64
// guarantees proper alignment for atomic operations.
type atomicUint64 struct {
	atomic.Uint64
}

//go:nosplit
func makeAtomicUint64(v uint64) atomicUint64 {
	return *(*atomicUint64)(unsafe.Pointer(&v))
}

//go:nosplit
func (a *atomicUint64) Raw() *uint64 {
	return (*uint64)(unsafe.Pointer(a))
}
