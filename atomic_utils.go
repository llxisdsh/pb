package pb

import (
	"math/bits"
	"runtime"
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
	u := loadUintptr(src)
	return *(*T)(unsafe.Pointer(&u))
}

//go:nosplit
func (a *atomicValue[T]) StoreNoWB(v T) {
	dst := (*uintptr)(unsafe.Pointer(&a.raw))
	storeUintptrNoWB(dst, *(*uintptr)(unsafe.Pointer(&v)))
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
func (a *atomicUint64) Load() uint64 {
	return loadUint64((*uint64)(unsafe.Pointer(a)))
}

//go:nosplit
func (a *atomicUint64) StoreNoWB(val uint64) {
	storeUint64NoWB((*uint64)(unsafe.Pointer(a)), val)
}

//go:nosplit
func (a *atomicUint64) Raw() *uint64 {
	return (*uint64)(unsafe.Pointer(a))
}

//goland:noinspection ALL
const useAutoDetectedTSO = atomicLevel == -1 &&
	(runtime.GOARCH == "amd64" || runtime.GOARCH == "386" ||
		runtime.GOARCH == "s390x" || runtime.GOARCH == "s390")

//go:nosplit
func loadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 1 {
		return *addr
	} else {
		return atomic.LoadPointer(addr)
	}
}

// storePointerNoWB stores a pointer value at the given address.
// Note: Must be called with lock held to ensure safety.
//
//go:nosplit
func storePointerNoWB(addr *unsafe.Pointer, val unsafe.Pointer) {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 2 {
		*addr = val
	} else {
		atomic.StorePointer(addr, val)
	}
}

//go:nosplit
func loadUintptr(addr *uintptr) uintptr {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 1 {
		return *addr
	} else {
		return atomic.LoadUintptr(addr)
	}
}

// storeUintptrNoWB stores an uintptr value at the given address.
// Note: Must be called with lock held to ensure safety.
//
//go:nosplit
func storeUintptrNoWB(addr *uintptr, val uintptr) {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 2 {
		*addr = val
	} else {
		atomic.StoreUintptr(addr, val)
	}
}

//go:nosplit
func loadUint64(addr *uint64) uint64 {
	//goland:noinspection ALL
	if (useAutoDetectedTSO || atomicLevel >= 1) && bits.UintSize >= 64 {
		return *addr
	} else {
		return atomic.LoadUint64(addr)
	}
}

// storeUint64NoWB stores an uint64 value at the given address.
// Note: Must be called with lock held to ensure safety.
//
//go:nosplit
func storeUint64NoWB(addr *uint64, val uint64) {
	//goland:noinspection ALL
	if (useAutoDetectedTSO || atomicLevel >= 2) && bits.UintSize >= 64 {
		*addr = val
	} else {
		atomic.StoreUint64(addr, val)
	}
}
